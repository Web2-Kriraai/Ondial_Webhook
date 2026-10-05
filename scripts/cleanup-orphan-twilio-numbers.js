/**
 * Release Twilio numbers that are on the carrier but NOT in Ondial inventory.
 * Keeps numbers that still exist in phonenumbers / user.phoneNumbers.
 *
 *   node scripts/cleanup-orphan-twilio-numbers.js
 *   node scripts/cleanup-orphan-twilio-numbers.js --apply
 */
const fs = require("fs");
const path = require("path");
const { MongoClient } = require("mongodb");

function loadEnv(f) {
  if (!fs.existsSync(f)) return;
  for (const line of fs.readFileSync(f, "utf8").split(/\r?\n/)) {
    const t = line.trim();
    if (!t || t.startsWith("#")) continue;
    const i = t.indexOf("=");
    if (i < 1) continue;
    let k = t.slice(0, i).trim();
    let v = t.slice(i + 1).trim();
    if (
      (v.startsWith('"') && v.endsWith('"')) ||
      (v.startsWith("'") && v.endsWith("'"))
    ) {
      v = v.slice(1, -1);
    }
    if (!process.env[k]) process.env[k] = v;
  }
}

loadEnv(path.join("C:/Users/ADMIN/Documents/GitHub/Ondial/.env"));

const apply = process.argv.includes("--apply");
const sid = process.env.TWILIO_ACCOUNT_SID?.trim();
const token = process.env.TWILIO_AUTH_TOKEN?.trim();
const auth = Buffer.from(`${sid}:${token}`).toString("base64");

async function twilioGet(apiPath) {
  const res = await fetch(`https://api.twilio.com/2010-04-01${apiPath}`, {
    headers: { Authorization: `Basic ${auth}`, Accept: "application/json" },
  });
  return { status: res.status, json: await res.json().catch(() => null) };
}

async function twilioDelete(apiPath) {
  const res = await fetch(`https://api.twilio.com/2010-04-01${apiPath}`, {
    method: "DELETE",
    headers: { Authorization: `Basic ${auth}` },
  });
  return { status: res.status, text: await res.text() };
}

(async () => {
  const list = await twilioGet(
    `/Accounts/${sid}/IncomingPhoneNumbers.json?PageSize=50`
  );
  const twilioNums = list.json?.incoming_phone_numbers || [];

  const client = new MongoClient(process.env.MONGODB_URI);
  await client.connect();
  const db = client.db();

  const orphans = [];
  for (const n of twilioNums) {
    const e164 = n.phone_number;
    const inCatalog = await db.collection("phonenumbers").findOne({ number: e164 });
    const inUser = await db.collection("users").findOne({
      "phoneNumbers.number": e164,
    });
    if (!inCatalog && !inUser) {
      orphans.push({ e164, sid: n.sid, friendlyName: n.friendly_name });
    }
  }

  console.log("Twilio owned:", twilioNums.length);
  console.log("Orphans (not in Ondial):", orphans.length);
  console.log(JSON.stringify(orphans, null, 2));

  if (!apply) {
    console.log("Dry-run. Re-run with --apply to DELETE orphans from Twilio.");
    await client.close();
    return;
  }

  for (const o of orphans) {
    const r = await twilioDelete(
      `/Accounts/${sid}/IncomingPhoneNumbers/${o.sid}.json`
    );
    console.log("Released", o.e164, r.status, r.text?.slice(0, 120));
  }

  await client.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
