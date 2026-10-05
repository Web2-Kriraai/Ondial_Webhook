const fs = require("fs");
const path = require("path");

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
loadEnv(path.join("C:/Users/ADMIN/Documents/GitHub/Ondial_Webhook/.env"));
loadEnv(path.join("C:/Users/ADMIN/Documents/GitHub/Ondial-Super-Admin/.env"));

const sid = process.env.TWILIO_ACCOUNT_SID?.trim();
const token = process.env.TWILIO_AUTH_TOKEN?.trim();
if (!sid || !token) {
  console.error("TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN missing");
  process.exit(1);
}

const auth = Buffer.from(`${sid}:${token}`).toString("base64");

async function twilioGet(apiPath) {
  const res = await fetch(`https://api.twilio.com/2010-04-01${apiPath}`, {
    headers: {
      Authorization: `Basic ${auth}`,
      Accept: "application/json",
    },
  });
  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = { raw: text.slice(0, 800) };
  }
  return { status: res.status, json };
}

(async () => {
  const out = {};

  // Account balance
  const bal = await twilioGet(`/Accounts/${sid}/Balance.json`);
  out.balanceHttp = bal.status;
  out.balance = bal.json
    ? {
        currency: bal.json.currency,
        balance: bal.json.balance,
        accountSid: bal.json.account_sid,
      }
    : bal.json;

  // Owned incoming numbers (paginated)
  let numbers = [];
  let next =
    `/Accounts/${sid}/IncomingPhoneNumbers.json?PageSize=50`;
  let pages = 0;
  while (next && pages < 20) {
    pages++;
    const pathOnly = next.startsWith("http")
      ? next.replace("https://api.twilio.com/2010-04-01", "")
      : next;
    const r = await twilioGet(pathOnly);
    if (r.status !== 200) {
      out.numbersError = { status: r.status, body: r.json };
      break;
    }
    const batch = Array.isArray(r.json?.incoming_phone_numbers)
      ? r.json.incoming_phone_numbers
      : [];
    numbers = numbers.concat(batch);
    next = r.json?.next_page_uri || null;
  }

  out.ownedNumbers = {
    total: numbers.length,
    sample: numbers.slice(0, 25).map((n) => ({
      phoneNumber: n.phone_number,
      friendlyName: n.friendly_name,
      sid: n.sid,
      status: n.status,
      capabilities: n.capabilities,
      isoCountry: n.iso_country || n.address_requirements || null,
    })),
    byCountry: numbers.reduce((acc, n) => {
      // Twilio list may not always include iso; infer from +country if needed
      const cc = n.iso_country || "unknown";
      acc[cc] = (acc[cc] || 0) + 1;
      return acc;
    }, {}),
  };

  // Quick available local US sample (marketplace)
  const avail = await twilioGet(
    `/Accounts/${sid}/AvailablePhoneNumbers/US/Local.json?PageSize=5`
  );
  out.availableUSLocal = {
    http: avail.status,
    count: Array.isArray(avail.json?.available_phone_numbers)
      ? avail.json.available_phone_numbers.length
      : 0,
    sample: (avail.json?.available_phone_numbers || []).slice(0, 5).map((n) => ({
      phoneNumber: n.phone_number,
      locality: n.locality,
      region: n.region,
      iso: n.iso_country,
    })),
    error: avail.status !== 200 ? avail.json : undefined,
  };

  // Ondial inventory twilio
  try {
    const { MongoClient } = require("mongodb");
    const client = new MongoClient(process.env.MONGODB_URI);
    await client.connect();
    const db = client.db();
    const inv = await db
      .collection("phonenumbers")
      .find({ provider: "twilio" })
      .project({ number: 1, status: 1, country: 1, countryName: 1 })
      .limit(200)
      .toArray();
    out.ondailTwilioInventory = {
      total: inv.length,
      available: inv.filter((n) => n.status === "available").length,
      purchased: inv.filter((n) => n.status === "purchased").length,
      sample: inv.slice(0, 15).map((n) => ({
        number: n.number,
        status: n.status,
        country: n.country,
      })),
    };
    await client.close();
  } catch (e) {
    out.ondailTwilioInventory = { error: e.message };
  }

  console.log(JSON.stringify(out, null, 2));
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
