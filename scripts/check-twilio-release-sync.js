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

const TWILIO_NUMBERS = ["+18382738437", "+17473357058", "+18312467891"];

(async () => {
  const client = new MongoClient(process.env.MONGODB_URI);
  await client.connect();
  const db = client.db();

  const catalog = await db
    .collection("phonenumbers")
    .find({ number: { $in: TWILIO_NUMBERS } })
    .toArray();

  const users = await db
    .collection("users")
    .find({ "phoneNumbers.number": { $in: TWILIO_NUMBERS } })
    .project({ email: 1, phoneNumbers: 1 })
    .toArray();

  const allTwilioCatalog = await db
    .collection("phonenumbers")
    .find({ provider: "twilio" })
    .project({
      number: 1,
      status: 1,
      twilioSid: 1,
      telnyxId: 1,
      purchasedBy: 1,
      provider: 1,
    })
    .toArray();

  const missingSid = allTwilioCatalog.filter((n) => !n.twilioSid);

  console.log(
    JSON.stringify(
      {
        catalogForOwnedTwilio: catalog.map((n) => ({
          number: n.number,
          status: n.status,
          provider: n.provider,
          twilioSid: n.twilioSid || null,
          purchasedBy: n.purchasedBy || null,
        })),
        usersHoldingThese: users.map((u) => ({
          email: u.email,
          nums: (u.phoneNumbers || [])
            .filter((p) => TWILIO_NUMBERS.includes(p.number))
            .map((p) => ({
              number: p.number,
              provider: p.provider,
              twilioSid: p.twilioSid || p.sid || null,
              id: p.id || p._id || null,
            })),
        })),
        allTwilioInOndial: allTwilioCatalog.length,
        twilioMissingSid: missingSid.map((n) => ({
          number: n.number,
          status: n.status,
        })),
      },
      null,
      2
    )
  );

  await client.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
