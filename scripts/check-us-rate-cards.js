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
loadEnv(path.join("C:/Users/ADMIN/Documents/GitHub/Ondial_Webhook/.env"));

(async () => {
  const c = new MongoClient(process.env.MONGODB_URI);
  await c.connect();
  const db = c.db();
  const usCards = await db
    .collection("provider_rate_cards")
    .countDocuments({ provider: "telnyx", countryIso: "US" });
  const nullIso = await db.collection("provider_rate_cards").countDocuments({
    provider: "telnyx",
    $or: [{ countryIso: null }, { countryIso: "" }, { countryIso: { $exists: false } }],
  });
  const sample = await db
    .collection("provider_rate_cards")
    .find({ provider: "telnyx", countryIso: "US" })
    .limit(3)
    .project({ destinationPrefix: 1, rateUsdPerMin: 1, countryIso: 1, description: 1 })
    .toArray();
  const usLikeDesc = await db
    .collection("provider_rate_cards")
    .countDocuments({
      provider: "telnyx",
      description: /united states/i,
    });
  const pricing = await db.collection("systemsettings").findOne({ key: "countryPricingV1" });
  const hasUS = !!pricing?.value?.countries?.US;
  const top = await db
    .collection("provider_rate_cards")
    .aggregate([
      { $match: { provider: "telnyx" } },
      { $group: { _id: "$countryIso", n: { $sum: 1 } } },
      { $sort: { n: -1 } },
      { $limit: 20 },
    ])
    .toArray();
  console.log(
    JSON.stringify(
      { usCards, nullIso, usLikeDesc, hasUS, sample, top },
      null,
      2
    )
  );
  await c.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
