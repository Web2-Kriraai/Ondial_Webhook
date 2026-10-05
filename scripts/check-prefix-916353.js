const path = require("path");
const fs = require("fs");
const { MongoClient } = require("mongodb");

function loadEnv() {
  for (const p of [
    path.join(__dirname, "../.env"),
    path.join(__dirname, "../../Ondial/.env"),
  ]) {
    if (!fs.existsSync(p)) continue;
    for (const line of fs.readFileSync(p, "utf8").split(/\r?\n/)) {
      const m = line.match(/^([^#=]+)=(.*)$/);
      if (!m) continue;
      const k = m[1].trim();
      let v = m[2].trim().replace(/^["']|["']$/g, "");
      if (!process.env[k]) process.env[k] = v;
    }
  }
}

(async () => {
  loadEnv();
  const client = new MongoClient(process.env.MONGODB_URI);
  await client.connect();
  const db = client.db();
  const prefixes = ["916353", "91635", "9163", "916", "91"];
  const cards = await db
    .collection("provider_rate_cards")
    .find({ provider: "twilio", destinationPrefix: { $in: prefixes } })
    .project({ destinationPrefix: 1, rateUsdPerMin: 1, countryIso: 1 })
    .toArray();
  cards.sort(
    (a, b) =>
      String(b.destinationPrefix).length - String(a.destinationPrefix).length
  );
  const log = await db.collection("CallLogs").findOne({
    call_id: "70a0b7fa-5014-4180-b2db-65db637f5e3b",
  });
  console.log(
    JSON.stringify(
      {
        cards: cards.slice(0, 20),
        billing: {
          duration: log?.duration,
          creditsDeductedAmount: log?.creditsDeductedAmount,
          totalRatePerMinute: log?.totalRatePerMinute,
          planRatePerMinute: log?.planRatePerMinute,
          pricingRateSource: log?.pricingRateSource,
          matchedPrefix: log?.matchedPrefix,
          status: log?.status,
          concurrencyReleased: log?.concurrencyReleased,
        },
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
