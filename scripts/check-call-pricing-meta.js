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
  const log = await db.collection("CallLogs").findOne({
    call_id: "70a0b7fa-5014-4180-b2db-65db637f5e3b",
  });
  const settings = await db
    .collection("systemsettings")
    .find({ key: { $in: ["pricingBasisV1", "providerCommissionV1"] } })
    .toArray();
  console.log(
    JSON.stringify(
      {
        env: {
          PRICING_BASIS: process.env.PRICING_BASIS || null,
          PRICING_DESTINATION_RATE: process.env.PRICING_DESTINATION_RATE || null,
        },
        settings: settings.map((s) => ({ key: s.key, value: s.value })),
        call: {
          totalRatePerMinute: log?.totalRatePerMinute,
          planRatePerMinute: log?.planRatePerMinute,
          pricingRateSource: log?.pricingRateSource,
          matchedPrefix: log?.matchedPrefix,
          providerCostUsdPerMin: log?.providerCostUsdPerMin ?? log?._providerCostUsdPerMin,
          creditsDeductedAmount: log?.creditsDeductedAmount,
          duration: log?.duration ?? log?.twilio?.duration,
          from: log?.from_number || log?.from,
          to: log?.to_number,
          campaign_id: log?.campaign_id,
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
