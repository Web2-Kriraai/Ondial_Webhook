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

function cell(packages, pkg, tier) {
  const c = packages?.[pkg]?.[tier];
  if (c == null) return null;
  if (typeof c === "object") return Number(c.sale ?? c.list);
  return Number(c);
}

(async () => {
  const client = new MongoClient(process.env.MONGODB_URI);
  await client.connect();
  const db = client.db();

  const comm = await db.collection("systemsettings").findOne({ key: "providerCommissionV1" });
  const pricing = await db.collection("systemsettings").findOne({ key: "countryPricingV1" });

  const telnyxPct =
    Number(comm?.value?.byProvider?.telnyx) || Number(comm?.value?.defaultPercent) || 40;
  const twilioPct =
    Number(comm?.value?.byProvider?.twilio) || Number(comm?.value?.defaultPercent) || 45;

  const agg = async (provider) => {
    const rows = await db
      .collection("provider_rate_cards")
      .aggregate([
        { $match: { provider, countryIso: "IN" } },
        {
          $group: {
            _id: null,
            min: { $min: "$rateUsdPerMin" },
            max: { $max: "$rateUsdPerMin" },
            avg: { $avg: "$rateUsdPerMin" },
            n: { $sum: 1 },
          },
        },
      ])
      .toArray();
    return rows[0] || null;
  };

  const telnyxIN = await agg("telnyx");
  const twilioIN = await agg("twilio");

  const countries = pricing?.value?.countries || {};
  const inEntry = countries.IN || {};
  const usEntry = countries.US || {};

  const out = {
    commission: { telnyxPct, twilioPct },
    cards: { telnyxIN, twilioIN },
    matrixIN_starter_standard: cell(inEntry.packages, "starter", "standard"),
    matrixIN_telnyx_starter_standard: cell(
      inEntry.providers?.telnyx?.packages,
      "starter",
      "standard"
    ),
    matrixIN_twilio_starter_standard: cell(
      inEntry.providers?.twilio?.packages,
      "starter",
      "standard"
    ),
    matrixUS_starter_standard: cell(usEntry.packages, "starter", "standard"),
    matrixUS_telnyx: cell(usEntry.providers?.telnyx?.packages, "starter", "standard"),
    matrixUS_twilio: cell(usEntry.providers?.twilio?.packages, "starter", "standard"),
  };

  if (twilioIN) {
    out.twilioPrefixSellMin = +(twilioIN.min * (1 + twilioPct / 100)).toFixed(6);
    out.twilioPrefixSellMax = +(twilioIN.max * (1 + twilioPct / 100)).toFixed(6);
  }
  if (telnyxIN) {
    out.telnyxPrefixSellMin = +(telnyxIN.min * (1 + telnyxPct / 100)).toFixed(6);
    out.telnyxPrefixSellMax = +(telnyxIN.max * (1 + telnyxPct / 100)).toFixed(6);
  }

  console.log(JSON.stringify(out, null, 2));
  await client.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
