const fs = require("fs");
const path = require("path");
const { MongoClient, ObjectId } = require("mongodb");

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

(async () => {
  const client = new MongoClient(process.env.MONGODB_URI);
  await client.connect();
  const db = client.db();

  const callId = "9bd0dccc-b847-45e7-8050-f70997447a7";
  const callSid = "CA83861b70695ee4b8e71db77a39fbabce";
  const campaignId = "6abf68bc9bdac890710e85fb";

  const log = await db.collection("CallLogs").findOne({
    $or: [
      { call_id: callId },
      { lead_id: callId },
      { "twilio.call_sid": callSid },
      { CallSid: callSid },
    ],
  });

  const tx = await db
    .collection("credittransactions")
    .find({ "reference.callId": callId })
    .sort({ createdAt: -1 })
    .limit(3)
    .toArray();

  const campaign = await db.collection("campaigns").findOne(
    { _id: new ObjectId(campaignId) },
    {
      projection: {
        name: 1,
        selectedPhoneNumber: 1,
        selectedPhoneProvider: 1,
        numberPolicySnapshot: 1,
        selectedVoice: 1,
        isForeign: 1,
      },
    }
  );

  const shadow = await db.collection("pricing_shadow_log").findOne({ callId });

  const pick = log
    ? {
        _id: String(log._id),
        call_id: log.call_id || log.lead_id,
        duration: log.twilio?.duration || log.duration || log.call_duration,
        creditsDeducted: log.creditsDeducted,
        creditsDeductedAmount: log.creditsDeductedAmount,
        totalRatePerMinute: log.totalRatePerMinute,
        planRatePerMinute: log.planRatePerMinute,
        kbRatePerMinute: log.kbRatePerMinute,
        destinationCountryIso: log.destinationCountryIso,
        pricingRateSource: log.pricingRateSource,
        matchedPrefix: log.matchedPrefix,
        destinationRateMode: log.destinationRateMode,
        providerCostUsdPerMin: log.providerCostUsdPerMin,
        commissionPercent: log.commissionPercent,
        from: log.from_number || log.twilio?.From,
        to: log.to_number || log.twilio?.To,
        isTestCall: log.isTestCall,
        numberPolicySnapshot: log.numberPolicySnapshot,
      }
    : null;

  console.log(
    JSON.stringify(
      {
        callLog: pick,
        campaign,
        creditTx: tx.map((t) => ({
          amount: t.amount,
          reference: t.reference,
          createdAt: t.createdAt,
        })),
        shadow: shadow
          ? {
              didIso: shadow.didIso,
              destIso: shadow.destIso,
              rateDid: shadow.rateDid,
              rateDest: shadow.rateDest,
              ratePrefixLive: shadow.ratePrefixLive,
              prefixRateSource: shadow.prefixRateSource,
              matchedPrefix: shadow.matchedPrefix,
              destinationRateMode: shadow.destinationRateMode,
              pricingBasisResolved: shadow.pricingBasisResolved,
              chargeDid: shadow.chargeDid,
              chargeDest: shadow.chargeDest,
              chargePrefixLive: shadow.chargePrefixLive,
            }
          : null,
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
