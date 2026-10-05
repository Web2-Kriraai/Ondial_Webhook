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
  const callId = "70a0b7fa-5014-4180-b2db-65db637f5e3b";
  const sid = "CA36f54ee4997f7beddc1caa0eff81f992";
  const client = new MongoClient(process.env.MONGODB_URI);
  await client.connect();
  const db = client.db();
  const filter = {
    $or: [{ call_id: callId }, { lead_id: callId }, { "twilio.call_sid": sid }],
  };
  const log = await db.collection("CallLogs").findOne(filter);
  const tc = await db.collection("TestCall").findOne(filter);
  const pick = (d) =>
    d
      ? {
          _id: String(d._id),
          call_id: d.call_id,
          status: d.status,
          to_number: d.to_number || d.to || null,
          isTestCall: d.isTestCall ?? null,
          concurrencyReleased: d.concurrencyReleased ?? null,
          callHangupAt: d.callHangupAt || null,
          creditsDeducted: d.creditsDeducted ?? null,
          creditsDeductedAmount: d.creditsDeductedAmount ?? null,
          totalRatePerMinute: d.totalRatePerMinute ?? null,
          pricingRateSource: d.pricingRateSource ?? null,
          matchedPrefix: d.matchedPrefix ?? null,
          destinationCountryIso: d.destinationCountryIso ?? null,
          twilio: d.twilio
            ? {
                status: d.twilio.status,
                duration: d.twilio.duration,
                call_sid: d.twilio.call_sid,
              }
            : null,
          updatedAt: d.updatedAt,
        }
      : null;
  console.log(JSON.stringify({ callLog: pick(log), testCall: pick(tc) }, null, 2));
  await client.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
