/**
 * Diagnose stuck TEST_CALL_IN_FLIGHT for a destination phone.
 * Usage: node scripts/check-stuck-test-call.js [phoneDigits]
 */
const path = require("path");
const fs = require("fs");
const { MongoClient, ObjectId } = require("mongodb");

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

function digits(s) {
  return String(s || "").replace(/\D/g, "");
}

function last10(s) {
  const d = digits(s);
  return d.length >= 10 ? d.slice(-10) : d;
}

async function main() {
  loadEnv();
  const needle = last10(process.argv[2] || "6353125194");
  const client = new MongoClient(process.env.MONGODB_URI);
  await client.connect();
  const db = client.db();

  const cutoff = new Date(Date.now() - 20 * 60 * 1000);

  const testCalls = await db
    .collection("TestCall")
    .find({ updatedAt: { $gte: new Date(Date.now() - 24 * 60 * 60 * 1000) } })
    .sort({ updatedAt: -1 })
    .limit(50)
    .toArray();

  const matchedTc = testCalls.filter((r) => last10(r.to_number) === needle);

  const callLogs = await db
    .collection("CallLogs")
    .find({
      $or: [{ isTestCall: true }, { is_test: true }, { is_test_call: true }],
      updatedAt: { $gte: new Date(Date.now() - 24 * 60 * 60 * 1000) },
    })
    .sort({ updatedAt: -1 })
    .limit(40)
    .toArray();

  const matchedLogs = callLogs.filter((r) => {
    const to = r.to_number || r.toNumber || r.mobileNumber || r.phone || "";
    return last10(to) === needle || last10(r.to) === needle;
  });

  console.log(
    JSON.stringify(
      {
        needle,
        cutoff20m: cutoff.toISOString(),
        matchedTestCalls: matchedTc.map((r) => ({
          _id: String(r._id),
          call_id: r.call_id,
          lead_id: r.lead_id,
          campaign_id: r.campaign_id,
          to_number: r.to_number,
          status: r.status,
          concurrencyReleased: r.concurrencyReleased ?? null,
          callHangupAt: r.callHangupAt || null,
          createdAt: r.createdAt,
          updatedAt: r.updatedAt,
          ageMin: r.updatedAt
            ? ((Date.now() - new Date(r.updatedAt).getTime()) / 60000).toFixed(1)
            : null,
          inFlightWindow: r.updatedAt ? new Date(r.updatedAt) >= cutoff : false,
        })),
        matchedCallLogs: matchedLogs.map((r) => ({
          _id: String(r._id),
          call_id: r.call_id,
          lead_id: r.lead_id,
          campaign_id: r.campaign_id,
          to_number: r.to_number || r.mobileNumber || r.phone,
          status: r.status,
          twilioStatus: r.twilio?.status || null,
          isTestCall: r.isTestCall,
          callHangupAt: r.callHangupAt || null,
          cdrPushedAt: r.cdrPushedAt || null,
          creditsDeducted: r.creditsDeducted ?? null,
          eventTypes: Array.isArray(r.call_data?.events)
            ? r.call_data.events.map((e) => e.event_type || e.type).slice(-8)
            : [],
          createdAt: r.createdAt,
          updatedAt: r.updatedAt,
        })),
      },
      null,
      2
    )
  );

  await client.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
