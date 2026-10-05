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
  const campaignId = "6abf68bc9bdac890710e85fb";
  const CallLogs = db.collection("CallLogs");
  const TestCall = db.collection("TestCall");

  const fromLogs = await CallLogs.find({
    campaign_id: campaignId,
    isTestCall: true,
  })
    .sort({ createdAt: -1 })
    .limit(5)
    .project({
      call_id: 1,
      status: 1,
      createdAt: 1,
      creditsDeducted: 1,
      creditsDeductedAmount: 1,
      "call_data.events": 1,
      twilio: 1,
      contactName: 1,
      to_number: 1,
    })
    .toArray();

  const fromTest = await TestCall.find({ campaign_id: campaignId })
    .sort({ updatedAt: -1, createdAt: -1 })
    .limit(5)
    .project({
      call_id: 1,
      status: 1,
      createdAt: 1,
      updatedAt: 1,
      creditsDeducted: 1,
      creditsDeductedAmount: 1,
      call_data: 1,
      twilio: 1,
      contactName: 1,
      to_number: 1,
      duration: 1,
      duration_ms: 1,
    })
    .toArray();

  console.log(
    "fromLogs count",
    fromLogs.length,
    fromLogs.map((r) => ({
      call_id: r.call_id,
      status: r.status,
      events: r.call_data?.events?.length || 0,
      credits: r.creditsDeductedAmount,
      createdAtType: typeof r.createdAt,
      createdAtIsDate: r.createdAt instanceof Date,
      contactName: r.contactName,
    }))
  );
  console.log(
    "fromTest count",
    fromTest.length,
    fromTest.map((r) => ({
      call_id: r.call_id,
      status: r.status,
      events: r.call_data?.events?.length || 0,
      credits: r.creditsDeductedAmount,
      duration: r.duration,
      contactName: r.contactName,
      twilio: r.twilio,
    }))
  );

  // Simulate richness
  function score(row, fromCL) {
    const events = row.call_data?.events;
    const eventCount = Array.isArray(events) ? events.length : 0;
    const duration =
      Number(row.duration_ms) > 0
        ? Number(row.duration_ms)
        : Number(row.duration) > 0
          ? Number(row.duration) * 1000
          : 0;
    const credits =
      Number(row.creditsDeductedAmount) > 0 || row.creditsDeducted === true ? 50_000 : 0;
    return eventCount * 1000 + duration + credits + (fromCL ? 10_000 : 0);
  }

  for (const t of fromTest) {
    const log = fromLogs.find((l) => l.call_id === t.call_id);
    console.log("pair", t.call_id, {
      testScore: score(t, false),
      logScore: log ? score(log, true) : null,
      winner: !log ? "test-only" : score(log, true) >= score(t, false) ? "CallLog" : "TestCall",
      logHasCompletedEvent: !!log?.call_data?.events?.some(
        (e) =>
          e.event_type === "twilio_call_status" &&
          String(e?.data?.CallStatus || "").toLowerCase() === "completed"
      ),
    });
  }

  await client.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
