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
  const now = new Date();
  const filter = {
    $or: [
      { call_id: "9bd0dccc-b847-45e7-8050-f709974744a7" },
      { lead_id: "9bd0dccc-b847-45e7-8050-f709974744a7" },
      { "twilio.call_sid": "CA83861b70695ee4b8e71db77a39fbabce" },
    ],
  };
  const set = {
    status: "completed",
    callHangupAt: now,
    concurrencyReleased: true,
    concurrencyReleasedAt: now,
    updatedAt: now,
  };
  const a = await db.collection("CallLogs").updateMany(filter, { $set: set });
  const b = await db.collection("TestCall").updateMany(filter, { $set: set });
  console.log(
    JSON.stringify({
      callLogsMatched: a.matchedCount,
      testCallsMatched: b.matchedCount,
      healed: true,
    })
  );
  await client.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
