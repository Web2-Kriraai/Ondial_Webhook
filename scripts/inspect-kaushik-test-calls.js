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
  const logs = await db
    .collection("CallLogs")
    .find({
      isTestCall: true,
      to_number: /6353125194/,
    })
    .sort({ createdAt: -1 })
    .limit(5)
    .project({
      call_id: 1,
      status: 1,
      duration: 1,
      "twilio.status": 1,
      "twilio.duration": 1,
      creditsDeducted: 1,
      creditsDeductedAmount: 1,
      createdAt: 1,
      "call_data.events.event_type": 1,
      "call_data.events.data.CallStatus": 1,
      "call_data.events.data.CallDuration": 1,
    })
    .toArray();
  console.log(JSON.stringify(logs, null, 2));
  await client.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
