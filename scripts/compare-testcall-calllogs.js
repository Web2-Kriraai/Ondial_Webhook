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
    .find({ to_number: /6353125194/ })
    .sort({ createdAt: -1 })
    .limit(5)
    .project({
      call_id: 1,
      isTestCall: 1,
      is_test: 1,
      status: 1,
      duration: 1,
      duration_ms: 1,
      creditsDeducted: 1,
      creditsDeductedAmount: 1,
      "twilio.duration": 1,
      "twilio.status": 1,
      createdAt: 1,
      campaign_id: 1,
    })
    .toArray();
  console.log("CallLogs", JSON.stringify(logs, null, 2));

  const tests = await db
    .collection("TestCall")
    .find({
      $or: [
        { phone_number: /6353125194/ },
        { mobileNumber: /6353125194/ },
        { to_number: /6353125194/ },
        { to: /6353125194/ },
        { contact_phone: /6353125194/ },
        { "twilio.to": /6353125194/ },
      ],
    })
    .sort({ createdAt: -1 })
    .limit(8)
    .project({
      call_id: 1,
      status: 1,
      duration: 1,
      duration_ms: 1,
      creditsDeducted: 1,
      creditsDeductedAmount: 1,
      call_data: 1,
      twilio: 1,
      telnyx: 1,
      createdAt: 1,
      updatedAt: 1,
      campaign_id: 1,
      contactName: 1,
      phone_number: 1,
      mobileNumber: 1,
      to: 1,
      to_number: 1,
      callHangupAt: 1,
    })
    .toArray();
  console.log("TestCall", JSON.stringify(tests, null, 2));

  await client.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
