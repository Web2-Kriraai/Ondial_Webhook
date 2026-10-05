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
  const id = "6abf68bc9bdac890710e85fb";
  const rows = await db
    .collection("contactprocessings")
    .find({ campaignId: id })
    .project({
      mobileNumber: 1,
      status: 1,
      callReceiveStatus: 1,
      sequenceNumber: 1,
      "contactData.name": 1,
      "contactData._wizardTestContact": 1,
      createdAt: 1,
      updatedAt: 1,
    })
    .toArray();
  console.log(JSON.stringify(rows, null, 2));
  const now = new Date();
  console.log({
    utc: now.toISOString(),
    est: now.toLocaleString("en-US", { timeZone: "America/New_York" }),
    ist: now.toLocaleString("en-IN", { timeZone: "Asia/Kolkata" }),
    callingWindowEst: "Fri 09:00-18:00",
  });
  await client.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
