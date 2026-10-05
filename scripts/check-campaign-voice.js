const path = require("path");
const fs = require("fs");
const { MongoClient, ObjectId } = require("mongodb");

function loadEnv() {
  for (const p of [
    path.join(__dirname, "../../Ondial/.env"),
    path.join(__dirname, "../.env"),
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
  const camp = await db.collection("campaigns").findOne(
    { _id: new ObjectId("6abf68bc9bdac890710e85fb") },
    {
      projection: {
        campaignName: 1,
        agentName: 1,
        selectedVoice: 1,
        voice: 1,
        voiceId: 1,
        voiceName: 1,
        status: 1,
        updatedAt: 1,
      },
    }
  );
  console.log(JSON.stringify(camp, null, 2));
  await client.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
