const path = require("path");
const fs = require("fs");
const { MongoClient, ObjectId } = require("mongodb");

function loadEnvFile(p) {
  const out = {};
  if (!fs.existsSync(p)) return out;
  for (const line of fs.readFileSync(p, "utf8").split(/\r?\n/)) {
    const m = line.match(/^([^#=]+)=(.*)$/);
    if (!m) continue;
    out[m[1].trim()] = m[2].trim().replace(/^["']|["']$/g, "");
  }
  return out;
}

(async () => {
  const id = "6abf68bc9bdac890710e85fb";
  const oid = new ObjectId(id);
  const ond = loadEnvFile(path.join(__dirname, "../../Ondial/.env"));
  const wh = loadEnvFile(path.join(__dirname, "../.env"));
  const uri = ond.MONGODB_URI || wh.MONGODB_URI;
  console.log("uri host", String(uri).replace(/:\/\/[^@]+@/, "://***@").slice(0, 120));
  console.log("db name hint", ond.MONGODB_DB || wh.MONGODB_DB || "(default)");

  const client = new MongoClient(uri);
  await client.connect();
  const dbName = ond.MONGODB_DB || wh.MONGODB_DB || undefined;
  const db = dbName ? client.db(dbName) : client.db();
  console.log("using db", db.databaseName);

  const cols = (await db.listCollections().toArray()).map((c) => c.name);
  const cpCols = cols.filter((n) => /contact/i.test(n));
  console.log("contact cols", cpCols);

  for (const name of cpCols) {
    const n1 = await db.collection(name).countDocuments({ campaignId: id });
    const n2 = await db.collection(name).countDocuments({ campaign_id: id });
    const n3 = await db.collection(name).countDocuments({ campaignId: oid });
    const n4 = await db.collection(name).countDocuments({
      $or: [
        { campaignId: id },
        { campaign_id: id },
        { campaignId: oid },
        { campaign_id: oid },
      ],
    });
    if (n1 || n2 || n3 || n4) {
      console.log(name, { n1, n2, n3, n4 });
      const sample = await db
        .collection(name)
        .find({
          $or: [
            { campaignId: id },
            { campaign_id: id },
            { campaignId: oid },
            { campaign_id: oid },
          ],
        })
        .limit(5)
        .project({
          mobileNumber: 1,
          status: 1,
          callReceiveStatus: 1,
          campaignId: 1,
          campaign_id: 1,
          "contactData.name": 1,
          "contactData._wizardTestContact": 1,
        })
        .toArray();
      console.log(JSON.stringify(sample, null, 2));
    }
  }

  // also search by mobile
  for (const name of cpCols) {
    const byMobile = await db
      .collection(name)
      .find({ mobileNumber: /6352617754|6353125194/ })
      .project({ campaignId: 1, campaign_id: 1, mobileNumber: 1, status: 1, "contactData.name": 1 })
      .limit(10)
      .toArray();
    if (byMobile.length) console.log("byMobile", name, JSON.stringify(byMobile, null, 2));
  }

  const camp = await db.collection("campaigns").findOne(
    { _id: oid },
    {
      projection: {
        campaignName: 1,
        status: 1,
        isPaused: 1,
        timezone: 1,
        startDate: 1,
        startTime: 1,
        endDate: 1,
        endTime: 1,
        callingHours: 1,
        immediateStart: 1,
        contactProcessingStatus: 1,
        selectedPhoneNumber: 1,
        pausedForInsufficientCredits: 1,
        pausedForScriptGeneration: 1,
        csvValidContacts: 1,
        manualContacts: 1,
        updatedAt: 1,
      },
    }
  );
  console.log("CAMP", JSON.stringify(camp, null, 2));

  const now = new Date();
  console.log("NOW", {
    utc: now.toISOString(),
    est: now.toLocaleString("en-US", { timeZone: "America/New_York" }),
    ist: now.toLocaleString("en-IN", { timeZone: "Asia/Kolkata" }),
  });

  await client.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
