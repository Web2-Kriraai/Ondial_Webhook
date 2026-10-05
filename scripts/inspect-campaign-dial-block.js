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

(async () => {
  loadEnv();
  const id = "6abf68bc9bdac890710e85fb";
  const oid = new ObjectId(id);
  const client = new MongoClient(process.env.MONGODB_URI);
  await client.connect();
  const db = client.db();

  const camp = await db.collection("campaigns").findOne({ _id: oid });
  console.log("provider/DID fields", {
    telephonyProvider: camp?.telephonyProvider,
    provider: camp?.provider,
    voiceProvider: camp?.voiceProvider,
    fromNumber: camp?.fromNumber,
    callerId: camp?.callerId,
    didNumber: camp?.didNumber,
    phoneNumber: camp?.phoneNumber,
    selectedNumber: camp?.selectedNumber,
    numberId: camp?.numberId,
    telnyxNumber: camp?.telnyxNumber,
    twilioNumber: camp?.twilioNumber,
    foreignProvider: camp?.foreignProvider,
    market: camp?.market,
    country: camp?.country,
    destinationCountry: camp?.destinationCountry,
    contactImportCountryIsoOverride: camp?.contactImportCountryIsoOverride,
    agentId: camp?.agentId,
    scriptId: camp?.scriptId,
    scriptStatus: camp?.scriptStatus,
    scriptReady: camp?.scriptReady,
    hasScript: !!camp?.script,
    numbers: camp?.numbers,
    assignedNumbers: camp?.assignedNumbers,
    dialerLocked: camp?.dialerLocked,
    dialLock: camp?.dialLock,
    nextDialAt: camp?.nextDialAt,
    lastDialAttemptAt: camp?.lastDialAttemptAt,
    lastError: camp?.lastError,
    dialError: camp?.dialError,
    queuePaused: camp?.queuePaused,
  });

  // dump all keys
  console.log("ALL_KEYS", Object.keys(camp).sort());

  // contactprocessings
  const cp = await db
    .collection("contactprocessings")
    .find({
      $or: [{ campaign_id: id }, { campaignId: id }, { campaign_id: oid }, { campaignId: oid }],
    })
    .limit(10)
    .toArray();
  console.log("contactprocessings", JSON.stringify(cp, null, 2).slice(0, 5000));

  // search contacts by campaign in any collection
  const cols = (await db.listCollections().toArray()).map((c) => c.name);
  for (const name of cols) {
    if (!/contact|lead|list|import/i.test(name)) continue;
    const n = await db
      .collection(name)
      .countDocuments({
        $or: [
          { campaign_id: id },
          { campaignId: id },
          { campaign_id: oid },
          { campaignId: oid },
          { campaign: id },
          { campaign: oid },
        ],
      })
      .catch(() => 0);
    if (n > 0) {
      const sample = await db
        .collection(name)
        .find({
          $or: [
            { campaign_id: id },
            { campaignId: id },
            { campaign_id: oid },
            { campaignId: oid },
            { campaign: id },
            { campaign: oid },
          ],
        })
        .limit(3)
        .toArray();
      console.log(`FOUND ${name} n=${n}`, JSON.stringify(sample, null, 2).slice(0, 2500));
    }
  }

  // campaign document may embed contacts
  if (Array.isArray(camp.contacts)) {
    console.log("embedded contacts", camp.contacts.length, JSON.stringify(camp.contacts.slice(0, 3), null, 2));
  }
  if (camp.contactList) console.log("contactList", typeof camp.contactList, JSON.stringify(camp.contactList).slice(0, 1000));

  // time check
  const now = new Date();
  console.log("NOW_UTC", now.toISOString());
  console.log("NOW_IST", now.toLocaleString("en-IN", { timeZone: "Asia/Kolkata" }));
  console.log("NOW_EST", now.toLocaleString("en-US", { timeZone: "America/New_York" }));
  console.log("campaign start/end", camp.startTime, camp.endTime, "tz", camp.timezone);
  console.log("callingHours.enabled", camp.callingHours?.enabled);
  console.log("friday", camp.callingHours?.schedule?.friday);
  console.log("sunday closed?", camp.callingHours?.schedule?.sunday?.closed);

  await client.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
