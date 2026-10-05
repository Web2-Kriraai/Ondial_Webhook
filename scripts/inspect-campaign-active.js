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
  const client = new MongoClient(process.env.MONGODB_URI);
  await client.connect();
  const db = client.db();

  const oid = new ObjectId(id);
  const campaign =
    (await db.collection("Campaign").findOne({ _id: oid })) ||
    (await db.collection("campaigns").findOne({ _id: oid })) ||
    (await db.collection("Campaigns").findOne({ _id: oid }));

  if (!campaign) {
    // find collection
    for (const name of ["Campaign", "campaigns", "Campaigns", "campaign"]) {
      const c = await db.collection(name).findOne({ _id: oid }).catch(() => null);
      if (c) {
        console.log("found in", name);
        break;
      }
    }
  }

  const colNames = await db.listCollections().toArray();
  const campCols = colNames.map((c) => c.name).filter((n) => /camp/i.test(n));
  console.log("campaign collections:", campCols);

  let doc = null;
  let foundCol = null;
  for (const name of campCols.length ? campCols : ["Campaign", "campaigns", "Campaigns"]) {
    doc =
      (await db.collection(name).findOne({ _id: oid })) ||
      (await db.collection(name).findOne({ _id: id }));
    if (doc) {
      foundCol = name;
      break;
    }
  }

  if (!doc) {
    console.log("Campaign not found for", id);
    await client.close();
    return;
  }

  const keys = [
    "name",
    "campaignName",
    "status",
    "campaignStatus",
    "state",
    "archive",
    "archived",
    "isActive",
    "active",
    "paused",
    "isPaused",
    "isForeign",
    "provider",
    "telephonyProvider",
    "voiceProvider",
    "concurrency",
    "maxConcurrency",
    "concurrent_calls",
    "dialMode",
    "schedule",
    "timezone",
    "startTime",
    "endTime",
    "callingHours",
    "contactCount",
    "totalContacts",
    "processedContacts",
    "remainingContacts",
    "createdBy",
    "userId",
    "user_id",
    "companyId",
    "orgId",
    "did",
    "fromNumber",
    "callerId",
    "phoneNumber",
    "numberPool",
    "agentId",
    "agent_id",
    "pipelineStatus",
    "runStatus",
    "lastError",
    "error",
    "blockedReason",
    "credits",
    "createdAt",
    "updatedAt",
    "startedAt",
    "pausedAt",
    "resumedAt",
  ];

  const summary = { _id: String(doc._id), collection: foundCol };
  for (const k of keys) {
    if (doc[k] !== undefined) summary[k] = doc[k];
  }
  // also dump top-level status-ish fields
  for (const [k, v] of Object.entries(doc)) {
    if (/status|pause|active|archive|schedule|concurr|provider|foreign|did|from|caller|pool|error|block|credit|remain|process|contact|test/i.test(k)) {
      if (summary[k] === undefined) summary[k] = v;
    }
  }

  console.log("CAMPAIGN_SUMMARY", JSON.stringify(summary, null, 2));

  // contacts
  const contactCols = colNames
    .map((c) => c.name)
    .filter((n) => /contact|lead/i.test(n));
  console.log("contact-ish collections:", contactCols.slice(0, 20));

  for (const name of ["CampaignContact", "campaign_contacts", "Contacts", "contacts", "Leads", "leads"]) {
    if (!contactCols.includes(name) && !(await db.listCollections({ name }).hasNext())) continue;
    const qVariants = [
      { campaign_id: id },
      { campaignId: id },
      { campaign_id: oid },
      { campaignId: oid },
    ];
    for (const q of qVariants) {
      const n = await db.collection(name).countDocuments(q).catch(() => -1);
      if (n > 0) {
        const statuses = await db
          .collection(name)
          .aggregate([
            { $match: q },
            { $group: { _id: { status: "$status", callStatus: "$callStatus", receiveStatus: "$callReceiveStatus", processed: "$processed" }, n: { $sum: 1 } } },
            { $sort: { n: -1 } },
            { $limit: 20 },
          ])
          .toArray();
        console.log(`CONTACTS ${name}`, JSON.stringify({ query: q, count: n, statuses }, null, 2));
        const sample = await db
          .collection(name)
          .find(q)
          .sort({ updatedAt: -1, createdAt: -1 })
          .limit(5)
          .project({
            name: 1,
            mobileNumber: 1,
            phone: 1,
            status: 1,
            callStatus: 1,
            callReceiveStatus: 1,
            processed: 1,
            lastCallAt: 1,
            nextCallAt: 1,
            locked: 1,
            inProgress: 1,
            error: 1,
            updatedAt: 1,
          })
          .toArray();
        console.log(`SAMPLE ${name}`, JSON.stringify(sample, null, 2));
        break;
      }
    }
  }

  // recent call logs / test calls for campaign
  const logs = await db
    .collection("CallLogs")
    .find({ campaign_id: id })
    .sort({ createdAt: -1 })
    .limit(8)
    .project({
      call_id: 1,
      status: 1,
      isTestCall: 1,
      createdAt: 1,
      to_number: 1,
      "twilio.status": 1,
      "twilio.duration": 1,
      creditsDeductedAmount: 1,
    })
    .toArray();
  console.log("RECENT_CALLLOGS", JSON.stringify(logs, null, 2));

  const tests = await db
    .collection("TestCall")
    .find({ campaign_id: id })
    .sort({ createdAt: -1 })
    .limit(5)
    .project({ call_id: 1, status: 1, createdAt: 1, updatedAt: 1, to_number: 1, callHangupAt: 1 })
    .toArray();
  console.log("RECENT_TESTCALL", JSON.stringify(tests, null, 2));

  // dialer / queue jobs if any
  for (const name of ["DialQueue", "dial_queue", "CallQueue", "campaign_jobs", "CampaignJob", "jobs"]) {
    const exists = await db.listCollections({ name }).hasNext();
    if (!exists) continue;
    const n = await db
      .collection(name)
      .countDocuments({
        $or: [{ campaign_id: id }, { campaignId: id }, { campaign_id: oid }, { campaignId: oid }],
      })
      .catch(() => 0);
    if (n > 0) {
      const rows = await db
        .collection(name)
        .find({
          $or: [{ campaign_id: id }, { campaignId: id }, { campaign_id: oid }, { campaignId: oid }],
        })
        .sort({ createdAt: -1 })
        .limit(5)
        .toArray();
      console.log(`QUEUE ${name} count=${n}`, JSON.stringify(rows, null, 2).slice(0, 3000));
    }
  }

  await client.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
