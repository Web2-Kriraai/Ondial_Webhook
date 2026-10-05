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
  const id = "6abf68bc9bdac890710e85fb";
  const oid = new ObjectId(id);
  const client = new MongoClient(process.env.MONGODB_URI);
  await client.connect();
  const db = client.db();

  const camp = await db.collection("campaigns").findOne(
    { _id: oid },
    {
      projection: {
        campaignName: 1,
        status: 1,
        isPaused: 1,
        timezone: 1,
        callingHours: 1,
        startDate: 1,
        startTime: 1,
        endDate: 1,
        endTime: 1,
        immediateStart: 1,
        concurrentCalls: 1,
        selectedPhoneNumber: 1,
        contactProcessingStatus: 1,
        pausedForInsufficientCredits: 1,
        pausedForScriptGeneration: 1,
        createdBy: 1,
        updatedAt: 1,
        lastDialAt: 1,
        lastEnqueuedAt: 1,
        dialerState: 1,
        cs1: 1,
        scheduler: 1,
      },
    }
  );
  console.log("CAMPAIGN", JSON.stringify(camp, null, 2));

  // CS1-ish collections
  const cols = (await db.listCollections().toArray()).map((c) => c.name);
  const interesting = cols.filter((n) =>
    /dial|queue|job|bull|schedul|slot|worker|call.?attempt|enqueue/i.test(n)
  );
  console.log("interesting cols", interesting);

  for (const name of interesting) {
    const n = await db
      .collection(name)
      .countDocuments({
        $or: [
          { campaignId: id },
          { campaign_id: id },
          { campaignId: oid },
          { campaign_id: oid },
          { "data.campaignId": id },
          { "data.campaign_id": id },
        ],
      })
      .catch(() => 0);
    if (n > 0) {
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
        .sort({ updatedAt: -1, createdAt: -1, timestamp: -1 })
        .limit(3)
        .toArray();
      console.log(`COL ${name} n=${n}`, JSON.stringify(sample, null, 2).slice(0, 2500));
    }
  }

  // Contact Hit detail — CS1 status fields
  const contacts = await db
    .collection("contactprocessings")
    .find({
      $or: [{ campaignId: oid }, { campaignId: id }],
    })
    .toArray();
  for (const c of contacts) {
    console.log("CONTACT", {
      id: String(c._id),
      name: c.contactData?.name,
      mobile: c.mobileNumber,
      status: c.status,
      callReceiveStatus: c.callReceiveStatus,
      wizardTest: !!c.contactData?._wizardTestContact,
      nextRetryAt: c.nextRetryAt,
      scheduledAt: c.scheduledAt,
      enqueuedAt: c.enqueuedAt,
      lastCallAttempt: c.lastCallAttempt,
      dialQueuedAt: c.dialQueuedAt,
      priority: c.priority,
      isFollowUp: c.isFollowUp,
      skipReason: c.skipReason,
      lastError: c.lastError || c.error,
      retryCount: c.retryCount,
      updatedAt: c.updatedAt,
      createdAt: c.createdAt,
    });
  }

  // Recent non-test CallLogs for this campaign (CS1 dials)
  const logs = await db
    .collection("CallLogs")
    .find({
      campaign_id: id,
      isTestCall: { $ne: true },
    })
    .sort({ createdAt: -1 })
    .limit(5)
    .project({
      call_id: 1,
      status: 1,
      createdAt: 1,
      to_number: 1,
      contact_id: 1,
      isTestCall: 1,
    })
    .toArray();
  console.log("NON_TEST_CALLLOGS", logs);

  const allLogs = await db.collection("CallLogs").countDocuments({ campaign_id: id });
  const testLogs = await db.collection("CallLogs").countDocuments({ campaign_id: id, isTestCall: true });
  console.log("calllog counts", { allLogs, testLogs, nonTest: allLogs - testLogs });

  // Time window check mirroring CS1 expectation
  const now = new Date();
  const estParts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    weekday: "long",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(now);
  const wd = estParts.find((p) => p.type === "weekday")?.value;
  const hour = Number(estParts.find((p) => p.type === "hour")?.value);
  const minute = Number(estParts.find((p) => p.type === "minute")?.value);
  const mins = hour * 60 + minute;
  const inWindow = wd !== "Sunday" && mins >= 9 * 60 && mins < 18 * 60;
  console.log("EST_WINDOW_CHECK", { wd, hour, minute, mins, inWindow, open: "09:00", close: "18:00" });
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
