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
  const camp = await db.collection("campaigns").findOne(
    { _id: oid },
    {
      projection: {
        campaignName: 1,
        status: 1,
        isPaused: 1,
        startDate: 1,
        startTime: 1,
        endDate: 1,
        endTime: 1,
        timezone: 1,
        immediateStart: 1,
        customizeSchedule: 1,
        tillCallsComplete: 1,
        callingHours: 1,
        selectedPhoneNumber: 1,
        selectedPhoneNumberId: 1,
        numberPolicySnapshot: 1,
        selectedVoice: 1,
        agentName: 1,
        isForeign: 1,
        contactProcessingStatus: 1,
        pausedForInsufficientCredits: 1,
        pausedForScriptGeneration: 1,
        servicePrompts: 1,
        isCustomScript: 1,
        formattedStartDateTime: 1,
        currentUTCTimestamp: 1,
        currentUserTimezoneTime: 1,
        businessHours: 1,
        maxRetryAttempts: 1,
        retryDelayMinutes: 1,
        nextCallLogsScanAt: 1,
        updatedAt: 1,
      },
    }
  );
  console.log(JSON.stringify(camp, null, 2));

  const pending = await db
    .collection("contactprocessings")
    .find({ campaignId: id, status: { $in: ["pending", "queued", "ready", "retry", "processing"] } })
    .project({ name: 1, mobileNumber: 1, status: 1, callReceiveStatus: 1, sequenceNumber: 1, contactData: 1, nextRetryAt: 1, createdAt: 1 })
    .toArray();
  console.log("PENDING_CONTACTS", JSON.stringify(pending, null, 2));

  const allCp = await db
    .collection("contactprocessings")
    .aggregate([
      { $match: { campaignId: id } },
      { $group: { _id: "$status", n: { $sum: 1 } } },
    ])
    .toArray();
  console.log("STATUS_BREAKDOWN", allCp);

  // phone number doc
  if (camp.selectedPhoneNumberId) {
    for (const name of ["phonenumbers", "PhoneNumbers", "numbers", "Numbers", "twilio_numbers", "telnyx_numbers", "company_numbers"]) {
      const exists = await db.listCollections({ name }).hasNext();
      if (!exists) continue;
      let doc =
        (await db.collection(name).findOne({ _id: new ObjectId(String(camp.selectedPhoneNumberId)) }).catch(() => null)) ||
        (await db.collection(name).findOne({ _id: camp.selectedPhoneNumberId }).catch(() => null)) ||
        (await db.collection(name).findOne({ phoneNumber: camp.selectedPhoneNumber }).catch(() => null));
      if (doc) {
        console.log("NUMBER_DOC", name, JSON.stringify(doc, null, 2).slice(0, 2000));
        break;
      }
    }
  }

  await client.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
