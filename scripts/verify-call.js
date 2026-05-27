require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });
const { MongoClient, ObjectId } = require("mongodb");

const CALL_ID = process.argv[2] || "53433625-9eb8-401e-96e6-e1228b3ff50f";
const CONTACT_ID = process.argv[3] || "6a169c9855ce49c2f99de43e";

(async () => {
    const client = new MongoClient(process.env.MONGODB_URI);
    await client.connect();
    const db = client.db();

    const callLog = await db.collection("CallLogs").findOne({
        $or: [{ lead_id: CALL_ID }, { call_id: CALL_ID }],
    });

    let contact = null;
    try {
        contact = await db.collection("contactprocessings").findOne(
            { _id: new ObjectId(CONTACT_ID) },
            {
                projection: {
                    callReceiveStatus: 1,
                    mobileNumber: 1,
                    campaign_id: 1,
                    config_id: 1,
                    status: 1,
                    updatedAt: 1,
                },
            }
        );
    } catch (e) {
        contact = { error: e.message };
    }

    const creditTx = await db
        .collection("credittransactions")
        .find({
            type: "call_deduction",
            $or: [
                { "reference.billingKey": { $regex: CALL_ID } },
                { "reference.callId": CALL_ID },
            ],
        })
        .sort({ createdAt: -1 })
        .limit(3)
        .toArray();

    const events = callLog?.call_data?.events || [];
    const eventTypes = events.map((e) => e.event_type);
    const expected = ["call_initiated", "call_ringing", "call_answered", "call_ended", "call_hangup"];

    const report = {
        call_unique_id: CALL_ID,
        contact_id: CONTACT_ID,
        callLogs: callLog
            ? {
                  found: true,
                  lead_id: callLog.lead_id,
                  call_id: callLog.call_id,
                  contact_id: callLog.contact_id,
                  campaign_id: callLog.campaign_id || null,
                  events_count: events.length,
                  event_types: eventTypes,
                  creditsDeducted: callLog.creditsDeducted ?? null,
                  creditsDeductedAmount: callLog.creditsDeductedAmount ?? null,
                  creditDeductionError: callLog.creditDeductionError ?? null,
                  updatedAt: callLog.updatedAt || null,
              }
            : { found: false },
        contactprocessings: contact
            ? {
                  found: true,
                  callReceiveStatus: contact.callReceiveStatus,
                  mobileNumber: contact.mobileNumber,
                  campaign_id: contact.campaign_id || null,
                  config_id: contact.config_id || null,
                  status: contact.status,
                  updatedAt: contact.updatedAt,
              }
            : { found: false },
        credittransactions: creditTx.map((t) => ({
            amount: t.amount,
            balanceAfter: t.balanceAfter,
            createdAt: t.createdAt,
            billingKey: t.reference?.billingKey,
            callDuration: t.reference?.callDuration,
        })),
        checks: {
            callLog_exists: !!callLog,
            contact_id_matches: callLog?.contact_id === CONTACT_ID,
            lead_id_matches_call_unique: callLog?.lead_id === CALL_ID,
            has_all_events: expected.every((t) => eventTypes.includes(t)),
            contact_status_completed: contact?.callReceiveStatus === 3,
            credits_deducted: callLog?.creditsDeducted === true,
        },
    };

    console.log(JSON.stringify(report, null, 2));
    await client.close();
})().catch((e) => {
    console.error(e.message);
    process.exit(1);
});
