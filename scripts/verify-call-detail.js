require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });
const { MongoClient, ObjectId } = require("mongodb");

const CONTACT_ID = "6a169c9855ce49c2f99de43e";
const CALL_ID = "53433625-9eb8-401e-96e6-e1228b3ff50f";

(async () => {
    const client = new MongoClient(process.env.MONGODB_URI);
    await client.connect();
    const db = client.db();

    const contact = await db.collection("contactprocessings").findOne({ _id: new ObjectId(CONTACT_ID) });
    const byPhone = await db
        .collection("contactprocessings")
        .find({ mobileNumber: { $regex: "6353125194" } })
        .project({ callReceiveStatus: 1, mobileNumber: 1, status: 1, updatedAt: 1, campaign_id: 1 })
        .toArray();

    const hangupEvent = await db.collection("CallLogs").findOne(
        { lead_id: CALL_ID },
        { projection: { "call_data.events": 1 } }
    );
    const events = hangupEvent?.call_data?.events || [];
    const hangup = events.find((e) => e.event_type === "call_hangup");

    console.log(
        JSON.stringify(
            {
                contact_full: {
                    _id: String(contact?._id),
                    callReceiveStatus: contact?.callReceiveStatus,
                    status: contact?.status,
                    mobileNumber: contact?.mobileNumber,
                    campaign_id: contact?.campaign_id,
                    lead_id: contact?.lead_id,
                    updatedAt: contact?.updatedAt,
                },
                contacts_with_same_phone: byPhone.map((c) => ({
                    _id: String(c._id),
                    callReceiveStatus: c.callReceiveStatus,
                    status: c.status,
                    updatedAt: c.updatedAt,
                })),
                hangup_event_duration: hangup?.data?.duration ?? hangup?.data?.durationSec,
                hangup_callStatus: hangup?.data?.callStatus,
            },
            null,
            2
        )
    );
    await client.close();
})();
