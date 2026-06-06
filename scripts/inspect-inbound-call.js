require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });
const { MongoClient } = require("mongodb");

const CALL_SID = process.argv[2] || "call_e5832b6030624a88b76c802cee00f5f3";
const PROVIDER_ID = process.argv[3] || "e5832b60-3062-4a88-b76c-802cee00f5f3";

(async () => {
    const client = new MongoClient(process.env.MONGODB_URI);
    await client.connect();
    const db = client.db();

    const docs = await db
        .collection("InboundConversation")
        .find({ $or: [{ call_sid: CALL_SID }, { call_id: PROVIDER_ID }] })
        .toArray();

    console.log(
        "InboundConversation docs:",
        JSON.stringify(
            docs.map((d) => ({
                _id: String(d._id),
                call_id: d.call_id,
                call_sid: d.call_sid,
                config_id: d.config_id != null ? String(d.config_id) : null,
                configId: d.configId != null ? String(d.configId) : null,
                inboundConfigId: d.inboundConfigId != null ? String(d.inboundConfigId) : null,
                userId: d.userId != null ? String(d.userId) : null,
                source: d.source,
                to_number: d.to_number,
                creditsDeducted: d.creditsDeducted,
                events: d.call_data?.events?.length || 0,
            })),
            null,
            2
        )
    );

    const cols = await db.listCollections().toArray();
    const withUser = await db.collection("InboundConversation").findOne({
        userId: { $exists: true, $ne: null },
    });
    console.log(
        "sample with userId:",
        withUser
            ? {
                  call_id: withUser.call_id,
                  call_sid: withUser.call_sid,
                  source: withUser.source,
                  config_id: withUser.config_id ? String(withUser.config_id) : null,
                  userId: String(withUser.userId),
                  to: withUser.to_number,
                  creditsDeducted: withUser.creditsDeducted,
              }
            : null
    );

    const didDocs = await db
        .collection("InboundConversation")
        .find({ to_number: { $regex: "9484957189" } })
        .limit(3)
        .toArray();
    console.log(
        "DID docs:",
        didDocs.map((d) => ({
            call_id: d.call_id,
            source: d.source,
            config_id: d.config_id ? String(d.config_id) : null,
            userId: d.userId ? String(d.userId) : null,
        }))
    );

    const { resolveCampaignIdFromInboundDid } = require("../lib/resolveInboundBillingContext");
    const campaignId = await resolveCampaignIdFromInboundDid("919484957189");
    console.log("campaign from DID 919484957189:", campaignId);

    await client.close();})().catch((e) => {
    console.error(e.message);
    process.exit(1);
});
