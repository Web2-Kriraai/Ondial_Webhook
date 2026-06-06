require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });
const { MongoClient, ObjectId } = require("mongodb");

(async () => {
    const client = new MongoClient(process.env.MONGODB_URI);
    await client.connect();
    const db = client.db();

    const cfgId = "6a23d3101c786903f717ad28";
    const camp = await db.collection("campaigns").findOne({ _id: new ObjectId(cfgId) });
    console.log("campaign by validate config_id:", camp ? { name: camp.name, createdBy: camp.createdBy } : null);

    const cols = (await db.listCollections().toArray()).map((c) => c.name);
    for (const col of cols) {
        try {
            const doc = await db.collection(col).findOne({ _id: new ObjectId(cfgId) });
            if (doc) console.log("found config_id in collection:", col, Object.keys(doc).slice(0, 12));
        } catch {
            /* ignore */
        }
    }

    const inboundWithCfg = await db
        .collection("InboundConversation")
        .find({ config_id: { $exists: true, $ne: null } })
        .limit(2)
        .toArray();
    console.log(
        "InboundConversation with config_id:",
        inboundWithCfg.map((d) => ({
            call_id: d.call_id,
            source: d.source,
            config_id: String(d.config_id),
            userId: d.userId ? String(d.userId) : null,
        }))
    );

    const recent = await db
        .collection("InboundConversation")
        .find({ config_id: { $exists: true, $ne: null } })
        .sort({ updatedAt: -1 })
        .limit(5)
        .toArray();
    console.log(
        "recent with config_id:",
        recent.map((x) => ({
            call_id: x.call_id,
            source: x.source,
            config_id: String(x.config_id),
            userId: x.userId ? String(x.userId) : null,
            to: x.to_number,
        }))
    );

    await client.close();
})().catch((e) => {
    console.error(e);
    process.exit(1);
});
