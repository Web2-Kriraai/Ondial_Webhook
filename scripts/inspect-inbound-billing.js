require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });
const { MongoClient, ObjectId } = require("mongodb");

(async () => {
    const client = new MongoClient(process.env.MONGODB_URI);
    await client.connect();
    const db = client.db();

    const cfgId = "6a141d63c66e079a0fea84d5";
    const cols = (await db.listCollections().toArray()).map((c) => c.name);
    for (const col of cols) {
        try {
            const d = await db.collection(col).findOne({ _id: new ObjectId(cfgId) });
            if (d) console.log("config in", col, Object.keys(d));
        } catch {
            /* ignore */
        }
    }

    const billed = await db.collection("InboundConversation").findOne({
        config_id: new ObjectId(cfgId),
        creditsDeducted: true,
    });
    console.log("billed inbound sample:", billed ? { call_id: billed.call_id, amount: billed.creditsDeductedAmount } : null);

    const tx = await db
        .collection("credittransactions")
        .find({ type: "call_deduction", "reference.source": /inbound/i })
        .sort({ createdAt: -1 })
        .limit(3)
        .toArray();
    console.log("inbound credit txs:", tx.map((t) => t.reference));

    await client.close();
})().catch(console.error);
