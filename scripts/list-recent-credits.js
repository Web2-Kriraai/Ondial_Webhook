require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });
const { MongoClient } = require("mongodb");

(async () => {
    const c = new MongoClient(process.env.MONGODB_URI);
    await c.connect();
    const db = c.db();
    const txs = await db
        .collection("credittransactions")
        .find({
            type: "call_deduction",
            createdAt: { $gte: new Date("2026-05-27T07:40:00.000Z") },
        })
        .sort({ createdAt: -1 })
        .limit(15)
        .toArray();
    for (const t of txs) {
        console.log({
            amount: t.amount,
            balanceAfter: t.balanceAfter,
            description: t.description,
            createdAt: t.createdAt,
            callDuration: t.reference?.callDuration,
            billingKey: t.reference?.billingKey,
            callId: t.reference?.callId,
            source: t.reference?.source,
        });
    }
    await c.close();
})();
