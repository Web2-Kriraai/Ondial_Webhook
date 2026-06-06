require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });
const { MongoClient } = require("mongodb");
const { phoneVariants } = require("../lib/resolveInboundBillingContext");

const TO = "919484957189";
const FROM = "7874748401";
const CALL_SID = "call_a619c62b85144a9c84452ba1c37aff59";

(async () => {
    const client = new MongoClient(process.env.MONGODB_URI);
    await client.connect();
    const db = client.db();
    const col = db.collection("InboundConversation");

    const toVars = phoneVariants(TO);
    const fromVars = phoneVariants(FROM);

    console.log("toVars", toVars);

    const queries = {
        by_source_to: await col.find({ source: "validate", to_number: { $in: toVars } }).sort({ startedAt: -1 }).limit(3).toArray(),
        by_to_only: await col
            .find({ to_number: { $in: toVars } })
            .sort({ startedAt: -1 })
            .limit(5)
            .toArray(),
        by_call_sid: await col.find({ call_sid: CALL_SID }).toArray(),
        by_userId_recent: await col
            .find({ userId: { $exists: true, $ne: null }, to_number: { $in: toVars } })
            .sort({ startedAt: -1 })
            .limit(5)
            .toArray(),
        by_internal_call_id: await col
            .find({ call_id: /^[a-f0-9]{24}$/i, to_number: { $in: toVars } })
            .sort({ startedAt: -1 })
            .limit(5)
            .toArray(),
    };

    for (const [name, docs] of Object.entries(queries)) {
        console.log(
            `\n${name}:`,
            docs.map((d) => ({
                _id: String(d._id),
                call_id: d.call_id,
                call_sid: d.call_sid,
                source: d.source,
                to: d.to_number || d.phone_number,
                from: d.from_number,
                userId: d.userId ? String(d.userId) : null,
                startedAt: d.startedAt,
                createdAt: d.createdAt,
            }))
        );
    }

    await client.close();
})().catch(console.error);
