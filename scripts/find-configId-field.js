require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });
const { MongoClient } = require("mongodb");

(async () => {
    const client = new MongoClient(process.env.MONGODB_URI);
    await client.connect();
    const db = client.db();
    const cols = (await db.listCollections().toArray()).map((c) => c.name);

    for (const col of cols) {
        if (col.startsWith("system.")) continue;
        try {
            const doc = await db.collection(col).findOne({ configId: { $exists: true } });
            if (doc) {
                console.log(col, {
                    keys: Object.keys(doc).slice(0, 25),
                    userId: doc.userId ? String(doc.userId) : null,
                    phone: doc.phoneNumber || doc.phone || doc.inboundNumber || doc.did || null,
                });
            }
        } catch {
            /* ignore */
        }
    }
    await client.close();
})().catch(console.error);
