require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });
const { MongoClient, ObjectId } = require("mongodb");

const CONFIG_ID = process.argv[2] || "69bba8558ad17e7c45776786";

(async () => {
    const client = new MongoClient(process.env.MONGODB_URI);
    await client.connect();
    const db = client.db();
    const id = new ObjectId(CONFIG_ID);
    const cols = (await db.listCollections().toArray()).map((c) => c.name);

    for (const col of cols) {
        if (col.startsWith("system.")) continue;
        try {
            const byId = await db.collection(col).findOne({ _id: id });
            if (byId) {
                console.log("by _id in", col, {
                    keys: Object.keys(byId),
                    userId: byId.userId ? String(byId.userId) : null,
                    createdBy: byId.createdBy || null,
                });
            }
            const byConfigId = await db.collection(col).findOne({ configId: id });
            if (byConfigId) {
                console.log("by configId in", col, {
                    _id: String(byConfigId._id),
                    userId: byConfigId.userId ? String(byConfigId.userId) : null,
                });
            }
        } catch {
            /* ignore */
        }
    }
    await client.close();
})().catch(console.error);
