require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });
const { MongoClient } = require("mongodb");

const PHONE = "9484957189";

(async () => {
    const client = new MongoClient(process.env.MONGODB_URI);
    await client.connect();
    const db = client.db();
    const cols = (await db.listCollections().toArray()).map((c) => c.name);

    for (const col of cols) {
        try {
            const doc = await db.collection(col).findOne({
                $or: [
                    { to_number: { $regex: PHONE } },
                    { phoneNumber: { $regex: PHONE } },
                    { phone: { $regex: PHONE } },
                    { did: { $regex: PHONE } },
                    { inboundNumber: { $regex: PHONE } },
                ],
            });
            if (doc) {
                console.log(col, {
                    _id: String(doc._id),
                    keys: Object.keys(doc).slice(0, 20),
                    userId: doc.userId ? String(doc.userId) : null,
                    config_id: doc.config_id ? String(doc.config_id) : doc.configId ? String(doc.configId) : null,
                });
            }
        } catch {
            /* ignore */
        }
    }
    await client.close();
})().catch(console.error);
