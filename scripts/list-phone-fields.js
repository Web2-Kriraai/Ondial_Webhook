require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });
const { MongoClient } = require("mongodb");

(async () => {
    const client = new MongoClient(process.env.MONGODB_URI);
    await client.connect();
    const db = client.db();
    const cols = (await db.listCollections().toArray()).map((c) => c.name).sort();

    for (const col of cols) {
        const doc = await db.collection(col).findOne({});
        if (!doc) continue;
        const keys = Object.keys(doc);
        const phoneKeys = keys.filter((k) => /phone|did|number|mobile/i.test(k));
        if (phoneKeys.length) {
            console.log(col, "phoneKeys:", phoneKeys, "sample:", phoneKeys.slice(0, 3).map((k) => doc[k]));
        }
    }
    await client.close();
})().catch(console.error);
