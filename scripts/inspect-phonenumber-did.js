require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });
const { MongoClient } = require("mongodb");
const { normalizePhone } = require("../callMapping");

const TO = process.argv[2] || "919484957189";

(async () => {
    const client = new MongoClient(process.env.MONGODB_URI);
    await client.connect();
    const db = client.db();

    const norm = normalizePhone(TO);
    const variants = new Set([
        TO,
        norm,
        `+91${norm}`,
        `+${TO}`,
        `91${norm}`,
    ]);

    const pn = await db.collection("phonenumbers").findOne({
        number: { $in: [...variants] },
    });
    console.log("phonenumbers match:", pn ? JSON.stringify(pn, null, 2) : null);

    await client.close();
})().catch(console.error);
