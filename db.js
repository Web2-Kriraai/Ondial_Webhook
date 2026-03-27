const { MongoClient } = require("mongodb");
const logger = require("./logger");

const MONGODB_URI = process.env.MONGODB_URI;

if (!MONGODB_URI) {
    throw new Error("MONGODB_URI is not defined in .env");
}

let client = null;
let db = null;

async function connectDB() {
    if (db) return db;

    client = new MongoClient(MONGODB_URI);
    await client.connect();
    db = client.db(); // uses the DB name from the URI ("ondial")

    await ensureIndexes(db);
    logger.info("[DB] Connected to MongoDB");
    return db;
}

function getDb() {
    if (!db) throw new Error("DB not connected. Call connectDB() first.");
    return db;
}

module.exports = { connectDB, getDb };

async function ensureIndexes(database) {
    await Promise.all([
        database.collection("contactprocessings").createIndex({ mobileNumber: 1 }),
        database.collection("contactprocessings").createIndex({ updatedAt: -1 }),
    ]);
    await ensureCallLogsLeadIdIndex(database);
}

/**
 * CallLogs may already have a non-unique index { lead_id: 1 } named "lead_id_1".
 * Creating unique: true with the same key would conflict on the auto name.
 * We drop the old index when it is not unique, then create unique if data allows.
 */
async function ensureCallLogsLeadIdIndex(database) {
    const coll = database.collection("CallLogs");
    const indexes = await coll.indexes();

    const leadIdx = indexes.find((i) => {
        const k = i.key || {};
        return Object.keys(k).length === 1 && k.lead_id === 1;
    });

    if (leadIdx && !leadIdx.unique) {
        try {
            await coll.dropIndex(leadIdx.name);
            logger.info("[DB] Dropped non-unique CallLogs index to replace with unique", {
                name: leadIdx.name,
            });
        } catch (err) {
            logger.warn("[DB] Could not drop CallLogs lead_id index", { error: err.message });
        }
    }

    if (leadIdx && leadIdx.unique) {
        return;
    }

    try {
        await coll.createIndex({ lead_id: 1 }, { unique: true });
        logger.info("[DB] CallLogs unique index on lead_id ensured");
    } catch (err) {
        const msg = String(err.message || "");
        const dupData =
            err.code === 11000 ||
            /duplicate key/i.test(msg) ||
            /E11000/i.test(msg) ||
            /duplicate key value/i.test(msg);

        if (dupData) {
            logger.warn(
                "[DB] CallLogs has duplicate lead_id values; cannot use unique index — creating non-unique index",
                { error: msg }
            );
            await coll.createIndex({ lead_id: 1 });
            return;
        }

        if (err.code === 85 || /IndexOptionsConflict|already exists/i.test(msg)) {
            logger.info("[DB] CallLogs lead_id index already present", { message: msg });
            return;
        }

        throw err;
    }
}
