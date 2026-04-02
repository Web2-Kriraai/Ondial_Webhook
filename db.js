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
        database.collection(process.env.ERRORLOG_COLLECTION || "ErrorLog").createIndex({ createdAt: -1 }),
        database
            .collection(process.env.ERRORLOG_COLLECTION || "ErrorLog")
            .createIndex({ type: 1, createdAt: -1 }),
    ]);
    await ensureCallLogsLeadIdIndex(database, process.env.CALLLOGS_COLLECTION || "CallLogs");
    await ensureCallLogsLeadIdIndex(database, process.env.TESTCALL_COLLECTION || "TestCall");
}

/**
 * CallLogs may already have a non-unique index { lead_id: 1 } named "lead_id_1".
 * Creating unique: true with the same key would conflict on the auto name.
 * We drop the old index when it is not unique, then create unique if data allows.
 */
async function ensureCallLogsLeadIdIndex(database, collectionName) {
    const coll = database.collection(collectionName);
    let indexes;
    try {
        indexes = await coll.indexes();
    } catch (err) {
        // Collection does not exist yet (no documents) — create empty namespace for indexes.
        if (err.code === 26 || /ns does not exist/i.test(String(err.message || ""))) {
            await database.createCollection(collectionName);
            indexes = await coll.indexes();
        } else {
            throw err;
        }
    }

    const leadIdx = indexes.find((i) => {
        const k = i.key || {};
        return Object.keys(k).length === 1 && k.lead_id === 1;
    });

    if (leadIdx && !leadIdx.unique) {
        try {
            await coll.dropIndex(leadIdx.name);
            logger.info("[DB] Dropped non-unique lead_id index to replace with unique", {
                name: leadIdx.name,
                collectionName,
            });
        } catch (err) {
            logger.warn("[DB] Could not drop lead_id index", { error: err.message, collectionName });
        }
    }

    if (leadIdx && leadIdx.unique) {
        return;
    }

    try {
        await coll.createIndex({ lead_id: 1 }, { unique: true });
        logger.info("[DB] Unique index on lead_id ensured", { collectionName });
    } catch (err) {
        const msg = String(err.message || "");
        const dupData =
            err.code === 11000 ||
            /duplicate key/i.test(msg) ||
            /E11000/i.test(msg) ||
            /duplicate key value/i.test(msg);

        if (dupData) {
            logger.warn(
                "[DB] Duplicate lead_id values; cannot use unique index — creating non-unique index",
                { error: msg, collectionName }
            );
            await coll.createIndex({ lead_id: 1 });
            return;
        }

        if (err.code === 85 || /IndexOptionsConflict|already exists/i.test(msg)) {
            logger.info("[DB] lead_id index already present", { message: msg, collectionName });
            return;
        }

        throw err;
    }
}
