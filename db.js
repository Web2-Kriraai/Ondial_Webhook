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
        database.collection("contactprocessings").createIndex({ lead_id: 1, campaign_id: 1 }),
        database.collection(process.env.ERRORLOG_COLLECTION || "ErrorLog").createIndex({ createdAt: -1 }),
        database
            .collection(process.env.ERRORLOG_COLLECTION || "ErrorLog")
            .createIndex({ type: 1, createdAt: -1 }),
    ]);
    await ensureCallLogsLeadIdIndex(database, process.env.CALLLOGS_COLLECTION || "CallLogs");
    await ensureCallLogsLeadIdIndex(database, process.env.TESTCALL_COLLECTION || "TestCall");
    await ensureInboundCallLogIndexByCallId(
        database,
        process.env.INBOUNDCALLLOG_COLLECTION || "InboundConversation"
    );
}

/**
 * Ensure uniqueness only for meaningful lead_id values.
 * Historical rows may contain null/empty lead_id; partial unique index avoids
 * startup failures caused by duplicate nulls.
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

    const leadIndexes = indexes.filter((i) => {
        const k = i.key || {};
        return Object.keys(k).length === 1 && k.lead_id === 1;
    });
    const hasUsableUniqueLeadIdx = leadIndexes.some((i) => i.unique === true);
    const hasNonUniqueLeadIdx = leadIndexes.some((i) => i.unique !== true);

    // IMPORTANT:
    // If non-empty lead_id duplicates exist, keep/use a non-unique index and
    // skip unique-index attempts to avoid repeated startup failures.
    const duplicateLeadRows = await coll
        .aggregate([
            { $match: { lead_id: { $type: "string", $gt: "" } } },
            { $group: { _id: "$lead_id", count: { $sum: 1 } } },
            { $match: { count: { $gt: 1 } } },
            { $limit: 1 },
        ])
        .toArray();
    const duplicateLead = duplicateLeadRows[0]?._id;
    if (duplicateLead) {
        if (!hasNonUniqueLeadIdx) {
            await coll.createIndex({ lead_id: 1 }, { name: "lead_id_nonunique_fallback" });
        }
        logger.info(
            "[DB] Duplicate non-empty lead_id detected; using non-unique lead_id index",
            { collectionName, sampleLeadId: String(duplicateLead) }
        );
        return;
    }

    // Drop legacy non-unique lead_id indexes so planner prefers the unique partial one.
    for (const leadIdx of leadIndexes) {
        if (leadIdx?.unique) continue;
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

    if (hasUsableUniqueLeadIdx) {
        return;
    }

    try {
        await coll.createIndex(
            { lead_id: 1 },
            {
                name: "lead_id_unique_nonempty",
                unique: true,
                // Ignore null/empty lead IDs so historical sparse data doesn't
                // break unique index creation.
                partialFilterExpression: {
                    lead_id: {
                        // Keep only non-empty strings for uniqueness.
                        // This avoids unsupported $ne/$not partial expressions on older Mongo builds.
                        $type: "string",
                        $gt: "",
                    },
                },
            }
        );
        logger.info("[DB] Partial unique index on lead_id ensured", { collectionName });
    } catch (err) {
        const msg = String(err.message || "");
        const dupData =
            err.code === 11000 ||
            /duplicate key/i.test(msg) ||
            /E11000/i.test(msg) ||
            /duplicate key value/i.test(msg);

        if (dupData) {
            logger.warn(
                "[DB] Duplicate non-empty lead_id values; cannot use unique index — creating non-unique index",
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

/** Inbound logs are keyed by call_id (string), not lead_id. */
async function ensureInboundCallLogIndexByCallId(database, collectionName) {
    const coll = database.collection(collectionName);
    let indexes;
    try {
        indexes = await coll.indexes();
    } catch (err) {
        if (err.code === 26 || /ns does not exist/i.test(String(err.message || ""))) {
            await database.createCollection(collectionName);
            indexes = await coll.indexes();
        } else {
            throw err;
        }
    }

    const callIdx = indexes.find((i) => {
        const k = i.key || {};
        return Object.keys(k).length === 1 && k.call_id === 1;
    });

    if (callIdx && !callIdx.unique) {
        try {
            await coll.dropIndex(callIdx.name);
            logger.info("[DB] Dropped non-unique call_id index to replace with unique", {
                name: callIdx.name,
                collectionName,
            });
        } catch (err) {
            logger.warn("[DB] Could not drop call_id index", { error: err.message, collectionName });
        }
    }

    if (callIdx && callIdx.unique) {
        return;
    }

    try {
        await coll.createIndex({ call_id: 1 }, { unique: true });
        logger.info("[DB] Unique index on call_id ensured (inbound)", { collectionName });
    } catch (err) {
        const msg = String(err.message || "");
        const dupData =
            err.code === 11000 ||
            /duplicate key/i.test(msg) ||
            /E11000/i.test(msg) ||
            /duplicate key value/i.test(msg);

        if (dupData) {
            logger.warn(
                "[DB] Duplicate call_id values; cannot use unique index — creating non-unique index",
                { error: msg, collectionName }
            );
            await coll.createIndex({ call_id: 1 });
            return;
        }

        if (err.code === 85 || /IndexOptionsConflict|already exists/i.test(msg)) {
            logger.info("[DB] call_id index already present", { message: msg, collectionName });
            return;
        }

        throw err;
    }
}
