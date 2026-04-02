const { getDb } = require("./db");
const logger = require("./logger");

const COLLECTION = process.env.ERRORLOG_COLLECTION || "ErrorLog";
const PAYLOAD_PREVIEW_MAX = 2000;

/**
 * Persist operational errors (e.g. webhook arrived but /api/call-mapping was missing or too late).
 */
async function logMissingCallMapping(entry) {
    try {
        const db = getDb();
        await db.collection(COLLECTION).insertOne({
            type: "missing_call_mapping",
            createdAt: new Date(),
            ...entry,
        });
        logger.warn("[ErrorLog] missing_call_mapping recorded", { source: entry.source, call_id: entry.call_id });
    } catch (err) {
        logger.error("[ErrorLog] insert failed", { error: err.message });
    }
}

function previewPayload(body) {
    if (body == null) return null;
    try {
        const s = JSON.stringify(body);
        if (s.length <= PAYLOAD_PREVIEW_MAX) return JSON.parse(s);
        return { truncated: true, preview: s.slice(0, PAYLOAD_PREVIEW_MAX) };
    } catch {
        return { serializationFailed: true };
    }
}

module.exports = { logMissingCallMapping, previewPayload };
