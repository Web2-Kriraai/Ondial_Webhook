const { getDb } = require("./db");
const logger = require("./logger");

const CALLLOGS_COLLECTION = process.env.CALLLOGS_COLLECTION || "CallLogs";
const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";

function resolveCollection({ contact_id }) {
    if (typeof contact_id === "string" && contact_id.startsWith("direct_")) {
        return TESTCALL_COLLECTION;
    }
    return CALLLOGS_COLLECTION;
}

// In-process lock: tracks lead_ids currently being stub-created.
// Prevents duplicate concurrent stub creation when multiple CDRs
// arrive simultaneously for a lead with no existing CallLogs doc.
const pendingStubs = new Set();

/**
 * Ensure a CallLogs document exists for this call.
 * If the AI calling system already created it (with conversation.turns etc.),
 * we just make sure call_data.events array exists without touching other fields.
 * If no document exists yet, we create a minimal one.
 *
 * Match priority:
 *   1. lead_id (our primary key — reliable)
 *   2. contact_id fallback (in case AI system stored differently)
 */
async function createCallLog({ lead_id, call_id, campaign_id, contact_id }) {
    if (!lead_id) return;
    try {
        const db = getDb();
        const collectionName = resolveCollection({ contact_id });

        // Step 1: Try to find existing doc by lead_id (created by AI calling system)
        const existing = await db.collection(collectionName).findOne({ lead_id: String(lead_id) });

        if (existing) {
            // Doc already exists — just ensure call_data.events array is initialized
            // without touching conversation.turns or any other AI-populated fields
            const needsInit = !existing.call_data || !Array.isArray(existing.call_data?.events);
            if (needsInit) {
                await db.collection(collectionName).updateOne(
                    { lead_id: String(lead_id) },
                    { $set: { "call_data.events": [] } }
                );
                logger.info(`[CallLog] Initialized call_data.events on existing doc lead_id=${lead_id}`);
            } else {
                logger.info(`[CallLog] Doc already exists for lead_id=${lead_id} — will append events`);
            }
            return;
        }

        // Step 2: No doc yet — create one (AI calling system may create it later)
        await db.collection(collectionName).updateOne(
            { lead_id: String(lead_id) },
            {
                $setOnInsert: {
                    contact_id:  String(contact_id || ""),
                    campaign_id: String(campaign_id || ""),
                    call_id:     String(call_id || lead_id),
                    lead_id:     String(lead_id),
                    createdAt:   new Date().toISOString(),
                    recordingUrl: "",
                    call_data:   { events: [] },
                    conversation: { turns: [] }
                }
            },
            { upsert: true }
        );

        logger.info(`[CallLog] Created log for lead_id=${lead_id} → contact_id=${contact_id}`);
    } catch (err) {
        logger.error(`[CallLog] createCallLog failed: ${err.message}`, { lead_id });
    }
}

/**
 * Append a webhook event to the CallLogs document.
 * Matches by lead_id. Also handles the case where call_data.events
 * doesn't exist yet (AI system may not have initialized it).
 *
 * @param {string} lead_id      - resolved from in-memory mapping
 * @param {string} event_type   - e.g. "call_initiated", "call_hangup", "cdr_push"
 * @param {object} eventData    - raw webhook body
 * @param {string} [recordingUrl] - if provided, also update recordingUrl field
 */
async function appendCallEvent(lead_id, event_type, eventData, recordingUrl = null, options = {}) {
    if (!lead_id) return;

    try {
        const db = getDb();
        const collectionName = options.collectionName || resolveCollection({ contact_id: options.contact_id });

        const newEvent = {
            timestamp:  new Date().toISOString(),
            event_type: event_type,
            data:       eventData
        };

        // Store recordingUrl directly on the event for easy access
        if (recordingUrl) {
            newEvent.recordingUrl = recordingUrl;
        }

        // Schema-safe append:
        // Ensure call_data is an object and call_data.events is an array, then append.
        // This avoids failures when an older document has call_data in a different shape.
        const pipeline = [
            { $set: { call_data: { $ifNull: ["$call_data", {}] } } },
            { $set: { "call_data.events": { $ifNull: ["$call_data.events", []] } } },
            { $set: { "call_data.events": { $concatArrays: ["$call_data.events", [newEvent]] } } },
        ];
        if (recordingUrl) {
            pipeline.push({ $set: { recordingUrl } });
        }

        // First try: match existing doc by lead_id
        let result = await db.collection(collectionName).updateOne(
            { lead_id: String(lead_id) },
            pipeline
        );

        if (result.matchedCount === 0) {
            // No doc found (server restarted, call-mapping missed, etc.)
            // Guard against race: if another concurrent request is already
            // creating the stub, wait briefly and retry instead of also logging.
            if (pendingStubs.has(String(lead_id))) {
                await new Promise(r => setTimeout(r, 250));
                return appendCallEvent(lead_id, event_type, eventData, recordingUrl);
            }

            pendingStubs.add(String(lead_id));
            logger.warn(`[CallLog] No CallLogs doc for lead_id=${lead_id} — creating stub`);

            try {
                await db.collection(collectionName).updateOne(
                    { lead_id: String(lead_id) },
                    {
                        $setOnInsert: {
                            lead_id:   String(lead_id),
                            createdAt: new Date().toISOString(),
                            call_data: { events: [] }
                        },
                        $push: { "call_data.events": newEvent },
                        ...(recordingUrl ? { $set: { recordingUrl } } : {})
                    },
                    { upsert: true }
                );
                logger.info(`[CallLog] Stub created and event '${event_type}' stored for lead_id=${lead_id}`);
            } finally {
                pendingStubs.delete(String(lead_id));
            }
            return;
        }

        logger.info(`[CallLog] Appended event '${event_type}' to lead_id=${lead_id}`);
    } catch (err) {
        // Handle case where call_data field doesn't exist at all in the doc
        if (err.message?.includes("call_data")) {
            try {
                const db = getDb();
                // Initialize call_data.events then retry
                await db.collection(collectionName).updateOne(
                    { lead_id: String(lead_id) },
                    { $set: { "call_data": { events: [] } } }
                );
                await appendCallEvent(lead_id, event_type, eventData, recordingUrl, options);
            } catch (retryErr) {
                logger.error(`[CallLog] appendCallEvent retry failed: ${retryErr.message}`, { lead_id, event_type });
            }
        } else {
            logger.error(`[CallLog] appendCallEvent failed: ${err.message}`, { lead_id, event_type });
        }
    }
}

module.exports = { createCallLog, appendCallEvent };
