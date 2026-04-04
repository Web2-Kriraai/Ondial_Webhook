const { getDb } = require("./db");
const { normalizeCallId } = require("./callMapping");
const logger = require("./logger");

const CALLLOGS_COLLECTION = process.env.CALLLOGS_COLLECTION || "CallLogs";
const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";
const INBOUNDCALLLOG_COLLECTION = process.env.INBOUNDCALLLOG_COLLECTION || "InboundConversation";

function resolveCollection({ contact_id }) {
    if (typeof contact_id === "string" && contact_id.startsWith("direct_")) {
        return TESTCALL_COLLECTION;
    }
    return CALLLOGS_COLLECTION;
}

const pendingStubs = new Set();

/**
 * Outbound / test: keyed by lead_id.
 * Inbound (InboundConversation): keyed by call_id only — no lead_id stored.
 */
async function createCallLog({ lead_id, call_id, campaign_id, contact_id, collectionName: explicitCollection }) {
    try {
        const db = getDb();
        const collectionName = explicitCollection || resolveCollection({ contact_id });

        if (collectionName === INBOUNDCALLLOG_COLLECTION) {
            const key = normalizeCallId(call_id);
            if (!key) {
                logger.warn("[CallLog] inbound createCallLog: missing/invalid call_id");
                return;
            }

            const existing = await db.collection(collectionName).findOne({ call_id: key });
            if (existing) {
                const needsInit = !existing.call_data || !Array.isArray(existing.call_data?.events);
                if (needsInit) {
                    await db.collection(collectionName).updateOne(
                        { call_id: key },
                        { $set: { "call_data.events": [] } }
                    );
                    logger.info(`[CallLog] Initialized call_data.events inbound call_id=${key}`);
                } else {
                    logger.info(`[CallLog] Inbound doc exists call_id=${key}`);
                }
                return;
            }

            await db.collection(collectionName).updateOne(
                { call_id: key },
                {
                    $setOnInsert: {
                        call_id: key,
                        contact_id: String(contact_id || ""),
                        campaign_id: String(campaign_id || ""),
                        call_direction: "inbound",
                        createdAt: new Date().toISOString(),
                        recordingUrl: "",
                        call_data: { events: [] },
                        conversation: { turns: [] },
                    },
                },
                { upsert: true }
            );
            logger.info(`[CallLog] Created inbound log call_id=${key} contact_id=${contact_id}`);
            return;
        }

        if (!lead_id) return;

        const existing = await db.collection(collectionName).findOne({ lead_id: String(lead_id) });

        if (existing) {
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

        await db.collection(collectionName).updateOne(
            { lead_id: String(lead_id) },
            {
                $setOnInsert: {
                    contact_id: String(contact_id || ""),
                    campaign_id: String(campaign_id || ""),
                    call_id: String(call_id || lead_id),
                    lead_id: String(lead_id),
                    createdAt: new Date().toISOString(),
                    recordingUrl: "",
                    call_data: { events: [] },
                    conversation: { turns: [] },
                },
            },
            { upsert: true }
        );

        logger.info(`[CallLog] Created log for lead_id=${lead_id} → contact_id=${contact_id}`);
    } catch (err) {
        logger.error(`[CallLog] createCallLog failed: ${err.message}`, { lead_id, call_id });
    }
}

async function appendCallEvent(lead_id, event_type, eventData, recordingUrl = null, options = {}) {
    const resolvedCollectionName =
        options.collectionName || resolveCollection({ contact_id: options.contact_id });
    const inbound = resolvedCollectionName === INBOUNDCALLLOG_COLLECTION;

    let docFilter;
    let stubKey;
    let logLabel;

    if (inbound) {
        const cid = normalizeCallId(
            options.callId || eventData?.call_id || eventData?.Call_UniqueId
        );
        if (!cid) {
            logger.warn("[CallLog] appendCallEvent inbound: missing call_id in payload");
            return;
        }
        docFilter = { call_id: cid };
        stubKey = `inbound:${cid}`;
        logLabel = `call_id=${cid}`;
    } else {
        if (!lead_id) return;
        docFilter = { lead_id: String(lead_id) };
        stubKey = String(lead_id);
        logLabel = `lead_id=${lead_id}`;
    }

    try {
        const db = getDb();
        const collectionName = resolvedCollectionName;

        const newEvent = {
            timestamp: new Date().toISOString(),
            event_type: event_type,
            data: eventData,
        };

        if (recordingUrl) {
            newEvent.recordingUrl = recordingUrl;
        }

        const pipeline = [
            { $set: { call_data: { $ifNull: ["$call_data", {}] } } },
            { $set: { "call_data.events": { $ifNull: ["$call_data.events", []] } } },
            { $set: { "call_data.events": { $concatArrays: ["$call_data.events", [newEvent]] } } },
        ];
        if (recordingUrl) {
            pipeline.push({ $set: { recordingUrl } });
        }

        let result = await db.collection(collectionName).updateOne(docFilter, pipeline);

        if (result.matchedCount === 0) {
            if (pendingStubs.has(stubKey)) {
                await new Promise((r) => setTimeout(r, 250));
                return appendCallEvent(lead_id, event_type, eventData, recordingUrl, options);
            }

            pendingStubs.add(stubKey);
            logger.warn(`[CallLog] No doc for ${logLabel} — creating stub`);

            try {
                const onInsert = inbound
                    ? {
                          call_id: docFilter.call_id,
                          call_direction: "inbound",
                          createdAt: new Date().toISOString(),
                          call_data: { events: [] },
                      }
                    : {
                          lead_id: String(lead_id),
                          createdAt: new Date().toISOString(),
                          call_data: { events: [] },
                      };

                await db.collection(collectionName).updateOne(
                    docFilter,
                    {
                        $setOnInsert: onInsert,
                        $push: { "call_data.events": newEvent },
                        ...(recordingUrl ? { $set: { recordingUrl } } : {}),
                    },
                    { upsert: true }
                );
                logger.info(`[CallLog] Stub created and event '${event_type}' stored for ${logLabel}`);
            } finally {
                pendingStubs.delete(stubKey);
            }
            return;
        }

        logger.info(`[CallLog] Appended event '${event_type}' to ${logLabel}`);
    } catch (err) {
        if (err.message?.includes("call_data")) {
            try {
                const db = getDb();
                await db.collection(resolvedCollectionName).updateOne(docFilter, {
                    $set: { call_data: { events: [] } },
                });
                await appendCallEvent(lead_id, event_type, eventData, recordingUrl, options);
            } catch (retryErr) {
                logger.error(`[CallLog] appendCallEvent retry failed: ${retryErr.message}`, {
                    logLabel,
                    event_type,
                });
            }
        } else {
            logger.error(`[CallLog] appendCallEvent failed: ${err.message}`, { logLabel, event_type });
        }
    }
}

module.exports = { createCallLog, appendCallEvent, INBOUNDCALLLOG_COLLECTION };
