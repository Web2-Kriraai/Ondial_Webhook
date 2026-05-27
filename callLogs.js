const { getDb } = require("./db");
const { normalizeCallId } = require("./callMapping");
const logger = require("./logger");

const CALLLOGS_COLLECTION = process.env.CALLLOGS_COLLECTION || "CallLogs";
const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";
const INBOUNDCALLLOG_COLLECTION = process.env.INBOUNDCALLLOG_COLLECTION || "InboundConversation";

/** Top-level lifecycle on InboundConversation only (not Twilio / CallLogs). */
const INBOUND_CONVERSATION_STATUS_ACTIVE = "active";
const INBOUND_CONVERSATION_STATUS_COMPLETED = "completed";

const UUID_RE =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function isUuidCallKey(value) {
    return typeof value === "string" && UUID_RE.test(value.trim());
}

function resolveOutboundLeadId(leadId, options = {}) {
    const fromOptions = pickNonEmpty(options.callUniqueId, options.callId);
    if (isUuidCallKey(fromOptions)) return String(fromOptions).trim();
    if (isUuidCallKey(leadId)) return String(leadId).trim();
    return leadId != null && String(leadId).trim() !== "" ? String(leadId).trim() : null;
}

function pickNonEmpty(...values) {
    for (const v of values) {
        if (v != null && String(v).trim() !== "") return String(v).trim();
    }
    return null;
}

/**
 * Dialer/voice-AI often creates CallLogs with API lead_id + provider call_id before webhooks.
 * Re-key that row to call_unique_id, or fold it into the webhook doc when both exist.
 */
async function reconcileDialerCallLogToUniqueId(db, collectionName, ctx) {
    const { contact_id, campaign_id, callUniqueId, providerCallId } = ctx;
    if (!contact_id || !callUniqueId || !providerCallId) return;

    const providerRaw = String(providerCallId).trim();
    const providerNorm = normalizeCallId(providerRaw) || providerRaw;
    const providerHex = providerNorm.replace(/-/g, "");

    const legacyQuery = {
        contact_id: String(contact_id),
        lead_id: { $ne: callUniqueId },
        $or: [{ call_id: providerRaw }, { call_id: providerNorm }, { call_id: providerHex }],
    };
    if (campaign_id) legacyQuery.campaign_id = String(campaign_id);

    const legacy = await db
        .collection(collectionName)
        .findOne(legacyQuery, { sort: { createdAt: -1 } });
    if (!legacy) return;

    const canonical = await db.collection(collectionName).findOne({ lead_id: callUniqueId });

    if (canonical && String(canonical._id) !== String(legacy._id)) {
        const mergeSet = {
            provider_call_id: providerNorm,
            updatedAt: new Date(),
        };
        if (legacy.conversation && !canonical.conversation) {
            mergeSet.conversation = legacy.conversation;
        }
        if (legacy.recordingUrl && !canonical.recordingUrl) {
            mergeSet.recordingUrl = legacy.recordingUrl;
        }
        const legacyEvents = Array.isArray(legacy.call_data?.events) ? legacy.call_data.events : [];
        await db.collection(collectionName).updateOne({ _id: canonical._id }, { $set: mergeSet });
        if (legacyEvents.length > 0) {
            await db.collection(collectionName).updateOne(
                { _id: canonical._id },
                { $push: { "call_data.events": { $each: legacyEvents } } }
            );
        }
        await db.collection(collectionName).deleteOne({ _id: legacy._id });
        logger.info("[CallLog] Merged legacy dialer CallLog into call_unique_id doc", {
            callUniqueId,
            legacyLeadId: legacy.lead_id,
            legacyCallId: legacy.call_id,
        });
        return;
    }

    if (!canonical) {
        await db.collection(collectionName).updateOne(
            { _id: legacy._id },
            {
                $set: {
                    lead_id: callUniqueId,
                    call_id: callUniqueId,
                    call_unique_id: callUniqueId,
                    provider_call_id: providerNorm,
                    legacy_lead_id: legacy.lead_id,
                    legacy_call_id: legacy.call_id,
                    updatedAt: new Date(),
                },
            }
        );
        logger.info("[CallLog] Re-keyed legacy dialer CallLog to call_unique_id", {
            callUniqueId,
            legacyLeadId: legacy.lead_id,
        });
    }
}

function resolveCollection({ contact_id }) {
    if (typeof contact_id === "string" && contact_id.startsWith("direct_")) {
        return TESTCALL_COLLECTION;
    }
    return CALLLOGS_COLLECTION;
}

const TWILIO_STATUS_EVENT = "twilio_call_status";

function buildTwilioStatusEvent({ CallSid, CallStatus, CallDuration, timestampIso }) {
    return {
        timestamp: new Date().toISOString(),
        event_type: TWILIO_STATUS_EVENT,
        data: {
            CallSid,
            CallStatus,
            CallDuration: CallDuration == null ? null : CallDuration,
            Timestamp: timestampIso,
        },
    };
}

/**
 * Applies Twilio snapshot fields and appends one call_data event (single round-trip).
 * Pipeline tolerates rows missing call_data (legacy or partial writes).
 */
async function mergeTwilioStatusIntoCallLog(collectionName, filter, twilioSetFields, eventDoc) {
    const db = getDb();
    const rootSet = { ...twilioSetFields, updatedAt: new Date() };
    const pipeline = [
        { $set: { call_data: { $ifNull: ["$call_data", {}] } } },
        { $set: { "call_data.events": { $ifNull: ["$call_data.events", []] } } },
        { $set: rootSet },
        {
            $set: {
                "call_data.events": {
                    $concatArrays: [{ $ifNull: ["$call_data.events", []] }, [eventDoc]],
                },
            },
        },
    ];
    const result = await db.collection(collectionName).updateOne(filter, pipeline);
    return result.matchedCount > 0;
}

/**
 * One MongoDB document per Twilio CallSid. lead_id is always `twilio:<CallSid>` so the same
 * contact_id / campaign can have many Twilio retries without colliding with the partial unique
 * index on CRM lead_id. Optional dialer ids are stored on twilio.external_* when provided.
 */
async function upsertTwilioAnchoredCallLog({
    collectionName,
    twilioCallSid,
    twilioSetFields,
    eventDoc,
    rootFromMapping,
}) {
    const db = getDb();
    const filter = { "twilio.call_sid": twilioCallSid };
    const campaignId = rootFromMapping.campaign_id != null ? String(rootFromMapping.campaign_id) : "";
    const contactId = rootFromMapping.contact_id != null ? String(rootFromMapping.contact_id) : "";
    const externalLead = rootFromMapping.lead_id != null ? String(rootFromMapping.lead_id).trim() : "";
    const externalCallRaw = rootFromMapping.call_id != null ? String(rootFromMapping.call_id).trim() : "";
    const externalCall = externalCallRaw ? normalizeCallId(externalCallRaw) || externalCallRaw : "";

    const syntheticLeadId = `twilio:${twilioCallSid}`;

    const $set = {
        ...twilioSetFields,
        "twilio.call_sid": twilioCallSid,
        lead_id: syntheticLeadId,
        call_id: twilioCallSid,
        call_direction: "outbound",
        updatedAt: new Date(),
    };
    if (campaignId) $set.campaign_id = campaignId;
    if (contactId) $set.contact_id = contactId;
    if (externalLead) $set["twilio.external_lead_id"] = externalLead;
    if (externalCall) $set["twilio.external_call_id"] = externalCall;

    const $setOnInsert = {
        createdAt: new Date().toISOString(),
        recordingUrl: "",
    };

    try {
        const result = await db.collection(collectionName).updateOne(
            filter,
            {
                $set,
                $setOnInsert,
                // Do not $setOnInsert call_data here — same update uses $push on call_data.events
                // and MongoDB rejects that combination (path conflict at call_data).
                $push: { "call_data.events": eventDoc },
            },
            { upsert: true }
        );

        return result.upsertedCount > 0 || result.modifiedCount > 0 || result.matchedCount > 0;
    } catch (err) {
        logger.error("[upsertTwilioAnchoredCallLog] Error during upsert", {
            error: err.message,
            filter,
            collection: collectionName,
        });
        throw err;
    }
}

/**
 * InboundConversation: mark call as live. Skips if already completed (no extra write when matchedCount 0).
 */
async function markInboundConversationActive(normalizedCallId) {
    if (!normalizedCallId) return;
    try {
        const db = getDb();
        await db.collection(INBOUNDCALLLOG_COLLECTION).updateOne(
            { call_id: normalizedCallId, status: { $ne: INBOUND_CONVERSATION_STATUS_COMPLETED } },
            { $set: { status: INBOUND_CONVERSATION_STATUS_ACTIVE, updatedAt: new Date() } }
        );
    } catch (err) {
        logger.error("[CallLog] markInboundConversationActive failed", {
            error: err.message,
            call_id: normalizedCallId,
        });
    }
}

/**
 * InboundConversation: terminal state after hangup / CDR / failure. Idempotent.
 */
async function markInboundConversationCompleted(normalizedCallId) {
    if (!normalizedCallId) return;
    try {
        const db = getDb();
        await db.collection(INBOUNDCALLLOG_COLLECTION).updateOne(
            { call_id: normalizedCallId },
            { $set: { status: INBOUND_CONVERSATION_STATUS_COMPLETED, updatedAt: new Date() } }
        );
    } catch (err) {
        logger.error("[CallLog] markInboundConversationCompleted failed", {
            error: err.message,
            call_id: normalizedCallId,
        });
    }
}

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
                await markInboundConversationActive(key);
                return;
            }

            await db.collection(collectionName).updateOne(
                { call_id: key },
                {
                    $setOnInsert: {
                        call_id: key,
                        contact_id: String(contact_id || ""),
                        config_id: String(campaign_id || ""),
                        call_direction: "inbound",
                        createdAt: new Date().toISOString(),
                        recordingUrl: "",
                        call_data: { events: [] },
                        status: INBOUND_CONVERSATION_STATUS_ACTIVE,
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
                },
            },
            { upsert: true }
        );

        logger.info(`[CallLog] Created log for lead_id=${lead_id} → contact_id=${contact_id}`);
    } catch (err) {
        logger.error(`[CallLog] createCallLog failed: ${err.message}`, { lead_id, call_id });
    }
}

/**
 * Pipeline upsert: creates the doc if missing, appends one event to call_data.events,
 * and lazy-fills identity fields via $ifNull so existing values are never overwritten.
 *
 * No more separate "stub creation" path — single round-trip, no path-conflict bugs,
 * no recursive retries, no in-memory pending-stub set.
 */
async function appendCallEvent(lead_id, event_type, eventData, recordingUrl = null, options = {}) {
    const resolvedCollectionName =
        options.collectionName || resolveCollection({ contact_id: options.contact_id });
    const inbound = resolvedCollectionName === INBOUNDCALLLOG_COLLECTION;

    let docFilter;
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
        logLabel = `call_id=${cid}`;
    } else {
        const effectiveLeadId = resolveOutboundLeadId(lead_id, options);
        if (!effectiveLeadId) return;

        try {
            const db = getDb();
            await reconcileDialerCallLogToUniqueId(db, resolvedCollectionName, {
                contact_id: options.contact_id,
                campaign_id: options.campaign_id,
                callUniqueId: effectiveLeadId,
                providerCallId: options.providerCallId,
            });
        } catch (reconcileErr) {
            logger.warn("[CallLog] reconcileDialerCallLogToUniqueId failed", {
                error: reconcileErr.message,
                lead_id: effectiveLeadId,
            });
        }

        docFilter = { lead_id: effectiveLeadId };
        logLabel = `lead_id=${effectiveLeadId}`;
    }

    try {
        const db = getDb();

        const newEvent = {
            timestamp: new Date().toISOString(),
            event_type: event_type,
            data: eventData,
        };
        if (recordingUrl) newEvent.recordingUrl = recordingUrl;

        // With aggregation-pipeline upsert, MongoDB does NOT inject filter fields into the new
        // document. We must set every identity field explicitly so the doc is queryable on
        // the next event. We use $ifNull so existing values on update are never overwritten.
        const identitySet = {
            createdAt: { $ifNull: ["$createdAt", new Date().toISOString()] },
            updatedAt: new Date(),
        };
        if (inbound) {
            // call_id IS the filter key — must always be present.
            identitySet.call_id = docFilter.call_id;
            identitySet.call_direction = { $ifNull: ["$call_direction", "inbound"] };
            identitySet.status = { $ifNull: ["$status", INBOUND_CONVERSATION_STATUS_ACTIVE] };
            if (options.contact_id) {
                identitySet.contact_id = { $ifNull: ["$contact_id", String(options.contact_id)] };
            }
            if (options.campaign_id) {
                identitySet.config_id = { $ifNull: ["$config_id", String(options.campaign_id)] };
            }
        } else {
            const outboundLeadId = docFilter.lead_id;
            identitySet.lead_id = outboundLeadId;
            identitySet.call_unique_id = { $ifNull: ["$call_unique_id", outboundLeadId] };
            if (options.contact_id) {
                identitySet.contact_id = { $ifNull: ["$contact_id", String(options.contact_id)] };
            }
            if (options.campaign_id) {
                identitySet.campaign_id = { $ifNull: ["$campaign_id", String(options.campaign_id)] };
            }
            if (options.callId) {
                identitySet.call_id = { $ifNull: ["$call_id", String(options.callId)] };
            }
            if (options.providerCallId) {
                identitySet.provider_call_id = {
                    $ifNull: ["$provider_call_id", String(options.providerCallId)],
                };
            }
        }

        // Separate stages avoid the "Updating path 'call_data.events' would create a conflict
        // at 'call_data'" error: each stage's output is the input to the next.
        const pipeline = [
            { $set: identitySet },
            { $set: { call_data: { $ifNull: ["$call_data", {}] } } },
            { $set: { "call_data.events": { $ifNull: ["$call_data.events", []] } } },
            { $set: { "call_data.events": { $concatArrays: ["$call_data.events", [newEvent]] } } },
        ];
        if (recordingUrl) {
            pipeline.push({ $set: { recordingUrl } });
        }

        const result = await db
            .collection(resolvedCollectionName)
            .updateOne(docFilter, pipeline, { upsert: true });

        if (result.upsertedCount > 0) {
            logger.info(`[CallLog] Created doc and stored event '${event_type}' for ${logLabel}`);
        } else if (result.modifiedCount > 0) {
            logger.info(`[CallLog] Appended event '${event_type}' to ${logLabel}`);
        } else {
            logger.warn(
                `[CallLog] appendCallEvent matched but did not modify (${logLabel}, event=${event_type})`
            );
        }
    } catch (err) {
        logger.error(`[CallLog] appendCallEvent failed: ${err.message}`, { logLabel, event_type });
    }
}

module.exports = {
    createCallLog,
    appendCallEvent,
    markInboundConversationActive,
    markInboundConversationCompleted,
    INBOUNDCALLLOG_COLLECTION,
    resolveCollection,
    buildTwilioStatusEvent,
    mergeTwilioStatusIntoCallLog,
    upsertTwilioAnchoredCallLog,
    TWILIO_STATUS_EVENT,
    isUuidCallKey,
};
