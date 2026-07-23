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

function resolveCollection() {
    return CALLLOGS_COLLECTION;
}

/**
 * All outbound webhook events → CallLogs (billing path). TestCall is mirrored after credit.
 */
async function resolveOutboundCollection() {
    return CALLLOGS_COLLECTION;
}

const TWILIO_STATUS_EVENT = "twilio_call_status";
const TELNYX_STATUS_EVENT = "telnyx_call_status";

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

function buildTelnyxStatusEvent({
    callControlId,
    eventType,
    hangupCause,
    durationSec,
    timestampIso,
}) {
    return {
        timestamp: new Date().toISOString(),
        event_type: TELNYX_STATUS_EVENT,
        data: {
            call_control_id: callControlId,
            event_type: eventType,
            hangup_cause: hangupCause || null,
            CallDuration: durationSec == null ? null : durationSec,
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
    const now = new Date();
    const rootSet = { ...twilioSetFields, updatedAt: now };
    const pipeline = [
        { $set: { call_data: { $ifNull: ["$call_data", {}] } } },
        { $set: { "call_data.events": { $ifNull: ["$call_data.events", []] } } },
        { $set: { createdAt: { $ifNull: ["$createdAt", now] } } },
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
 * Applies Telnyx snapshot fields and appends one call_data event.
 */
async function mergeTelnyxStatusIntoCallLog(collectionName, filter, telnyxSetFields, eventDoc) {
    const db = getDb();
    const now = new Date();
    const rootSet = { ...telnyxSetFields, updatedAt: now };
    const pipeline = [
        { $set: { call_data: { $ifNull: ["$call_data", {}] } } },
        { $set: { "call_data.events": { $ifNull: ["$call_data.events", []] } } },
        { $set: { createdAt: { $ifNull: ["$createdAt", now] } } },
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
 * One MongoDB document per Telnyx call_control_id.
 */
async function upsertTelnyxAnchoredCallLog({
    collectionName,
    callControlId,
    telnyxSetFields,
    eventDoc,
    rootFromMapping,
}) {
    const db = getDb();
    const filter = { "telnyx.call_control_id": callControlId };
    const campaignId = rootFromMapping.campaign_id != null ? String(rootFromMapping.campaign_id) : "";
    const contactId = rootFromMapping.contact_id != null ? String(rootFromMapping.contact_id) : "";
    const externalLead = rootFromMapping.lead_id != null ? String(rootFromMapping.lead_id).trim() : "";
    const externalCallRaw = rootFromMapping.call_id != null ? String(rootFromMapping.call_id).trim() : "";
    const externalCall = externalCallRaw ? normalizeCallId(externalCallRaw) || externalCallRaw : "";

    const syntheticLeadId = `telnyx:${callControlId}`;
    const now = new Date();

    const $set = {
        ...telnyxSetFields,
        "telnyx.call_control_id": callControlId,
        lead_id: syntheticLeadId,
        call_id: callControlId,
        call_direction: "outbound",
        updatedAt: now,
    };
    if (campaignId) $set.campaign_id = campaignId;
    if (contactId) $set.contact_id = contactId;
    if (externalLead) $set["telnyx.external_lead_id"] = externalLead;
    if (externalCall) $set["telnyx.external_call_id"] = externalCall;

    const $setOnInsert = {
        createdAt: now,
        recordingUrl: "",
    };

    try {
        const result = await db.collection(collectionName).updateOne(
            filter,
            {
                $set,
                $setOnInsert,
                $push: { "call_data.events": eventDoc },
            },
            { upsert: true }
        );
        return result;
    } catch (err) {
        // Path conflict if call_data missing on insert — retry with pipeline-style merge
        if (String(err?.message || "").includes("conflict")) {
            const matched = await mergeTelnyxStatusIntoCallLog(
                collectionName,
                filter,
                $set,
                eventDoc
            );
            if (!matched) {
                await db.collection(collectionName).updateOne(
                    filter,
                    {
                        $set: { ...$set, call_data: { events: [eventDoc] } },
                        $setOnInsert,
                    },
                    { upsert: true }
                );
            }
            return { upserted: true };
        }
        logger.error("[upsertTelnyxAnchoredCallLog] Error during upsert", {
            error: err.message,
            call_control_id: callControlId,
        });
        throw err;
    }
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
    const now = new Date();

    const $set = {
        ...twilioSetFields,
        "twilio.call_sid": twilioCallSid,
        lead_id: syntheticLeadId,
        call_id: twilioCallSid,
        call_direction: "outbound",
        updatedAt: now,
    };
    if (campaignId) $set.campaign_id = campaignId;
    if (contactId) $set.contact_id = contactId;
    if (externalLead) $set["twilio.external_lead_id"] = externalLead;
    if (externalCall) $set["twilio.external_call_id"] = externalCall;

    const $setOnInsert = {
        createdAt: now,
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
async function markInboundConversationCompleted(mongoFilter) {
    if (!mongoFilter || typeof mongoFilter !== "object") return;
    try {
        const db = getDb();
        await db.collection(INBOUNDCALLLOG_COLLECTION).updateOne(mongoFilter, {
            $set: { status: INBOUND_CONVERSATION_STATUS_COMPLETED, updatedAt: new Date() },
        });
    } catch (err) {
        logger.error("[CallLog] markInboundConversationCompleted failed", {
            error: err.message,
            filter: mongoFilter,
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

            try {
                await updateOneWithUpsertRaceRetry(db.collection(collectionName), { call_id: key }, [
                    {
                        $set: {
                            call_id: { $ifNull: ["$call_id", key] },
                            contact_id: { $ifNull: ["$contact_id", String(contact_id || "")] },
                            config_id: { $ifNull: ["$config_id", String(campaign_id || "")] },
                            call_direction: { $ifNull: ["$call_direction", "inbound"] },
                            createdAt: { $ifNull: ["$createdAt", new Date().toISOString()] },
                            recordingUrl: { $ifNull: ["$recordingUrl", ""] },
                            call_data: { $ifNull: ["$call_data", { events: [] }] },
                            status: { $ifNull: ["$status", INBOUND_CONVERSATION_STATUS_ACTIVE] },
                        },
                    },
                    { $set: { "call_data.events": { $ifNull: ["$call_data.events", []] } } },
                ]);
            } catch (err) {
                if (!isDuplicateKeyError(err)) throw err;
            }
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

        try {
            await updateOneWithUpsertRaceRetry(
                db.collection(collectionName),
                { lead_id: String(lead_id) },
                [
                    {
                        $set: {
                            contact_id: { $ifNull: ["$contact_id", String(contact_id || "")] },
                            campaign_id: { $ifNull: ["$campaign_id", String(campaign_id || "")] },
                            call_id: { $ifNull: ["$call_id", String(call_id || lead_id)] },
                            lead_id: String(lead_id),
                            createdAt: { $ifNull: ["$createdAt", new Date().toISOString()] },
                            recordingUrl: { $ifNull: ["$recordingUrl", ""] },
                            call_data: { $ifNull: ["$call_data", { events: [] }] },
                        },
                    },
                    { $set: { "call_data.events": { $ifNull: ["$call_data.events", []] } } },
                ]
            );
        } catch (err) {
            if (!isDuplicateKeyError(err)) throw err;
        }

        logger.info(`[CallLog] Created log for lead_id=${lead_id} → contact_id=${contact_id}`);
    } catch (err) {
        logger.error(`[CallLog] createCallLog failed: ${err.message}`, { lead_id, call_id });
    }
}

function isDuplicateKeyError(err) {
    const msg = String(err?.message || "");
    return err?.code === 11000 || /E11000|duplicate key/i.test(msg);
}

/**
 * Concurrent webhook upserts (e.g. call_initiated + call_ringing) can both insert when
 * no doc exists yet. With a unique lead_id/call_id index the loser gets E11000 — retry
 * as a normal update so both events land on the same document.
 */
async function updateOneWithUpsertRaceRetry(coll, filter, pipeline, { maxAttempts = 4 } = {}) {
    let lastErr = null;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            return await coll.updateOne(filter, pipeline, { upsert: true });
        } catch (err) {
            lastErr = err;
            if (!isDuplicateKeyError(err) || attempt === maxAttempts) throw err;
            logger.warn("[CallLog] Upsert race (duplicate key); retrying as update", {
                attempt,
                filter,
            });
        }
    }
    throw lastErr;
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
    let inboundCid = null;

    if (inbound) {
        inboundCid = normalizeCallId(
            options.callId || eventData?.call_id || eventData?.Call_UniqueId
        );
        if (!inboundCid && !options.inboundDocFilter) {
            logger.warn("[CallLog] appendCallEvent inbound: missing call_id in payload");
            return;
        }
        docFilter = options.inboundDocFilter || { call_id: inboundCid };
        logLabel = JSON.stringify(docFilter);
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
            identitySet.call_id = {
                $ifNull: ["$call_id", String(options.inboundPreserveCallId || inboundCid || "")],
            };
            identitySet.call_direction = { $ifNull: ["$call_direction", "inbound"] };
            identitySet.status = { $ifNull: ["$status", INBOUND_CONVERSATION_STATUS_ACTIVE] };
            if (options.inboundCallSid) {
                identitySet.call_sid = { $ifNull: ["$call_sid", String(options.inboundCallSid)] };
            }
            if (options.providerCallId) {
                identitySet.provider_call_id = {
                    $ifNull: ["$provider_call_id", String(options.providerCallId)],
                };
            }
            if (options.contact_id) {
                identitySet.contact_id = { $ifNull: ["$contact_id", String(options.contact_id)] };
            }
            if (options.campaign_id) {
                identitySet.config_id = { $ifNull: ["$config_id", String(options.campaign_id)] };
            }
            if (options.toPhone) {
                identitySet.to_number = { $ifNull: ["$to_number", String(options.toPhone)] };
            }
            if (options.fromPhone) {
                identitySet.from_number = { $ifNull: ["$from_number", String(options.fromPhone)] };
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
            if (options.isTestCall === true) {
                identitySet.isTestCall = { $literal: true };
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

        const result = await updateOneWithUpsertRaceRetry(
            db.collection(resolvedCollectionName),
            docFilter,
            pipeline
        );

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
    TESTCALL_COLLECTION,
    resolveCollection,
    resolveOutboundCollection,
    buildTwilioStatusEvent,
    mergeTwilioStatusIntoCallLog,
    upsertTwilioAnchoredCallLog,
    TWILIO_STATUS_EVENT,
    buildTelnyxStatusEvent,
    mergeTelnyxStatusIntoCallLog,
    upsertTelnyxAnchoredCallLog,
    TELNYX_STATUS_EVENT,
    isUuidCallKey,
};
