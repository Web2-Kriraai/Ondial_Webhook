const { ObjectId } = require("mongodb");
const crypto = require("crypto");
const { getDb } = require("./db");
const { getRedis } = require("./redis");
const {
    lookupMapping,
    lookupMappingByPhone,
    enrichPhoneMapping,
    normalizeCallId,
    normalizePhone,
    markCallAnswered,
    hasAnsweredFlag,
} = require("./callMapping");
const { appendCallEvent, INBOUNDCALLLOG_COLLECTION } = require("./callLogs");
const { logMissingCallMapping, previewPayload } = require("./errorLog");
const logger = require("./logger");
const callEvents = require("./events");
const { notifyOndialInboundWebhook } = require("./inboundNotify");
const { mapCdrSummaryToReceiveStatus } = require("./lib/cdrSummaryStatus");

/**
 * callReceiveStatus values:
 *   0 = call failed (technical error)
 *   1 = call went out but user didn't answer (no answer / busy)
 *   2 = call answered / running
 *   3 = call successfully completed
 */

const PROCESSED_TTL_MS = Number(process.env.PROCESSED_WEBHOOK_TTL_MS || 2 * 60 * 60 * 1000);
const PROCESSING_TTL_MS = Number(process.env.PROCESSING_WEBHOOK_TTL_MS || 5 * 60 * 1000);
const REPLAY_NONCE_TTL_SEC = Number(process.env.WEBHOOK_REPLAY_NONCE_TTL_SEC || 3600);

// ─── DB Update Helpers ────────────────────────────────────────────────────────

function isMongoObjectIdString(s) {
    return typeof s === "string" && /^[a-fA-F0-9]{24}$/.test(s);
}

/**
 * Test / direct-dial flows use synthetic ids like "direct_9408645627" — not valid ObjectIds.
 * Resolve embedded digits as phone and update via mobile lookup.
 */
async function updateByContactId(contactId, newStatus, context = "") {
    if (contactId == null || String(contactId).trim() === "") return;

    const cid = String(contactId);

    if (isMongoObjectIdString(cid)) {
        try {
            const db = getDb();
            const oid = new ObjectId(cid);
            const shouldBlockDowngrade = newStatus !== 3;
            const filter = shouldBlockDowngrade
                ? {
                    _id: oid,
                    $or: [
                        { callReceiveStatus: { $ne: 3 } },
                        { status: { $ne: "completed" } }
                    ]
                }
                : { _id: oid };
            const result = await db.collection("contactprocessings").updateOne(
                filter,
                { $set: { callReceiveStatus: newStatus, updatedAt: new Date() } }
            );
            if (result.matchedCount === 0) {
                const current = await db.collection("contactprocessings").findOne(
                    { _id: oid },
                    { projection: { _id: 1, status: 1, callReceiveStatus: 1 } }
                );
                if (current?._id && shouldBlockDowngrade && String(current.status) === "completed" && Number(current.callReceiveStatus) === 3) {
                    logger.warn(`[Webhook] Downgrade blocked for finalized contact_id=${cid} (${context})`, {
                        attemptedStatus: newStatus,
                        effectiveStatus: 3
                    });
                    return { applied: false, blocked: true, effectiveStatus: 3, contactId: cid };
                }
                logger.warn(`[Webhook] No contact found for contact_id=${cid} (${context})`);
                return { applied: false, blocked: false, effectiveStatus: null, contactId: cid };
            } else {
                logger.info(`[Webhook] callReceiveStatus=${newStatus} for contact_id=${cid} (${context})`);
                return { applied: true, blocked: false, effectiveStatus: newStatus, contactId: cid };
            }
        } catch (err) {
            logger.error(`[Webhook] updateByContactId failed: ${err.message}`, { contactId: cid, context });
            return { applied: false, blocked: false, effectiveStatus: null, contactId: cid };
        }
        return { applied: false, blocked: false, effectiveStatus: null, contactId: cid };
    }

    if (cid.startsWith("direct_")) {
        const digits = cid.slice("direct_".length).replace(/\D/g, "");
        if (digits) {
            return await updateByMobile(digits, newStatus, `${context} [direct_contact_id]`);
        }
    }

    logger.warn(`[Webhook] Skipping callReceiveStatus — contact_id is not an ObjectId`, {
        contactId: cid,
        context,
    });
    return { applied: false, blocked: false, effectiveStatus: null, contactId: cid };
}

function normalizeMobile(num) {
    if (!num) return null;
    const s = String(num).replace(/\D/g, "");
    if (s.length === 12 && s.startsWith("91")) return s.slice(2);
    if (s.length === 10) return s;
    return s;
}

async function updateByMobile(mobileRaw, newStatus, context = "") {
    try {
        const db = getDb();
        const mobile = normalizeMobile(mobileRaw);
        if (!mobile) return;
        const variants = [mobile];
        if (mobile.length === 10) variants.push(`91${mobile}`, `+91${mobile}`);
        // Avoid broad updateMany and pick the most recent contact only.
        const contact = await db.collection("contactprocessings").findOne(
            { mobileNumber: { $in: variants } },
            { sort: { updatedAt: -1, _id: -1 }, projection: { _id: 1, status: 1, callReceiveStatus: 1 } }
        );
        if (!contact?._id) {
            logger.warn(`[Webhook] Fallback phone match found no record (${context})`, { mobile });
            return { applied: false, blocked: false, effectiveStatus: null, contactId: null };
        }
        const shouldBlockDowngrade = newStatus !== 3;
        if (
            shouldBlockDowngrade &&
            String(contact.status) === "completed" &&
            Number(contact.callReceiveStatus) === 3
        ) {
            logger.warn(`[Webhook] Downgrade blocked for finalized contact via mobile (${context})`, {
                attemptedStatus: newStatus,
                effectiveStatus: 3,
                contactId: String(contact._id),
                mobile
            });
            return { applied: false, blocked: true, effectiveStatus: 3, contactId: String(contact._id) };
        }
        await db.collection("contactprocessings").updateOne(
            { _id: contact._id },
            { $set: { callReceiveStatus: newStatus, updatedAt: new Date() } }
        );
        logger.warn(`[Webhook] Fallback phone match updated one contact (${context})`, {
            mobile,
            contactId: String(contact._id),
            status: newStatus,
        });
        return { applied: true, blocked: false, effectiveStatus: newStatus, contactId: String(contact._id) };
    } catch (err) {
        logger.error(`[Webhook] updateByMobile failed: ${err.message}`, { mobileRaw, context });
        return { applied: false, blocked: false, effectiveStatus: null, contactId: null };
    }
}

/**
 * Resolve the contact from a webhook event:
 *   1. Lookup by call_id in Redis mapping (exact match)
 *   2. Lookup by phone in Redis mapping (handles Asterisk call_id change on answer)
 *      — skipped when step 1 already resolved an inbound log mapping (avoid wrong merge)
 *   3. Fallback: DB phone scan (last resort — outbound only; never for inbound-only mapping)
 */
async function updateStatus(callId, mobileRaw, newStatus, context = "") {
    let mapping = await lookupMapping(callId);
    const inboundByCallId = mapping?.collectionName === INBOUNDCALLLOG_COLLECTION;

    if (!mapping?.contact_id && mobileRaw && !inboundByCallId) {
        const byPhone = await lookupMappingByPhone(mobileRaw);
        if (byPhone) mapping = byPhone;
    }

    const contact_id = mapping?.contact_id;
    const campaign_id = mapping?.campaign_id;
    const isInboundMapping = mapping?.collectionName === INBOUNDCALLLOG_COLLECTION;

    if (contact_id) {
        const updateResult = await updateByContactId(contact_id, newStatus, context);
        const emittedStatus = Number.isFinite(updateResult?.effectiveStatus) ? updateResult.effectiveStatus : newStatus;
        if (campaign_id || isInboundMapping) {
            callEvents.emit("call_update", {
                campaign_id: campaign_id || null,
                call_id: callId,
                contact_id,
                status: emittedStatus,
                timestamp: new Date().toISOString()
            });
        }
        return;
    }

    if (isInboundMapping) {
        callEvents.emit("call_update", {
            campaign_id: campaign_id || null,
            call_id: callId,
            contact_id: null,
            status: newStatus,
            timestamp: new Date().toISOString()
        });
        return;
    }

    logger.warn(`[Webhook] No mapping for call_id=${normalizeCallId(callId)} — falling back to broad phone match`);
    const updateResult = await updateByMobile(mobileRaw, newStatus, context);
    const emittedStatus = Number.isFinite(updateResult?.effectiveStatus) ? updateResult.effectiveStatus : newStatus;

    callEvents.emit("call_update", {
        call_id: callId,
        status: emittedStatus,
        timestamp: new Date().toISOString()
    });
}

function eventCallIdFromPayload(ev) {
    return normalizeCallId(ev?.data?.call_id ?? ev?.data?.Call_UniqueId);
}

/**
 * Events for this outbound dial only. Outbound CallLogs are keyed by lead_id and accumulate
 * many calls; without scoping, an old call_answered would make BUSY/no-answer look "completed".
 */
function sliceEventsForThisCallLeg(events, normalizedCallId) {
    if (!normalizedCallId || !Array.isArray(events) || events.length === 0) return [];
    let start = -1;
    for (let i = 0; i < events.length; i++) {
        const e = events[i];
        const ty = String(e?.event_type || "").toLowerCase();
        if (ty === "call_initiated" && eventCallIdFromPayload(e) === normalizedCallId) {
            start = i;
        }
    }
    if (start === -1) {
        for (let i = 0; i < events.length; i++) {
            if (eventCallIdFromPayload(events[i]) === normalizedCallId) {
                start = i;
                break;
            }
        }
    }
    if (start === -1) return [];
    return events.slice(start);
}

async function didCallReachAnsweredStage({ leadId, callId, collectionName, toPhone }) {
    try {
        const db = getDb();
        const normalized = normalizeCallId(callId);
        if (!normalized) return false;

        const isInbound = collectionName === INBOUNDCALLLOG_COLLECTION;
        let doc = null;

        if (isInbound) {
            doc = await db.collection(collectionName).findOne(
                { call_id: normalized },
                { projection: { "call_data.events": 1 } }
            );
        } else if (hasLeadId(leadId)) {
            doc = await db.collection(collectionName || "CallLogs").findOne(
                { lead_id: String(leadId) },
                { projection: { "call_data.events": 1 } }
            );
        }

        const events = Array.isArray(doc?.call_data?.events) ? doc.call_data.events : [];
        const slice = isInbound ? events : sliceEventsForThisCallLeg(events, normalized);
        const normTo = toPhone ? normalizePhone(toPhone) : null;

        return slice.some((e) => {
            if (String(e?.event_type || "").toLowerCase() !== "call_answered") return false;
            if (eventCallIdFromPayload(e) === normalized) return true;
            if (normTo && normalizePhone(e?.data?.to) === normTo) return true;
            return false;
        });
    } catch (err) {
        logger.warn(`[Webhook] didCallReachAnsweredStage lookup failed: ${err.message}`);
        return false;
    }
}

// ─── Event Webhook Handler ────────────────────────────────────────────────────

function hasLeadId(v) {
    return v != null && String(v).trim() !== "";
}

async function handleEventWebhook(body) {
    const { event, call_id, to, duration } = body;
    const mapping = (await lookupMapping(call_id)) || (await lookupMappingByPhone(to));
    const lead_id = mapping?.lead_id || null;
    const contact_id = mapping?.contact_id || null;
    const collectionName = mapping?.collectionName || null;
    const isInboundLog = collectionName === INBOUNDCALLLOG_COLLECTION;

    if (!hasLeadId(lead_id) && !isInboundLog) {
        await logMissingCallMapping({
            source: "event_webhook",
            reason: "no_lead_id_after_redis_lookup",
            event: event || null,
            call_id: call_id || null,
            to: to || null,
            contact_id_from_mapping: contact_id,
            body_preview: previewPayload(body),
        });
    }

    const docKey = normalizeCallId(call_id);
    if (isInboundLog) {
        if (docKey) {
            await appendCallEvent(docKey, event, body, null, {
                contact_id,
                collectionName,
                callId: docKey,
            });
        } else {
            logger.warn("[Webhook] Inbound log: missing call_id on event payload", { event });
        }
    } else {
        await appendCallEvent(lead_id, event, body, null, { contact_id, collectionName });
    }

    switch (event) {
        case "call_initiated":
        case "call_ringing":
            // Register the destination phone in the mapping so that
            // subsequent call_answered/call_hangup (which may have a different
            // Asterisk call_id) can still do a precise contact lookup by phone.
            if (event === "call_initiated") {
                await enrichPhoneMapping(call_id, to);
            }
            // Do NOT update DB — worker already sets status=1 when call is placed.
            logger.info(`[Webhook] Ignoring ${event} for call_id=${call_id} — no DB update`);

            // Fire SSE anyway so UI call logs reload the ringing/initiated state
            callEvents.emit("call_update", {
                campaign_id: mapping?.campaign_id || null,
                call_id,
                event,
                timestamp: new Date().toISOString()
            });
            break;

        case "call_answered":
            await markCallAnswered(call_id, to);
            await updateStatus(call_id, to, 2, event);
            break;

        case "call_hangup": {
            const dur = parseInt(duration, 10) || 0;
            const answeredStageSeen =
                (await hasAnsweredFlag(call_id, to)) ||
                (await didCallReachAnsweredStage({
                    leadId: lead_id,
                    callId: call_id,
                    collectionName,
                    toPhone: to,
                }));
            const hangupStatus = answeredStageSeen ? 3 : 1;
            await updateStatus(
                call_id,
                to,
                hangupStatus,
                `${event} duration=${dur}s answeredSeen=${answeredStageSeen ? "yes" : "no"}`
            );
            // Outbound ondial.ai notify: only when inbound call ends (not on ring/initiated/answered)
            if (isInboundLog && docKey) {
                await notifyOndialInboundWebhook(docKey);
            }
            break;
        }

        case "call_failed":
            // Technical failure (network, provider error) — status=0
            await updateStatus(call_id, to, 0, event);
            break;

        default:
            logger.info(`[Webhook] Unhandled event: ${event}`);
    }
}

// ─── Summary (CDR Push) Webhook Handler ──────────────────────────────────────

async function handleSummaryWebhook(body) {
    const { Call_UniqueId, To_number, Duration, CallStatus, RecordingURL, lead_id: cdr_lead_id } = body;

    const dur = parseInt(Duration, 10) || 0;

    // Look up mapping first (needed for answered-stage checks and collections)
    const mapping = (await lookupMapping(Call_UniqueId)) || (await lookupMappingByPhone(To_number));
    const lead_id = mapping?.lead_id || String(cdr_lead_id || "");
    const contact_id = mapping?.contact_id || null;
    const collectionName = mapping?.collectionName || null;
    const isInboundLog = collectionName === INBOUNDCALLLOG_COLLECTION;
    const cdrCallKey = normalizeCallId(Call_UniqueId);

    /**
     * Status mapping for CDR summary:
     *   3 = provider says ANSWER, or we have answer evidence + talk time / contradictory CDR fix
     *   1 = BUSY / NO ANSWER / ring-only (dur>0 without answer path)
     *   0 = no duration and not answered
     *
     * `dur > 0` alone is not enough (can be ring time) unless we saw call_answered (Redis/Mongo).
     * Contradictory CDR (e.g. NO ANSWER + duration) → 3 only with answer evidence.
     */
    const answeredEvidence =
        (await hasAnsweredFlag(Call_UniqueId, To_number)) ||
        (await didCallReachAnsweredStage({
            leadId: lead_id,
            callId: Call_UniqueId,
            collectionName,
            toPhone: To_number,
        }));

    let newStatus;
    if (CallStatus === "ANSWER") {
        newStatus = 3;
    } else if (CallStatus === "BUSY" || CallStatus === "NO ANSWER") {
        if (dur > 0 && answeredEvidence) {
            newStatus = 3;
        } else {
            newStatus = 1;
        }
    } else if (dur > 0) {
        newStatus = answeredEvidence ? 3 : 1;
    } else {
        newStatus = 0;
    }

    if (!hasLeadId(lead_id) && !(isInboundLog && cdrCallKey)) {
        await logMissingCallMapping({
            source: "summary_webhook",
            reason: "no_lead_id_mapping_or_cdr_body",
            call_unique_id: Call_UniqueId || null,
            to_number: To_number || null,
            contact_id_from_mapping: contact_id,
            body_preview: previewPayload(body),
        });
    }

    if (isInboundLog && cdrCallKey) {
        await appendCallEvent(cdrCallKey, "cdr_push", body, RecordingURL || null, {
            contact_id,
            collectionName,
            callId: cdrCallKey,
        });
        await notifyOndialInboundWebhook(cdrCallKey);
    } else {
        await appendCallEvent(lead_id, "cdr_push", body, RecordingURL || null, { contact_id, collectionName });
    }

    if (newStatus === 3) {
        await markCallAnswered(Call_UniqueId, To_number);
    }

    await updateStatus(Call_UniqueId, To_number, newStatus, `summary Duration=${dur}s`);
}

// ─── Main Router ──────────────────────────────────────────────────────────────

async function reserveReplayNonce(meta = {}) {
    const eventId = meta?.eventId ? String(meta.eventId).trim() : '';
    if (!eventId) return { ok: true };
    const redis = getRedis();
    const key = `replay:event:${eventId}`;
    const setOk = await redis.set(key, '1', 'EX', REPLAY_NONCE_TTL_SEC, 'NX');
    if (setOk !== 'OK') return { ok: false, reason: 'replay_event_id' };
    return { ok: true };
}

async function handleWebhook(body, meta = {}) {
    const replay = await reserveReplayNonce(meta);
    if (!replay.ok) {
        logger.warn('[Webhook] Replay blocked by event-id nonce', { reason: replay.reason });
        return;
    }
    const genericKey = `payload:${payloadHash(body)}`;
    const dedupeState = await beginDedupeProcessing(genericKey);
    if (dedupeState.skip) {
        logger.info(
            dedupeState.inflight
                ? "[Webhook] Payload processing already in-flight, skipping duplicate"
                : "[Webhook] Duplicate payload skipped"
        );
        return;
    }
    try {
        if (body.event) {
            await handleEventWebhook(body);
        } else if (body.Call_UniqueId) {
            await handleSummaryWebhook(body);
        } else {
            logger.warn("[Webhook] Unknown payload shape, skipping");
        }
        await markDedupeProcessed(genericKey);
    } catch (err) {
        await clearDedupeProcessing(genericKey);
        throw err;
    }
}

module.exports = { handleWebhook };

async function beginDedupeProcessing(key) {
    const redis = getRedis();
    const doneKey = `dedupe:done:${key}`;
    const lockKey = `dedupe:lock:${key}`;
    const done = await redis.get(doneKey);
    if (done) return { skip: true, inflight: false };
    const lock = await redis.set(lockKey, "1", "PX", PROCESSING_TTL_MS, "NX");
    if (lock !== "OK") return { skip: true, inflight: true };
    return { skip: false };
}

async function markDedupeProcessed(key) {
    const redis = getRedis();
    const doneKey = `dedupe:done:${key}`;
    const lockKey = `dedupe:lock:${key}`;
    const ttlSec = Math.ceil(PROCESSED_TTL_MS / 1000);
    await redis.multi().set(doneKey, "1", "EX", ttlSec).del(lockKey).exec();
}

async function clearDedupeProcessing(key) {
    const redis = getRedis();
    await redis.del(`dedupe:lock:${key}`);
}

function payloadHash(body) {
    return crypto.createHash("sha256").update(JSON.stringify(body || {})).digest("hex");
}
