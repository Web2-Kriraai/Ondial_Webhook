const { ObjectId } = require("mongodb");
const crypto = require("crypto");
const { getDb } = require("./db");
const { getRedis } = require("./redis");
const {
    lookupMapping,
    lookupMappingByPhone,
    enrichPhoneMapping,
    registerCallMapping,
    normalizeCallId,
    normalizePhone,
    markCallAnswered,
    hasAnsweredFlag,
} = require("./callMapping");
const { extractCustomParameters, pickNonEmpty } = require("./lib/customParameters");
const {
    appendCallEvent,
    INBOUNDCALLLOG_COLLECTION,
    markInboundConversationCompleted,
} = require("./callLogs");
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

            console.log("[Webhook] updating callReceiveStatus", {
                contact_id: cid,
                newStatus,
                context,
            });

            const result = await db.collection("contactprocessings").updateOne(
                filter,
                { $set: { callReceiveStatus: newStatus, updatedAt: new Date() } }
            );

            console.log("[Webhook] callReceiveStatus update result", {
                contact_id: cid,
                matchedCount: result.matchedCount,
                modifiedCount: result.modifiedCount,
                newStatus,
            });

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
 * Campaign id is not always echoed in telephony customParameters — load from contact row.
 */
async function resolveCampaignIdFromContact(contactId) {
    if (!isMongoObjectIdString(contactId)) return null;
    try {
        const db = getDb();
        const doc = await db.collection("contactprocessings").findOne(
            { _id: new ObjectId(String(contactId)) },
            { projection: { campaign_id: 1, config_id: 1 } }
        );
        return pickNonEmpty(doc?.campaign_id, doc?.config_id);
    } catch (err) {
        logger.warn("[Webhook] resolveCampaignIdFromContact failed", {
            contactId: String(contactId),
            error: err.message,
        });
        return null;
    }
}

async function enrichIdentityFromContact(identity) {
    if (!identity?.contact_id || identity.campaign_id) return identity;
    const campaign_id = await resolveCampaignIdFromContact(identity.contact_id);
    if (!campaign_id) return identity;
    return { ...identity, campaign_id };
}

/**
 * Payload-first identity resolver.
 *
 * New outbound flow: telephony webhook payload itself carries `contact_id`, `campaign_id`,
 * and `call_unique_id` (alias of `call_id`), so we no longer require /api/call-mapping.
 * Redis mapping is still consulted as a fallback so:
 *   - legacy /api/call-mapping callers keep working unchanged,
 *   - inbound flag (collectionName === InboundConversation) keeps coming from /api/inbound-mapping.
 */
function resolveIdentityFromPayload(body, mapping) {
    const rawCallId =
        body?.call_unique_id ?? body?.call_id ?? body?.Call_UniqueId ?? mapping?.call_id;
    const callUniqueId = pickNonEmpty(body?.call_unique_id, rawCallId);
    const contactId = pickNonEmpty(body?.contact_id, mapping?.contact_id);
    const leadId = pickNonEmpty(body?.lead_id, callUniqueId, mapping?.lead_id);

    return {
        callId: callUniqueId || rawCallId,
        normalizedCallId: normalizeCallId(callUniqueId || rawCallId),
        providerCallId: pickNonEmpty(body?.provider_call_id, mapping?.provider_call_id),
        contact_id: contactId,
        campaign_id: pickNonEmpty(body?.campaign_id, mapping?.campaign_id),
        lead_id: leadId,
        call_unique_id: callUniqueId,
        collectionName: mapping?.collectionName || null,
    };
}

/**
 * Resolve the contact from a webhook event:
 *   - If caller provides `precomputed` (payload-first identity), use it directly to avoid Redis lookups.
 *   - Else fall back to legacy mapping resolution:
 *       1. Lookup by call_id in Redis mapping
 *       2. Lookup by phone in Redis mapping
 *       3. DB phone scan
 */
async function updateStatus(callId, mobileRaw, newStatus, context = "", precomputed = null) {
    let contact_id = precomputed?.contact_id || null;
    let campaign_id = precomputed?.campaign_id || null;
    let collectionName = precomputed?.collectionName || null;

    if (!contact_id) {
        let mapping = await lookupMapping(callId);
        const inboundByCallId = mapping?.collectionName === INBOUNDCALLLOG_COLLECTION;

        if (!mapping?.contact_id && mobileRaw && !inboundByCallId) {
            const byPhone = await lookupMappingByPhone(mobileRaw);
            if (byPhone) mapping = byPhone;
        }

        contact_id = mapping?.contact_id || null;
        campaign_id = campaign_id || mapping?.campaign_id || null;
        collectionName = collectionName || mapping?.collectionName || null;
    }

    const isInboundMapping = collectionName === INBOUNDCALLLOG_COLLECTION;

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

    // Payload-first identity.
    // - Skip phone-fallback Redis GET when payload already has contact_id.
    // - Still do one call_id Redis GET to detect inbound mapping coming from legacy /api/inbound-mapping.
    let mapping = await lookupMapping(call_id);
    if (!mapping && !pickNonEmpty(body?.contact_id) && to) {
        mapping = await lookupMappingByPhone(to);
    }
    let identity = resolveIdentityFromPayload(body, mapping);
    if (!identity.campaign_id && identity.contact_id) {
        identity = await enrichIdentityFromContact(identity);
    }

    const lead_id = identity.lead_id;
    const contact_id = identity.contact_id;

    // Direction-driven collection routing: provider tells us inbound/outbound directly via call.direction.
    const isInboundByPayload = String(body?.direction || "").toLowerCase() === "inbound";
    const isInboundByMapping = identity.collectionName === INBOUNDCALLLOG_COLLECTION;
    const isInboundLog = isInboundByPayload || isInboundByMapping;
    const collectionName = isInboundLog ? INBOUNDCALLLOG_COLLECTION : identity.collectionName;
    const docKey = identity.normalizedCallId;

    console.log("[Webhook][Event] received", {
        event: event || null,
        call_id: docKey || call_id || null,
        contact_id: contact_id || null,
        campaign_id: identity.campaign_id || null,
        lead_id: lead_id || null,
        to: to || null,
        duration: duration ?? null,
        callStatus: body?.callStatus || null,
        answered: body?.answered ?? null,
        direction: body?.direction || null,
        source: mapping ? "redis+payload" : "payload-only",
        isInbound: isInboundLog,
    });

    if (!contact_id && !hasLeadId(lead_id) && !isInboundLog) {
        await logMissingCallMapping({
            source: "event_webhook",
            reason: "no_identity_in_payload_or_mapping",
            event: event || null,
            call_id: call_id || null,
            to: to || null,
            contact_id_from_mapping: mapping?.contact_id || null,
            body_preview: previewPayload(body),
        });
    }

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
        // Outbound key precedence: explicit lead_id (legacy /api/call-mapping or payload),
        // else fall back to normalized call_id so each call gets its own document.
        const outboundKey = lead_id || docKey;
        if (outboundKey) {
            await appendCallEvent(outboundKey, event, body, null, {
                contact_id,
                campaign_id: identity.campaign_id,
                callId: docKey,
                collectionName,
            });
        } else {
            logger.warn("[Webhook] Outbound event missing both lead_id and call_id — skipping append", { event });
        }
    }

    switch (event) {
        case "call_initiated":
        case "call_ringing":
            // Register the destination phone in the mapping so that
            // subsequent call_answered/call_hangup (which may have a different
            // Asterisk call_id) can still do a precise contact lookup by phone.
            // Harmless no-op when mapping doesn't exist (new payload-first flow).
            if (event === "call_initiated") {
                const mapCallId = identity.normalizedCallId || normalizeCallId(call_id);
                if (mapCallId && contact_id) {
                    await registerCallMapping({
                        call_id: mapCallId,
                        lead_id: identity.lead_id || mapCallId,
                        campaign_id: identity.campaign_id || "",
                        contact_id,
                        phone: to,
                    });
                }
                await enrichPhoneMapping(call_id, to);
            }
            // Do NOT update DB — worker already sets status=1 when call is placed.
            logger.info(`[Webhook] Ignoring ${event} for call_id=${call_id} — no DB update`);

            // Fire SSE anyway so UI call logs reload the ringing/initiated state
            callEvents.emit("call_update", {
                campaign_id: identity.campaign_id || null,
                call_id,
                event,
                timestamp: new Date().toISOString()
            });
            break;

        case "call_answered":
            await markCallAnswered(call_id, to);
            await updateStatus(call_id, to, 2, event, identity);
            break;

        case "call_hangup": {
            const dur = parseInt(duration, 10) || 0;

            // Provider-authoritative status when present (new provider sends callStatus + answered flag).
            // Fall back to Redis answered-flag + event-history scan only when callStatus is absent.
            let hangupStatus;
            let statusSource;
            if (body?.callStatus) {
                const cs = String(body.callStatus).toUpperCase();
                if (cs === "ANSWER" || cs === "ANSWERED") {
                    hangupStatus = 3;
                } else if (cs === "BUSY" || cs === "NO ANSWER" || cs === "NOANSWER" || cs === "FAILED") {
                    hangupStatus = 1;
                } else {
                    hangupStatus = body?.answered ? 3 : 1;
                }
                statusSource = `callStatus=${body.callStatus}`;
            } else {
                const answeredStageSeen =
                    body?.answered === true ||
                    (await hasAnsweredFlag(call_id, to)) ||
                    (await didCallReachAnsweredStage({
                        leadId: lead_id || docKey,
                        callId: call_id,
                        collectionName,
                        toPhone: to,
                    }));
                hangupStatus = answeredStageSeen ? 3 : 1;
                statusSource = `answeredSeen=${answeredStageSeen ? "yes" : "no"}`;
            }

            await updateStatus(
                call_id,
                to,
                hangupStatus,
                `${event} duration=${dur}s ${statusSource}`,
                identity
            );
            // Outbound ondial.ai notify: only when inbound call ends (not on ring/initiated/answered)
            if (isInboundLog && docKey) {
                await markInboundConversationCompleted(docKey);
                await notifyOndialInboundWebhook(docKey);
            }
            break;
        }

        case "call_failed": {
            if (isInboundLog && docKey) {
                await markInboundConversationCompleted(docKey);
            }
            // Technical failure (network, provider error) — status=0
            await updateStatus(call_id, to, 0, event, identity);
            break;
        }

        case "call_ended":
            // Provider fires `call.ended` before `call.completed`; events already appended above.
            logger.info(
                `[Webhook] call_ended recorded for call_id=${call_id} — status/credit handled on call_hangup`
            );
            break;

        default:
            logger.info(`[Webhook] Unhandled event: ${event}`);
    }
}

// ─── Summary (CDR Push) Webhook Handler ──────────────────────────────────────

async function handleSummaryWebhook(body) {
    const { Call_UniqueId, To_number, Duration, CallStatus, RecordingURL, lead_id: cdr_lead_id } = body;

    const dur = parseInt(Duration, 10) || 0;

    // Payload-first identity: CDR summary uses Call_UniqueId; allow payload-provided contact_id/campaign_id.
    let mapping = await lookupMapping(Call_UniqueId);
    if (!mapping && !pickNonEmpty(body?.contact_id) && To_number) {
        mapping = await lookupMappingByPhone(To_number);
    }
    let identity = resolveIdentityFromPayload(
        {
            call_id: Call_UniqueId,
            call_unique_id: body?.call_unique_id || Call_UniqueId,
            contact_id: body?.contact_id,
            campaign_id: body?.campaign_id,
            lead_id: cdr_lead_id,
        },
        mapping
    );
    if (!identity.campaign_id && identity.contact_id) {
        identity = await enrichIdentityFromContact(identity);
    }

    const lead_id = identity.lead_id || "";
    const contact_id = identity.contact_id || null;
    const collectionName = identity.collectionName;
    const isInboundLog = collectionName === INBOUNDCALLLOG_COLLECTION;
    const cdrCallKey = identity.normalizedCallId;

    console.log("[Webhook][Summary] received", {
        Call_UniqueId: cdrCallKey || Call_UniqueId || null,
        CallStatus: CallStatus || null,
        Duration: dur,
        contact_id: contact_id || null,
        campaign_id: identity.campaign_id || null,
        lead_id: lead_id || null,
        to: To_number || null,
        source: mapping ? "redis+payload" : "payload-only",
        isInbound: isInboundLog,
    });

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
            leadId: lead_id || cdrCallKey,
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

    if (!contact_id && !hasLeadId(lead_id) && !(isInboundLog && cdrCallKey)) {
        await logMissingCallMapping({
            source: "summary_webhook",
            reason: "no_identity_in_payload_or_mapping",
            call_unique_id: Call_UniqueId || null,
            to_number: To_number || null,
            contact_id_from_mapping: mapping?.contact_id || null,
            body_preview: previewPayload(body),
        });
    }

    if (isInboundLog && cdrCallKey) {
        await appendCallEvent(cdrCallKey, "cdr_push", body, RecordingURL || null, {
            contact_id,
            collectionName,
            callId: cdrCallKey,
        });
        await markInboundConversationCompleted(cdrCallKey);
        await notifyOndialInboundWebhook(cdrCallKey);
    } else {
        // Outbound key precedence: lead_id (legacy/payload) else call_id (new payload-first flow).
        const outboundKey = lead_id || cdrCallKey;
        if (outboundKey) {
            await appendCallEvent(outboundKey, "cdr_push", body, RecordingURL || null, {
                contact_id,
                campaign_id: identity.campaign_id,
                callId: cdrCallKey,
                collectionName,
            });
        } else {
            logger.warn("[Webhook] Outbound summary missing both lead_id and Call_UniqueId — skipping append");
        }
    }

    if (newStatus === 3) {
        await markCallAnswered(Call_UniqueId, To_number);
    }

    await updateStatus(Call_UniqueId, To_number, newStatus, `summary Duration=${dur}s`, identity);
}

// ─── Provider Payload Normalizer ─────────────────────────────────────────────
//
// New telephony provider sends nested payloads with custom event names:
//   {
//     event: "call.initiated" | "call.ringing" | "call.answered" | "call.completed" | "call.failed",
//     call: {
//       id, direction, from, to, status, callStatus, hangupCause,
//       startedAt, ringingAt, answeredAt, endedAt,
//       ringDurationSec, durationSec,
//       customParameters: { contact_id, campaign_id, call_unique_id, lead_id? }
//     },
//     legs: [...]
//   }
//
// We convert it once at the router boundary to the legacy flat shape:
//   { event: "call_initiated"|..., call_id, to, from, duration, contact_id,
//     campaign_id, call_unique_id, lead_id, callStatus, answered, direction,
//     recordingUrl, _raw }
//
// Legacy payloads (already flat) pass through untouched.
const NEW_EVENT_MAP = {
    "call.initiated": "call_initiated",
    "call.ringing": "call_ringing",
    "call.answered": "call_answered",
    "call.ended": "call_ended",
    "call.completed": "call_hangup",
    "call.failed": "call_failed",
};

function isNewProviderShape(body) {
    return (
        body &&
        typeof body === "object" &&
        typeof body.event === "string" &&
        body.event.includes(".") &&
        body.call &&
        typeof body.call === "object"
    );
}

function deriveDurationSec(call) {
    if (call.durationSec != null && !Number.isNaN(Number(call.durationSec))) {
        return Number(call.durationSec);
    }
    if (call.answeredAt && call.endedAt) {
        const ms = new Date(call.endedAt).getTime() - new Date(call.answeredAt).getTime();
        if (Number.isFinite(ms) && ms >= 0) return Math.floor(ms / 1000);
    }
    return null;
}

function normalizeWebhookPayload(body) {
    if (!isNewProviderShape(body)) return body;

    const c = body.call || {};
    const cp = extractCustomParameters(c, body);

    const callUniqueId = cp.call_unique_id;
    const providerCallId = c.id || null;
    const canonicalCallId = callUniqueId || providerCallId;

    console.log("[Webhook] customParameters received", {
        event: body.event,
        provider_call_id: providerCallId,
        call_unique_id: callUniqueId,
        direction: c.direction || null,
        customParameters: cp.raw,
        extracted: {
            contact_id: cp.contact_id,
            campaign_id: cp.campaign_id,
            call_unique_id: callUniqueId,
            lead_id: pickNonEmpty(cp.lead_id, callUniqueId),
        },
    });

    const normEvent = NEW_EVENT_MAP[body.event] || body.event;
    const durationSec = deriveDurationSec(c);

    return {
        event: normEvent,
        call_id: canonicalCallId,
        to: c.to || null,
        from: c.from || null,
        duration: durationSec,
        contact_id: cp.contact_id,
        campaign_id: cp.campaign_id,
        call_unique_id: callUniqueId,
        lead_id: pickNonEmpty(cp.lead_id, callUniqueId),
        provider_call_id: providerCallId,
        callStatus: c.callStatus || null,
        answered: c.answeredAt != null,
        direction: c.direction || null,
        recordingUrl: c.recordingUrl || c.recordingURL || c.recording_url || null,
        _raw: body,
    };
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
        const wasNewShape = isNewProviderShape(body);
        const normalized = wasNewShape ? normalizeWebhookPayload(body) : body;
        const payloadShape = normalized.event ? "event" : normalized.Call_UniqueId ? "summary" : "unknown";

        console.log("[Webhook] received payload", {
            shape: payloadShape,
            providerShape: wasNewShape ? "new" : "legacy",
            normalizedEvent: normalized.event || null,
            body,
        });

        if (normalized.event) {
            await handleEventWebhook(normalized);
        } else if (normalized.Call_UniqueId) {
            await handleSummaryWebhook(normalized);
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
