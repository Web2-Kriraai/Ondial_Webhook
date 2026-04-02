const { ObjectId } = require("mongodb");
const crypto = require("crypto");
const { getDb } = require("./db");
const { getRedis } = require("./redis");
const { lookupMapping, lookupMappingByPhone, enrichPhoneMapping, normalizeCallId } = require("./callMapping");
const { appendCallEvent } = require("./callLogs");
const { logMissingCallMapping, previewPayload } = require("./errorLog");
const logger = require("./logger");
const callEvents = require("./events");

/**
 * callReceiveStatus values:
 *   0 = call failed (technical error)
 *   1 = call went out but user didn't answer (no answer / busy)
 *   2 = call answered / running
 *   3 = call successfully completed
 */

const PROCESSED_TTL_MS = Number(process.env.PROCESSED_WEBHOOK_TTL_MS || 2 * 60 * 60 * 1000);

// ─── DB Update Helpers ────────────────────────────────────────────────────────

async function updateByContactId(contactId, newStatus, context = "") {
    try {
        const db = getDb();
        const result = await db.collection("contactprocessings").updateOne(
            { _id: new ObjectId(contactId) },
            { $set: { callReceiveStatus: newStatus, updatedAt: new Date() } }
        );
        if (result.matchedCount === 0) {
            logger.warn(`[Webhook] No contact found for contact_id=${contactId} (${context})`);
        } else {
            logger.info(`[Webhook] callReceiveStatus=${newStatus} for contact_id=${contactId} (${context})`);
        }
    } catch (err) {
        logger.error(`[Webhook] updateByContactId failed: ${err.message}`, { contactId, context });
    }
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
            { sort: { updatedAt: -1, _id: -1 }, projection: { _id: 1 } }
        );
        if (!contact?._id) {
            logger.warn(`[Webhook] Fallback phone match found no record (${context})`, { mobile });
            return;
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
    } catch (err) {
        logger.error(`[Webhook] updateByMobile failed: ${err.message}`, { mobileRaw, context });
    }
}

/**
 * Resolve the contact from a webhook event:
 *   1. Lookup by call_id in Redis mapping (exact match)
 *   2. Lookup by phone in Redis mapping (handles Asterisk call_id change on answer)
 *   3. Fallback: DB phone scan (last resort — updates one best match)
 */
async function updateStatus(callId, mobileRaw, newStatus, context = "") {
    // Priority 1: exact call_id mapping
    let mapping = await lookupMapping(callId);
    let contact_id = mapping?.contact_id;
    let campaign_id = mapping?.campaign_id;

    if (!contact_id) {
        // Priority 2: phone-based mapping lookup
        mapping = await lookupMappingByPhone(mobileRaw);
        contact_id = mapping?.contact_id;
        campaign_id = mapping?.campaign_id;
    }

    if (contact_id) {
        await updateByContactId(contact_id, newStatus, context);
        if (campaign_id) {
            callEvents.emit("call_update", {
                campaign_id,
                call_id: callId,
                contact_id,
                status: newStatus,
                timestamp: new Date().toISOString()
            });
        }
        return;
    }

    // Priority 3: broad DB scan (last resort)
    logger.warn(`[Webhook] No mapping for call_id=${normalizeCallId(callId)} — falling back to broad phone match`);
    await updateByMobile(mobileRaw, newStatus, context);

    callEvents.emit("call_update", {
        call_id: callId,
        status: newStatus,
        timestamp: new Date().toISOString()
    });
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

    if (!hasLeadId(lead_id)) {
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

    // Always append the event to calllogs regardless of type
    await appendCallEvent(lead_id, event, body, null, { contact_id });

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
            await updateStatus(call_id, to, 2, event);
            break;

        case "call_hangup": {
            const dur = parseInt(duration, 10) || 0;
            if (dur > 0) {
                // Call completed successfully
                await updateStatus(call_id, to, 3, `${event} duration=${dur}s`);
            } else {
                // Call went out (rang) but user didn't answer — status=1
                await updateStatus(call_id, to, 1, `${event} no-answer duration=0`);
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

    const dedupeKey = Call_UniqueId ? `cdr:${Call_UniqueId}` : null;
    if (dedupeKey) {
        const stored = await reserveDedupe(dedupeKey);
        if (!stored) {
            logger.info(`[Webhook] Duplicate CDR skipped: Call_UniqueId=${Call_UniqueId}`);
            return;
        }
    }

    const dur = parseInt(Duration, 10) || 0;

    /**
     * Status mapping for CDR summary:
     *   3 = ANSWER + duration > 0  → call completed successfully
     *   1 = BUSY / NO ANSWER       → call went out but user didn't answer
     *   0 = everything else        → technical failure
     *
     * Note: We trust duration > 0 over CallStatus when ANSWER is present,
     * because some CDRs send contradictory data (e.g. NO ANSWER + duration=41s).
     */
    let newStatus;
    if (CallStatus === "ANSWER" && dur > 0) {
        newStatus = 3;  // completed
    } else if (CallStatus === "BUSY" || CallStatus === "NO ANSWER") {
        newStatus = 1;  // rang but not answered
    } else if (dur > 0) {
        newStatus = 3;  // has duration → treat as completed
    } else {
        newStatus = 0;  // technical failure
    }

    // Look up mapping first to get our lead_id; fall back to lead_id from CDR body
    const mapping = (await lookupMapping(Call_UniqueId)) || (await lookupMappingByPhone(To_number));
    const lead_id = mapping?.lead_id || String(cdr_lead_id || "");
    const contact_id = mapping?.contact_id || null;

    if (!hasLeadId(lead_id)) {
        await logMissingCallMapping({
            source: "summary_webhook",
            reason: "no_lead_id_mapping_or_cdr_body",
            call_unique_id: Call_UniqueId || null,
            to_number: To_number || null,
            contact_id_from_mapping: contact_id,
            body_preview: previewPayload(body),
        });
    }

    // Append CDR push as "cdr_push" event + update recordingUrl
    await appendCallEvent(lead_id, "cdr_push", body, RecordingURL || null, { contact_id });

    await updateStatus(Call_UniqueId, To_number, newStatus, `summary Duration=${dur}s`);
}

// ─── Main Router ──────────────────────────────────────────────────────────────

async function handleWebhook(body) {
    const genericKey = `payload:${payloadHash(body)}`;
    const stored = await reserveDedupe(genericKey);
    if (!stored) {
        logger.info("[Webhook] Duplicate payload skipped");
        return;
    }

    if (body.event) {
        await handleEventWebhook(body);
    } else if (body.Call_UniqueId) {
        await handleSummaryWebhook(body);
    } else {
        logger.warn("[Webhook] Unknown payload shape, skipping", body);
    }
}

module.exports = { handleWebhook };

async function reserveDedupe(key) {
    const redis = getRedis();
    const ttlSec = Math.ceil(PROCESSED_TTL_MS / 1000);
    const result = await redis.set(`dedupe:${key}`, "1", "EX", ttlSec, "NX");
    return result === "OK";
}

function payloadHash(body) {
    return crypto.createHash("sha256").update(JSON.stringify(body || {})).digest("hex");
}
