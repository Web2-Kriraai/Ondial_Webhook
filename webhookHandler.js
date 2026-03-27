const { ObjectId } = require("mongodb");
const { getDb } = require("./db");
const { lookupMapping, lookupMappingByPhone, enrichPhoneMapping, normalizeCallId } = require("./callMapping");
const { appendCallEvent } = require("./callLogs");
const logger = require("./logger");
const callEvents = require("./events");

/**
 * callReceiveStatus values:
 *   0 = call failed (technical error)
 *   1 = call went out but user didn't answer (no answer / busy)
 *   2 = call answered / running
 *   3 = call successfully completed
 */

// ─── CDR Deduplication ───────────────────────────────────────────────────────
// Stores Call_UniqueId values we've already processed to prevent duplicate
// CDR events when the provider retries the same webhook hundreds of times.
const processedCDRs = new Set();
const CDR_TTL_MS = 2 * 60 * 60 * 1000; // 2 hours

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
        const result = await db.collection("contactprocessings").updateMany(
            { mobileNumber: { $in: variants } },
            { $set: { callReceiveStatus: newStatus, updatedAt: new Date() } }
        );
        logger.warn(
            `[Webhook] Fallback phone match — callReceiveStatus=${newStatus} for mobile ${mobile} matched: ${result.matchedCount} (${context})`
        );
    } catch (err) {
        logger.error(`[Webhook] updateByMobile failed: ${err.message}`, { mobileRaw, context });
    }
}

/**
 * Resolve the contact from a webhook event:
 *   1. Lookup by call_id in memory map (exact match)
 *   2. Lookup by phone in memory map (handles Asterisk call_id change on answer)
 *   3. Fallback: broad DB phone scan (last resort — may match multiple contacts)
 */
async function updateStatus(callId, mobileRaw, newStatus, context = "") {
    // Priority 1: exact call_id mapping
    let mapping = lookupMapping(callId);
    let contact_id = mapping?.contact_id;
    let campaign_id = mapping?.campaign_id;

    if (!contact_id) {
        // Priority 2: phone-based mapping lookup
        mapping = lookupMappingByPhone(mobileRaw);
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

async function handleEventWebhook(body) {
    const { event, call_id, to, duration } = body;
    const mapping = lookupMapping(call_id) || lookupMappingByPhone(to);
    const lead_id = mapping?.lead_id || null;

    // Always append the event to calllogs regardless of type
    await appendCallEvent(lead_id, event, body);

    switch (event) {
        case "call_initiated":
        case "call_ringing":
            // Register the destination phone in the mapping so that
            // subsequent call_answered/call_hangup (which may have a different
            // Asterisk call_id) can still do a precise contact lookup by phone.
            if (event === "call_initiated") {
                enrichPhoneMapping(call_id, to);
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

    // ── Deduplication: skip if we already processed this CDR ──────────────────
    if (Call_UniqueId && processedCDRs.has(Call_UniqueId)) {
        logger.info(`[Webhook] Duplicate CDR skipped: Call_UniqueId=${Call_UniqueId}`);
        return;
    }
    if (Call_UniqueId) {
        processedCDRs.add(Call_UniqueId);
        // Auto-remove after TTL to prevent memory growth
        setTimeout(() => processedCDRs.delete(Call_UniqueId), CDR_TTL_MS);
    }
    // ─────────────────────────────────────────────────────────────────────────

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
    const mapping = lookupMapping(Call_UniqueId) || lookupMappingByPhone(To_number);
    const lead_id = mapping?.lead_id || String(cdr_lead_id || "");

    // Append CDR push as "cdr_push" event + update recordingUrl
    await appendCallEvent(lead_id, "cdr_push", body, RecordingURL || null);

    await updateStatus(Call_UniqueId, To_number, newStatus, `summary Duration=${dur}s`);
}

// ─── Main Router ──────────────────────────────────────────────────────────────

async function handleWebhook(body) {
    if (body.event) {
        await handleEventWebhook(body);
    } else if (body.Call_UniqueId) {
        await handleSummaryWebhook(body);
    } else {
        logger.warn("[Webhook] Unknown payload shape, skipping", body);
    }
}

module.exports = { handleWebhook };
