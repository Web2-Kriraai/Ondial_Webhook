const logger = require("./logger");
const { getRedis } = require("./redis");

/**
 * Redis-backed store for call_id → { lead_id, campaign_id, contact_id } mappings.
 *
 * Flow:
 *   t=0s    — Worker calls initiate API → gets lead_id instantly
 *   t=~1-2s — Their server POSTs { lead_id, call_id, campaign_id, contact_id } to /api/call-mapping
 *   t=~2-3s — Telephony webhooks arrive with call_id → we look up mapping here ✅
 *
 * Redis is used so mapping remains shared across app instances.
 * TTL cleanup keeps keys short-lived and memory usage bounded.
 */

const TTL_MS = Number(process.env.MAPPING_TTL_MS || 60 * 60 * 1000);

/**
 * Normalize call_id: strip "cid_" prefix if present.
 * "cid_1774064469.3997258" → "1774064469.3997258"
 */
function normalizeCallId(callId) {
    if (!callId) return null;
    return String(callId).replace(/^cid_/, "");
}

/**
 * Strip country code to get a 10-digit mobile for indexing.
 */
function normalizePhone(num) {
    if (!num) return null;
    const s = String(num).replace(/\D/g, "");
    if (s.length === 12 && s.startsWith("91")) return s.slice(2);
    if (s.length === 10) return s;
    return s;
}

/**
 * Store mapping: call_id → { lead_id, campaign_id, contact_id, phone }
 * Also indexes by phone so lookup works even when Asterisk changes call_id on answer.
 */
async function registerCallMapping({ lead_id, call_id, campaign_id, contact_id, phone, collectionName }) {
    const key = normalizeCallId(call_id);
    if (!key) {
        logger.warn("[CallMapping] Invalid call_id, cannot store mapping");
        return;
    }

    const entry = {
        lead_id: String(lead_id || ""),
        campaign_id: String(campaign_id || ""),
        contact_id: String(contact_id || ""),
        phone: normalizePhone(phone) || "",
        ...(collectionName ? { collectionName: String(collectionName) } : {}),
        storedAt: Date.now()
    };

    const redis = getRedis();
    const payload = JSON.stringify({
        ...entry,
        updatedAt: Date.now(),
    });
    const ttlSec = Math.ceil(TTL_MS / 1000);
    await redis.set(`map:call:${key}`, payload, "EX", ttlSec);
    if (entry.phone) {
        await redis.set(`map:phone:${entry.phone}`, payload, "EX", ttlSec);
    }

    logger.info(`[CallMapping] Stored: call_id=${key} contact_id=${contact_id} phone=${entry.phone}`);
}

/**
 * Look up mapping by call_id. Returns null if not found.
 */
async function lookupMapping(callId) {
    const key = normalizeCallId(callId);
    if (!key) return null;
    const redis = getRedis();
    const raw = await redis.get(`map:call:${key}`);
    return raw ? JSON.parse(raw) : null;
}

/**
 * Look up mapping by phone number (10-digit normalized).
 * Used when Asterisk assigns a different call_id on answered leg.
 */
async function lookupMappingByPhone(phone) {
    const key = normalizePhone(phone);
    if (!key) return null;
    const redis = getRedis();
    const raw = await redis.get(`map:phone:${key}`);
    return raw ? JSON.parse(raw) : null;
}

/**
 * When call_initiated arrives, we know the actual `to` phone for this call_id.
 * Patch the phone into the existing mapping entry and add it to phoneIndex.
 * This ensures phone-based fallback works for call_answered/call_hangup.
 */
async function enrichPhoneMapping(callId, phone) {
    const key = normalizeCallId(callId);
    if (!key) return;
    const redis = getRedis();
    const raw = await redis.get(`map:call:${key}`);
    const entry = raw ? JSON.parse(raw) : null;
    if (!entry) return; // mapping not registered yet — skip

    const phoneKey = normalizePhone(phone);
    if (!phoneKey || entry.phone === phoneKey) return; // already set

    entry.phone = phoneKey;
    entry.updatedAt = Date.now();
    const payload = JSON.stringify(entry);
    const ttlSec = Math.ceil(TTL_MS / 1000);
    await redis.set(`map:call:${key}`, payload, "EX", ttlSec);
    await redis.set(`map:phone:${phoneKey}`, payload, "EX", ttlSec);
    logger.info(`[CallMapping] Phone enriched: call_id=${key} → phone=${phoneKey}`);
}

/**
 * Mark that this call reached the answered stage (Redis, same TTL as mappings).
 * Indexed by call_id and by `to` phone so hangup/CDR still match if Asterisk swaps call_id.
 */
async function markCallAnswered(callId, phoneRaw) {
    const redis = getRedis();
    const ttlSec = Math.ceil(TTL_MS / 1000);
    const cid = normalizeCallId(callId);
    if (cid) {
        await redis.set(`map:answered:${cid}`, "1", "EX", ttlSec);
    }
    const phoneKey = normalizePhone(phoneRaw);
    if (phoneKey) {
        await redis.set(`map:answered:phone:${phoneKey}`, "1", "EX", ttlSec);
    }
}

/**
 * True if call_answered was recorded for this call_id or normalized destination phone.
 */
async function hasAnsweredFlag(callId, phoneRaw) {
    const redis = getRedis();
    const cid = normalizeCallId(callId);
    if (cid) {
        const v = await redis.get(`map:answered:${cid}`);
        if (v === "1") return true;
    }
    const phoneKey = normalizePhone(phoneRaw);
    if (phoneKey) {
        const v = await redis.get(`map:answered:phone:${phoneKey}`);
        if (v === "1") return true;
    }
    return false;
}

function normalizeTwilioCallSid(callSid) {
    if (!callSid) return null;
    const sid = String(callSid).trim();
    return sid || null;
}

/**
 * Store mapping for Twilio callback correlation:
 * twilio CallSid -> { call_id, lead_id, campaign_id, contact_id }.
 */
async function registerTwilioCallSidMapping({
    twilio_call_sid,
    campaign_id,
    contact_id,
    collectionName,
}) {
    const sid = normalizeTwilioCallSid(twilio_call_sid);
    if (!sid) {
        logger.warn("[CallMapping] Invalid twilio_call_sid, cannot store mapping");
        return;
    }

    const entry = {
        twilio_call_sid: sid,
        campaign_id: String(campaign_id || ""),
        contact_id: String(contact_id || ""),
        collectionName: collectionName ? String(collectionName) : "",
        updatedAt: Date.now(),
    };

    const redis = getRedis();
    const ttlSec = Math.ceil(TTL_MS / 1000);
    await redis.set(`map:twilio:sid:${sid}`, JSON.stringify(entry), "EX", ttlSec);
    logger.info("[CallMapping] Stored Twilio SID mapping", {
        twilio_call_sid: sid,
        contact_id: entry.contact_id,
    });
}

async function lookupTwilioCallSidMapping(twilioCallSid) {
    const sid = normalizeTwilioCallSid(twilioCallSid);
    if (!sid) return null;
    const redis = getRedis();
    const raw = await redis.get(`map:twilio:sid:${sid}`);
    return raw ? JSON.parse(raw) : null;
}

module.exports = {
    registerCallMapping,
    lookupMapping,
    lookupMappingByPhone,
    enrichPhoneMapping,
    normalizeCallId,
    normalizePhone,
    markCallAnswered,
    hasAnsweredFlag,
    registerTwilioCallSidMapping,
    lookupTwilioCallSidMapping,
    normalizeTwilioCallSid,
};