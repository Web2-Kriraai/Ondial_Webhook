const logger = require("./logger");

/**
 * In-memory store for call_id → { lead_id, campaign_id, contact_id } mappings.
 *
 * Flow:
 *   t=0s    — Worker calls initiate API → gets lead_id instantly
 *   t=~1-2s — Their server POSTs { lead_id, call_id, campaign_id, contact_id } to /api/call-mapping
 *   t=~2-3s — Telephony webhooks arrive with call_id → we look up mapping here ✅
 *
 * In-memory is safe here because:
 *   - Mapping is always stored BEFORE webhooks arrive (1-2s gap)
 *   - Calls complete within minutes; no need for persistence
 *   - Auto-cleanup after 1 hour prevents memory leaks
 */

const mappings = new Map();   // call_id → { lead_id, campaign_id, contact_id, phone, storedAt }
const phoneIndex = new Map(); // normalizedPhone → { lead_id, campaign_id, contact_id, storedAt }
const TTL_MS = 60 * 60 * 1000; // 1 hour auto-cleanup

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
function registerCallMapping({ lead_id, call_id, campaign_id, contact_id, phone }) {
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
        storedAt: Date.now()
    };

    mappings.set(key, entry);

    // Also index by phone for fallback lookup
    const phoneKey = entry.phone;
    if (phoneKey) {
        phoneIndex.set(phoneKey, entry);
    }

    logger.info(`[CallMapping] Stored in-memory: call_id=${key} → contact_id=${contact_id} phone=${phoneKey} (total: ${mappings.size})`);

    // Schedule auto-cleanup after TTL
    setTimeout(() => {
        if (mappings.has(key)) {
            mappings.delete(key);
        }
        if (phoneKey && phoneIndex.get(phoneKey) === entry) {
            phoneIndex.delete(phoneKey);
        }
        logger.info(`[CallMapping] Expired: call_id=${key}`);
    }, TTL_MS);
}

/**
 * Look up mapping by call_id. Returns null if not found.
 */
function lookupMapping(callId) {
    const key = normalizeCallId(callId);
    if (!key) return null;
    return mappings.get(key) || null;
}

/**
 * Look up mapping by phone number (10-digit normalized).
 * Used when Asterisk assigns a different call_id on answered leg.
 */
function lookupMappingByPhone(phone) {
    const key = normalizePhone(phone);
    if (!key) return null;
    return phoneIndex.get(key) || null;
}

/**
 * When call_initiated arrives, we know the actual `to` phone for this call_id.
 * Patch the phone into the existing mapping entry and add it to phoneIndex.
 * This ensures phone-based fallback works for call_answered/call_hangup.
 */
function enrichPhoneMapping(callId, phone) {
    const key = normalizeCallId(callId);
    if (!key) return;
    const entry = mappings.get(key);
    if (!entry) return; // mapping not registered yet — skip

    const phoneKey = normalizePhone(phone);
    if (!phoneKey || entry.phone === phoneKey) return; // already set

    entry.phone = phoneKey;
    phoneIndex.set(phoneKey, entry);
    logger.info(`[CallMapping] Phone enriched: call_id=${key} → phone=${phoneKey}`);
}

module.exports = { registerCallMapping, lookupMapping, lookupMappingByPhone, enrichPhoneMapping, normalizeCallId, normalizePhone };