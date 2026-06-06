/**
 * Link telephony provider call.id (UUID) to Ondial InboundConversation.call_sid (call_<hex>).
 */
const logger = require("../logger");
const { INBOUNDCALLLOG_COLLECTION } = require("./inboundCall");

function isMongoObjectIdString(s) {
    return typeof s === "string" && /^[a-f0-9]{24}$/i.test(s);
}

function objectIdToString(v) {
    if (v == null) return null;
    if (typeof v === "string") {
        const t = v.trim();
        return isMongoObjectIdString(t) ? t : null;
    }
    if (typeof v === "object" && typeof v.toString === "function") {
        const t = v.toString();
        return isMongoObjectIdString(t) ? t : null;
    }
    return null;
}

/** 92bd645e-e976-4a14-969a-5176d7fed547 → call_92bd645ee9764a14969a5176d7fed547 */
function providerCallIdToCallSid(providerCallId) {
    const raw = String(providerCallId || "").trim();
    if (!raw) return null;
    if (/^call_[a-f0-9]{32}$/i.test(raw)) return raw.toLowerCase();
    const hex = raw.replace(/-/g, "").toLowerCase();
    if (!/^[a-f0-9]{32}$/.test(hex)) return null;
    return `call_${hex}`;
}

/** call_92bd645ee9764a14969a5176d7fed547 → 92bd645e-e976-4a14-969a-5176d7fed547 */
function callSidToProviderCallId(callSid) {
    const m = String(callSid || "")
        .trim()
        .match(/^call_([a-f0-9]{32})$/i);
    if (!m) return null;
    const h = m[1];
    return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

/**
 * Find the Ondial InboundConversation row (validate / UI) without /api/inbound-mapping.
 * @returns {Promise<{ doc, filter, providerCallId, callSid, campaignId, contactId, userId }>}
 */
async function resolveInboundConversationAnchor(providerCallId) {
    const providerId = String(providerCallId || "").trim();
    const callSid = providerCallIdToCallSid(providerId);
    const hyphenated = callSidToProviderCallId(callSid) || providerId;

    const empty = {
        doc: null,
        filter: callSid ? { call_sid: callSid } : { call_id: hyphenated },
        providerCallId: hyphenated,
        callSid,
        campaignId: null,
        contactId: null,
        userId: null,
    };

    if (!providerId && !callSid) return empty;

    try {
        const { getDb } = require("../db");
        const db = getDb();
        const or = [{ call_id: providerId }, { call_id: hyphenated }];
        if (callSid) or.push({ call_sid: callSid });

        const doc = await db.collection(INBOUNDCALLLOG_COLLECTION).findOne({ $or: or });

        if (!doc) {
            return empty;
        }

        const filter = doc.call_sid ? { call_sid: String(doc.call_sid) } : { call_id: doc.call_id };

        return {
            doc,
            filter,
            providerCallId: hyphenated,
            callSid: doc.call_sid ? String(doc.call_sid) : callSid,
            campaignId:
                objectIdToString(doc.config_id) ||
                objectIdToString(doc.campaign_id) ||
                objectIdToString(doc.campaignId),
            contactId: objectIdToString(doc.contact_id),
            userId: objectIdToString(doc.userId),
        };
    } catch (err) {
        logger.warn("[InboundAnchor] lookup failed", {
            providerCallId: providerId,
            error: err.message,
        });
        return empty;
    }
}

module.exports = {
    providerCallIdToCallSid,
    callSidToProviderCallId,
    resolveInboundConversationAnchor,
};
