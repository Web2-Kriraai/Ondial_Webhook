/**
 * Resolve inbound billing ids when webhook payload has no campaign_id / contact_id.
 */
const logger = require("../logger");
const { INBOUNDCALLLOG_COLLECTION } = require("./inboundCall");
const { extractConfigIdFromDoc, objectIdToString } = require("./mongoObjectId");

function normalizePhoneDigits(num) {
    if (!num) return null;
    const s = String(num).replace(/\D/g, "");
    if (s.length === 12 && s.startsWith("91")) return s.slice(2);
    if (s.length === 10) return s;
    return s;
}

function phoneVariants(raw) {
    const norm = normalizePhoneDigits(raw);
    const digits = String(raw || "").replace(/\D/g, "");
    const variants = new Set();
    if (raw) variants.add(String(raw).trim());
    if (digits) variants.add(digits);
    if (norm) {
        variants.add(norm);
        variants.add(`91${norm}`);
        variants.add(`+91${norm}`);
    }
    if (digits.length === 12 && digits.startsWith("91")) {
        variants.add(`+${digits}`);
    }
    return [...variants].filter(Boolean);
}

/** Campaign that owns the inbound DID (campaigns.selectedPhoneNumber). */
async function resolveCampaignIdFromInboundDid(toPhone) {
    const variants = phoneVariants(toPhone);
    if (!variants.length) return null;

    const { getDb } = require("../db");
    const db = getDb();
    const campaign = await db.collection("campaigns").findOne({
        selectedPhoneNumber: { $in: variants },
    });
    return campaign ? String(campaign._id) : null;
}

/** Recent validate row for same DID — fallback when stub doc has no config. */
async function resolveConfigFromDidHistory(toPhone) {
    const variants = phoneVariants(toPhone);
    if (!variants.length) return { configId: null, userId: null };

    const { getDb } = require("../db");
    const db = getDb();
    const doc = await db.collection(INBOUNDCALLLOG_COLLECTION).findOne(
        {
            to_number: { $in: variants },
            $or: [
                { config_id: { $exists: true, $ne: null } },
                { userId: { $exists: true, $ne: null } },
            ],
        },
        { sort: { updatedAt: -1 } }
    );

    if (!doc) return { configId: null, userId: null };
    return {
        configId: extractConfigIdFromDoc(doc),
        userId: objectIdToString(doc.userId),
    };
}

/**
 * @param {object} opts
 * @param {object} [opts.anchor] - from resolveInboundConversationAnchor
 * @param {string} [opts.toPhone] - inbound DID (webhook `to`)
 */
async function resolveInboundBillingContext({ anchor, toPhone }) {
    const doc = anchor?.doc;
    const isValidateDoc = doc?.source === "validate";
    let campaignId = null;
    let userId = anchor?.userId || objectIdToString(doc?.userId);
    let source = null;

    // Validate rows store inbound bot config_id — bill via DID-owned campaign, not bot config.
    if (!isValidateDoc) {
        campaignId = extractConfigIdFromDoc(doc) || anchor?.campaignId || null;
        if (campaignId) source = "inbound_doc";
    }

    if (!campaignId && toPhone) {
        campaignId = await resolveCampaignIdFromInboundDid(toPhone);
        if (campaignId) source = "campaign_did";
    }

    if (!campaignId && toPhone) {
        const hist = await resolveConfigFromDidHistory(toPhone);
        if (hist.configId) {
            campaignId = hist.configId;
            source = "did_history_config";
        }
        if (!userId && hist.userId) userId = hist.userId;
    }

    if (source) {
        logger.info("[InboundBilling] Resolved billing context", {
            toPhone: toPhone || null,
            campaignId,
            userId: userId || null,
            source,
            call_sid: anchor?.callSid || null,
            validate_doc: isValidateDoc,
        });
    }

    return { campaignId, userId, source };
}

module.exports = {
    resolveCampaignIdFromInboundDid,
    resolveInboundBillingContext,
    phoneVariants,
};
