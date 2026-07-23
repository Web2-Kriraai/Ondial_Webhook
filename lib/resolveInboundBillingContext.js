/**
 * Resolve inbound billing ids when webhook payload has no campaign_id / contact_id.
 */
const logger = require("../logger");
const { ObjectId } = require("mongodb");
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

/** Campaign that owns the inbound DID — only same-account, non-dead statuses. */
async function resolveCampaignIdFromInboundDid(toPhone, { userId, userEmail } = {}) {
    const variants = phoneVariants(toPhone);
    if (!variants.length) return null;

    const { getDb } = require("../db");
    const db = getDb();

    const deadStatuses = ["draft", "archived", "expired", "deleted"];
    const base = {
        selectedPhoneNumber: { $in: variants },
        status: { $nin: deadStatuses },
    };

    let ownerEmail = userEmail ? String(userEmail).trim().toLowerCase() : null;
    let ownerId = objectIdToString(userId);
    if (ownerId && !ownerEmail) {
        try {
            const u = await db.collection("users").findOne(
                { _id: new ObjectId(ownerId) },
                { projection: { email: 1 } }
            );
            if (u?.email) ownerEmail = String(u.email).trim().toLowerCase();
        } catch {
            /* ignore */
        }
    }

    // Never pick another account's leftover selectedPhoneNumber (common data pollution).
    if (ownerId || ownerEmail) {
        const ownerOr = [];
        if (ownerId) {
            ownerOr.push({ userId: ownerId });
            try {
                ownerOr.push({ userId: new ObjectId(ownerId) });
            } catch {
                /* ignore */
            }
        }
        if (ownerEmail) ownerOr.push({ createdBy: ownerEmail }, { createdBy: userEmail });
        const campaign = await db.collection("campaigns").findOne({
            ...base,
            $or: ownerOr,
        });
        return campaign ? String(campaign._id) : null;
    }

    return null;
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

/** Inbound bot config that owns the DID (inboundconfigs.phoneNumber). */
async function resolveInboundConfigIdFromDid(toPhone) {
    const variants = phoneVariants(toPhone);
    if (!variants.length) return { configId: null, userId: null };

    const { getDb } = require("../db");
    const db = getDb();
    const cfg = await db.collection("inboundconfigs").findOne({
        phoneNumber: { $in: variants },
    });
    if (!cfg) return { configId: null, userId: null };
    return {
        configId: objectIdToString(cfg._id),
        userId: objectIdToString(cfg.userId),
    };
}

/**
 * @param {object} opts
 * @param {object} [opts.anchor] - from resolveInboundConversationAnchor
 * @param {string} [opts.toPhone] - inbound DID (webhook `to`)
 * @returns {Promise<{ campaignId: string|null, inboundConfigId: string|null, userId: string|null, source: string|null }>}
 */
async function resolveInboundBillingContext({ anchor, toPhone }) {
    const doc = anchor?.doc;
    const isValidateDoc = doc?.source === "validate";
    let campaignId = null;
    let inboundConfigId = null;
    let userId = anchor?.userId || objectIdToString(doc?.userId);
    let source = null;

    // Validate / UI rows store inbound bot config_id (not a campaigns._id).
    // Prefer this over any campaign DID match — foreign/draft campaigns often
    // still have selectedPhoneNumber set to the same number.
    inboundConfigId =
        objectIdToString(doc?.config_id) ||
        objectIdToString(doc?.inboundConfigId) ||
        objectIdToString(doc?.configId) ||
        null;

    // Ignore polluted stub config_id when it is actually a campaigns._id.
    if (inboundConfigId && !isValidateDoc) {
        const { getDb } = require("../db");
        const db = getDb();
        try {
            const asCfg = await db
                .collection("inboundconfigs")
                .findOne({ _id: new ObjectId(inboundConfigId) }, { projection: { _id: 1 } });
            if (!asCfg) inboundConfigId = null;
        } catch {
            inboundConfigId = null;
        }
    }

    if (!inboundConfigId && toPhone) {
        const byDid = await resolveInboundConfigIdFromDid(toPhone);
        if (byDid.configId) {
            inboundConfigId = byDid.configId;
            source = "inbound_config_did";
        }
        if (!userId && byDid.userId) userId = byDid.userId;
    }

    if (!inboundConfigId && toPhone) {
        const hist = await resolveConfigFromDidHistory(toPhone);
        if (hist.configId) {
            // Only accept history id if it is a real inboundconfig
            const { getDb } = require("../db");
            const db = getDb();
            try {
                const asCfg = await db
                    .collection("inboundconfigs")
                    .findOne({ _id: new ObjectId(hist.configId) }, { projection: { _id: 1 } });
                if (asCfg) {
                    inboundConfigId = hist.configId;
                    if (!source) source = "did_history_config";
                }
            } catch {
                /* ignore */
            }
        }
        if (!userId && hist.userId) userId = hist.userId;
    }

    // Campaign billing only when this call is not already tied to an inbound bot,
    // and only for a campaign owned by the same account as the call.
    if (!inboundConfigId && !isValidateDoc) {
        const docCampaignId =
            objectIdToString(doc?.campaign_id) ||
            objectIdToString(doc?.campaignId) ||
            objectIdToString(anchor?.campaignId) ||
            null;
        if (docCampaignId) {
            campaignId = docCampaignId;
            source = "inbound_doc";
        }
    }

    if (!inboundConfigId && !campaignId && toPhone) {
        campaignId = await resolveCampaignIdFromInboundDid(toPhone, { userId });
        if (campaignId) source = "campaign_did";
    }

    if ((campaignId || inboundConfigId) && !userId) {
        const { getDb } = require("../db");
        const db = getDb();
        try {
            if (inboundConfigId) {
                const cfg = await db
                    .collection("inboundconfigs")
                    .findOne({ _id: new ObjectId(inboundConfigId) });
                if (cfg?.userId) userId = objectIdToString(cfg.userId);
            }
            if (!userId && campaignId) {
                const campaign = await db.collection("campaigns").findOne({ _id: new ObjectId(campaignId) });
                if (campaign?.userId) {
                    userId = objectIdToString(campaign.userId);
                } else if (campaign?.createdBy) {
                    const user = await db.collection("users").findOne({ email: campaign.createdBy });
                    if (user) userId = objectIdToString(user._id);
                }
            }
        } catch {
            /* ignore invalid ids */
        }
    }

    if (source || campaignId || inboundConfigId) {
        logger.info("[InboundBilling] Resolved billing context", {
            toPhone: toPhone || null,
            campaignId,
            inboundConfigId,
            userId: userId || null,
            source,
            call_sid: anchor?.callSid || null,
            validate_doc: isValidateDoc,
        });
    }

    return { campaignId, inboundConfigId, userId, source };
}

module.exports = {
    resolveCampaignIdFromInboundDid,
    resolveInboundConfigIdFromDid,
    resolveInboundBillingContext,
    phoneVariants,
};
