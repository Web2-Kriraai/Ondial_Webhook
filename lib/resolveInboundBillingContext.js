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

    const deadStatuses = ["draft", "archived", "expired", "deleted", "completed", "paused", "cancelled"];
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

/** Inbound bot config that currently owns the DID — active only (DID reuse safe). */
async function resolveInboundConfigIdFromDid(toPhone) {
    const variants = phoneVariants(toPhone);
    if (!variants.length) return { configId: null, userId: null };

    const { getDb } = require("../db");
    const db = getDb();

    // Prefer active agent. Never inherit from inactive leftover after number release.
    const cfg =
        (await db.collection("inboundconfigs").findOne({
            phoneNumber: { $in: variants },
            status: "active",
        })) ||
        null;

    if (!cfg) return { configId: null, userId: null };

    const configId = objectIdToString(cfg._id);
    let userId = objectIdToString(cfg.userId);

    // Prefer catalog purchasedBy when present — reject mismatch (released/reassigned DID).
    try {
        const catalog = await db.collection("phonenumbers").findOne(
            { number: { $in: variants } },
            { projection: { purchasedBy: 1, status: 1 } }
        );
        if (catalog) {
            if (catalog.status === "available" || !catalog.purchasedBy) {
                logger.info("[InboundBilling] Active inbound config ignored — catalog not owned", {
                    toPhone,
                    configId,
                });
                return { configId: null, userId: null };
            }
            const purchaser = objectIdToString(catalog.purchasedBy);
            if (purchaser && userId && purchaser !== userId) {
                // Org: allow if config user is under same root — soft check via purchaser only for now.
                // Strict: require config.userId === purchasedBy OR config under purchaser's org.
                const configUser = await db.collection("users").findOne(
                    { _id: new ObjectId(userId) },
                    { projection: { createdBy: 1 } }
                );
                const rootOfConfig = objectIdToString(configUser?.createdBy) || userId;
                const purchaserUser = await db.collection("users").findOne(
                    { _id: new ObjectId(purchaser) },
                    { projection: { createdBy: 1 } }
                );
                const rootOfPurchaser =
                    objectIdToString(purchaserUser?.createdBy) || purchaser;
                if (rootOfConfig !== rootOfPurchaser && userId !== purchaser) {
                    logger.info("[InboundBilling] Active inbound config owner mismatch vs catalog", {
                        toPhone,
                        configId,
                        configUserId: userId,
                        purchasedBy: purchaser,
                    });
                    return { configId: null, userId: null };
                }
            }
            if (!userId && purchaser) userId = purchaser;
        }
    } catch (err) {
        logger.warn("[InboundBilling] catalog ownership check failed", {
            toPhone,
            error: err?.message || err,
        });
    }

    return { configId, userId };
}

/**
 * @deprecated DID-history inheritance is unsafe after number reuse.
 * Kept as no-op export for older scripts; always returns empty.
 */
async function resolveConfigFromDidHistory(_toPhone) {
    return { configId: null, userId: null };
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

    // Do NOT inherit userId/config_id from prior to_number history (DID reuse leak).

    // Campaign billing only when this call is not already tied to an inbound bot,
    // and only for a campaign owned by the same account as the call.
    if (!inboundConfigId && !isValidateDoc) {
        const docCampaignId =
            objectIdToString(doc?.campaign_id) ||
            objectIdToString(doc?.campaignId) ||
            objectIdToString(anchor?.campaignId) ||
            null;
        if (docCampaignId) {
            // Validate the campaign status is not dead before accepting it for billing.
            const { getDb } = require("../db");
            const db = getDb();
            try {
                const deadStatuses = ["draft", "archived", "expired", "deleted", "completed", "paused", "cancelled"];
                const camp = await db.collection("campaigns").findOne(
                    { _id: new ObjectId(docCampaignId) },
                    { projection: { status: 1 } }
                );
                if (camp && !deadStatuses.includes(String(camp.status || "").toLowerCase())) {
                    campaignId = docCampaignId;
                    source = "inbound_doc";
                } else if (camp) {
                    logger.info("[InboundBilling] Skipping dead-status campaign from inbound doc", {
                        campaignId: docCampaignId,
                        status: camp.status,
                    });
                } else {
                    campaignId = docCampaignId;
                    source = "inbound_doc";
                }
            } catch {
                campaignId = docCampaignId;
                source = "inbound_doc";
            }
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
    resolveConfigFromDidHistory,
    resolveInboundBillingContext,
    phoneVariants,
};
