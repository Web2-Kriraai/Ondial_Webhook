const { ObjectId } = require("mongodb");
const { getDb } = require("../db");
const logger = require("../logger");

function isMongoObjectIdString(s) {
    return typeof s === "string" && /^[a-fA-F0-9]{24}$/.test(s);
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

/** contactprocessings uses `campaignId` (mongoose); legacy rows may use campaign_id / config_id. */
async function resolveCampaignIdFromContact(contactId) {
    if (!isMongoObjectIdString(String(contactId || ""))) return null;
    try {
        const db = getDb();
        const doc = await db.collection("contactprocessings").findOne(
            { _id: new ObjectId(String(contactId)) },
            { projection: { campaignId: 1, campaign_id: 1, config_id: 1 } }
        );
        return (
            objectIdToString(doc?.campaignId) ||
            objectIdToString(doc?.campaign_id) ||
            objectIdToString(doc?.config_id) ||
            null
        );
    } catch (err) {
        logger.warn("[Webhook] resolveCampaignIdFromContact failed", {
            contactId: String(contactId),
            error: err.message,
        });
        return null;
    }
}

module.exports = { resolveCampaignIdFromContact, objectIdToString, isMongoObjectIdString };
