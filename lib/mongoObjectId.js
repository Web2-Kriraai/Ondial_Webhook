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

function extractConfigIdFromDoc(doc) {
    if (!doc) return null;
    return (
        objectIdToString(doc.config_id) ||
        objectIdToString(doc.configId) ||
        objectIdToString(doc.inboundConfigId) ||
        objectIdToString(doc.campaign_id) ||
        objectIdToString(doc.campaignId)
    );
}

module.exports = {
    isMongoObjectIdString,
    objectIdToString,
    extractConfigIdFromDoc,
};
