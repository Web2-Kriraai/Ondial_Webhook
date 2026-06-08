const INBOUNDCALLLOG_COLLECTION = process.env.INBOUNDCALLLOG_COLLECTION || "InboundConversation";

/** Inbound when mapping, provider direction, or legacy call_type says inbound. */
function isInboundWebhook(body, identity, mapping) {
    if (identity?.collectionName === INBOUNDCALLLOG_COLLECTION) return true;
    if (mapping?.collectionName === INBOUNDCALLLOG_COLLECTION) return true;
    if (String(body?.direction || "").toLowerCase() === "inbound") return true;
    if (String(body?.call_type || "").toLowerCase() === "inbound") return true;
    return false;
}

module.exports = {
    INBOUNDCALLLOG_COLLECTION,
    isInboundWebhook,
};
