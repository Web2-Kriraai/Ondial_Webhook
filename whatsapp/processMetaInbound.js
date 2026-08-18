const { getDb } = require("../db");
const { processInboundChatPayload } = require("./processInboundChat");
const { applyMetaStatusUpdates } = require("./metaStatusUpdates");
const { applyMetaTemplateStatusUpdates } = require("./metaTemplateStatusUpdates");

/**
 * Process Meta inbound chat messages (STOP + AI relay).
 */
async function processMetaInboundChat(payload) {
  const db = getDb();
  return processInboundChatPayload(db, payload, { source: "whatsapp" });
}

/**
 * Full Meta webhook job: inbound chat + delivery statuses + template status.
 */
async function processMetaInboundPayload(payload) {
  const db = getDb();
  const [inbound, statuses, templateStatuses] = await Promise.all([
    processMetaInboundChat(payload),
    applyMetaStatusUpdates(db, payload),
    applyMetaTemplateStatusUpdates(db, payload),
  ]);
  return { inbound, statuses, templateStatuses };
}

module.exports = {
  processMetaInboundChat,
  processMetaInboundPayload,
};
