const logger = require("../logger");
const { getDb } = require("../db");
const { processInboundChatPayload } = require("./processInboundChat");

/**
 * AiSensy inbound chat: same STOP + slim AI relay as Meta.
 * Idempotent on messageId so CS1 queue consumers cannot double-reply.
 */
async function processAisensyInboundPayload(payload) {
  const db = getDb();
  return processInboundChatPayload(db, payload, { source: "aisensy" });
}

function processAisensyInboundSafe(payload) {
  processAisensyInboundPayload(payload).catch((err) => {
    logger.error("[AiSensy inbound] AI relay error", { error: err.message || err });
  });
}

module.exports = {
  processAisensyInboundPayload,
  processAisensyInboundSafe,
};
