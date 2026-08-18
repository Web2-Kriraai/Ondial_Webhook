const { parseInboundMessages, isStopMessage } = require("./inboundParser");
const { relayInboundWhatsAppMessage } = require("./whatsappAiRelay");
const logger = require("../logger");

/**
 * Shared inbound chat handling for Meta and AiSensy webhook events.
 * STOP → unsubscribe list; otherwise slim-payload AI relay.
 */
async function processInboundChatPayload(db, payload, { source = "whatsapp" } = {}) {
  const inboundMessages = parseInboundMessages(payload);
  let stops = 0;
  let relays = 0;
  let skipped = 0;

  for (const inbound of inboundMessages) {
    if (!inbound.text) {
      skipped += 1;
      continue;
    }

    if (isStopMessage(inbound.text)) {
      await db.collection("whatsappunsubscribes").updateOne(
        { phone: inbound.phone },
        {
          $set: {
            phone: inbound.phone,
            source: `${source}_stop`,
            updatedAt: new Date(),
          },
          $setOnInsert: { createdAt: new Date() },
        },
        { upsert: true }
      );
      stops += 1;
      continue;
    }

    const result = await relayInboundWhatsAppMessage(db, {
      phone: inbound.phone,
      text: inbound.text,
      messageId: inbound.messageId,
      timestamp: inbound.timestamp,
      messageType: inbound.type || "text",
    });
    if (result.handled) relays += 1;
    else skipped += 1;
    logger.info("[WhatsApp inbound] AI relay", {
      source,
      phone: inbound.phone,
      type: inbound.type || null,
      handled: result.handled,
      success: result.success,
      reason: result.reason,
      error: result.error,
      kind: result.kind,
    });
  }

  return { processed: inboundMessages.length, stops, relays, skipped };
}

module.exports = {
  processInboundChatPayload,
};
