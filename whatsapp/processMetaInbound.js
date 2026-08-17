const { getDb } = require("../db");
const { parseInboundMessages, isStopMessage } = require("./inboundParser");
const { relayInboundWhatsAppMessage } = require("./whatsappAiRelay");
const { applyMetaStatusUpdates } = require("./metaStatusUpdates");
const { applyMetaTemplateStatusUpdates } = require("./metaTemplateStatusUpdates");

/**
 * Process Meta inbound chat messages (STOP + AI relay).
 */
async function processMetaInboundChat(payload) {
  const db = getDb();
  const inboundMessages = parseInboundMessages(payload);
  let stops = 0;
  let relays = 0;

  for (const inbound of inboundMessages) {
    if (!inbound.text && inbound.raw?.type !== "replied") continue;

    if (isStopMessage(inbound.text)) {
      await db.collection("whatsappunsubscribes").updateOne(
        { phone: inbound.phone },
        {
          $set: { phone: inbound.phone, source: "whatsapp_stop", updatedAt: new Date() },
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
    });
    if (result.handled) relays += 1;
    console.log("[WhatsApp inbound] AI relay:", {
      phone: inbound.phone,
      handled: result.handled,
      success: result.success,
      reason: result.reason,
      error: result.error,
      kind: result.kind,
    });
  }

  return { processed: inboundMessages.length, stops, relays };
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
