function normalizeWebhookPhone(raw) {
  if (!raw) return "";
  return String(raw).replace(/[\s+\-()]/g, "");
}

const INBOUND_TYPES = new Set([
  "replied",
  "reply",
  "incoming",
  "inbound",
  "message",
  "message_received",
  "user_message",
  "text",
  "button",
  "interactive",
  "image",
  "video",
  "audio",
  "voice",
  "document",
  "sticker",
  "location",
  "contacts",
]);

const SKIP_TYPES = new Set(["reaction", "system", "ephemeral", "unsupported"]);

function extractInboundText(event) {
  return String(
    event.text ||
      event.message ||
      event.reply ||
      event.body ||
      event.messageText ||
      event.content ||
      event.data?.text ||
      event.data?.message ||
      ""
  ).trim();
}

function mediaPlaceholder(type) {
  switch (String(type || "").toLowerCase()) {
    case "image":
      return "[Customer sent an image]";
    case "video":
      return "[Customer sent a video]";
    case "audio":
    case "voice":
      return "[Customer sent a voice message]";
    case "document":
      return "[Customer sent a document]";
    case "sticker":
      return "[Customer sent a sticker]";
    case "location":
      return "[Customer shared a location]";
    case "contacts":
      return "[Customer shared a contact]";
    default:
      return "";
  }
}

function extractTextFromMetaMessage(message) {
  if (message.text?.body) return String(message.text.body).trim();
  if (message.button?.text) return String(message.button.text).trim();
  if (message.interactive?.button_reply?.title) {
    return String(message.interactive.button_reply.title).trim();
  }
  if (message.interactive?.list_reply?.title) {
    return String(message.interactive.list_reply.title).trim();
  }
  if (message.image?.caption) return String(message.image.caption).trim();
  if (message.video?.caption) return String(message.video.caption).trim();
  if (message.document?.caption) return String(message.document.caption).trim();
  return mediaPlaceholder(message.type);
}

function parseTimestamp(raw) {
  if (!raw) return new Date();
  if (raw instanceof Date) return raw;
  const numeric = Number(raw);
  if (Number.isFinite(numeric)) {
    return new Date(numeric < 1e12 ? numeric * 1000 : numeric);
  }
  const parsed = new Date(raw);
  return Number.isNaN(parsed.getTime()) ? new Date() : parsed;
}

function normalizeEvents(payload) {
  if (Array.isArray(payload)) return payload;
  if (payload?.events) return payload.events;
  if (payload?.entry) {
    return payload.entry.flatMap((entry) =>
      (entry.changes || []).flatMap((change) => {
        const value = change.value || {};
        const messages = value.messages || [];
        return messages.map((message) => ({
          ...message,
          _metaValue: value,
          _field: change.field,
        }));
      })
    );
  }
  return [payload];
}

function parseSingleInbound(event) {
  if (!event || typeof event !== "object") return null;

  if (event.from && (event.text?.body || event.type || event.button || event.interactive)) {
    const type = String(event.type || "text").toLowerCase();
    if (SKIP_TYPES.has(type)) return null;
    const text = extractTextFromMetaMessage(event);
    if (!text) return null;
    return {
      phone: normalizeWebhookPhone(event.from),
      text,
      type,
      messageId: String(event.id || event.messageId || ""),
      timestamp: parseTimestamp(event.timestamp),
      raw: event,
    };
  }

  const type = String(event.type || event.event || event.status || "").toLowerCase();
  if (SKIP_TYPES.has(type)) return null;
  if (!INBOUND_TYPES.has(type)) return null;

  const phone = normalizeWebhookPhone(
    event.phone ||
      event.destination ||
      event.from ||
      event.userNumber ||
      event.user_number ||
      event.wa_id
  );
  if (!phone) return null;

  const text = extractInboundText(event) || mediaPlaceholder(type);
  if (!text) return null;

  return {
    phone,
    text,
    type,
    messageId: String(event.messageId || event.message_id || event.id || ""),
    timestamp: parseTimestamp(event.timestamp || event.createdAt),
    campaignName: event.campaignName || event.campaign_name || event.campaign || "",
    raw: event,
  };
}

function parseInboundMessages(payload) {
  const events = normalizeEvents(payload);
  const inbound = [];
  for (const event of events) {
    const parsed = parseSingleInbound(event);
    if (parsed) inbound.push(parsed);
  }
  return inbound;
}

function isStopMessage(text) {
  const t = String(text || "").trim().toUpperCase();
  return t === "STOP" || t === "UNSUBSCRIBE" || t === "CANCEL" || t === "OPT OUT";
}

module.exports = {
  parseInboundMessages,
  isStopMessage,
};
