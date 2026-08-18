const WHATSAPP_REPLY_MAX = 900;

function asText(value, max = 0) {
  const text = String(value ?? "").trim();
  if (!text) return "";
  if (max > 0 && text.length > max) return `${text.slice(0, max)}…`;
  return text;
}

function asBool(value, fallback = null) {
  if (value === true || value === "true" || value === 1 || value === "1") return true;
  if (value === false || value === "false" || value === 0 || value === "0") return false;
  return fallback;
}

function requestPayloadOf(request = {}) {
  if (request?.payload && typeof request.payload === "object") return request.payload;
  return request;
}

function normalizeCallbackUpdate(raw) {
  if (raw == null) return null;
  if (typeof raw !== "object" || Array.isArray(raw)) return null;
  const status = asBool(raw.status ?? raw.Status, null);
  if (status == null) return null;
  const scheduled =
    raw.scheduled_at ??
    raw.scheduledAt ??
    raw.Scheduled_At ??
    raw.window ??
    null;
  const acknowledged = asBool(raw.acknowledged ?? raw.Acknowledged, status);
  return {
    status,
    scheduled_at: scheduled == null || scheduled === "" ? null : String(scheduled).trim(),
    acknowledged,
  };
}

function serviceIdFrom(wizardServiceId, subServiceId) {
  const wizard = String(wizardServiceId || "").trim();
  const sub = String(subServiceId || "").trim();
  if (wizard && sub) return `${wizard}.${sub}`;
  return wizard || sub || "";
}

function normalizeWhatsappAiReplyResponse(data = {}, request = {}) {
  const req = requestPayloadOf(request);
  const body =
    data?.payload &&
    typeof data.payload === "object" &&
    (data.payload.reply_text || data.payload.should_reply != null)
      ? data.payload
      : data;

  const reply = asText(
    body.reply_text ?? body.reply ?? body.message ?? body.text ?? body.response ?? "",
    WHATSAPP_REPLY_MAX
  );
  const explicitShould = asBool(body.should_reply ?? body.shouldReply, null);
  const shouldReply = explicitShould == null ? Boolean(reply) : explicitShould;
  const wizard = asText(body.wizard_service_id || req.wizard_service_id);
  const sub = asText(body.sub_service_id || req.sub_service_id);
  const callbackUpdate = normalizeCallbackUpdate(
    body.callback_update || body.callback_request || body.callback_requested || null
  );

  const hasContent = Boolean(reply) || callbackUpdate != null || explicitShould === false;
  if (!hasContent) {
    return { ok: false, error: "AI returned empty reply" };
  }

  return {
    ok: true,
    should_reply: shouldReply,
    reply_text: reply,
    reply: shouldReply ? reply : "",
    language: asText(body.language || req.language) || "en",
    service_id: asText(body.service_id) || serviceIdFrom(wizard, sub),
    wizard_service_id: wizard,
    sub_service_id: sub,
    session_id: asText(body.session_id || req.session_id) || null,
    call_id: asText(body.call_id || req.call_id) || null,
    callback_update: callbackUpdate,
  };
}

function canSendWhatsappReply(result = {}) {
  return result?.ok === true && result.should_reply === true && Boolean(result.reply_text || result.reply);
}

module.exports = {
  normalizeCallbackUpdate,
  normalizeWhatsappAiReplyResponse,
  canSendWhatsappReply,
  serviceIdFrom,
};
