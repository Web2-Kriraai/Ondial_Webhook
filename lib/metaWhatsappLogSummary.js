/**
 * Safe summary of Meta Cloud API WhatsApp webhook payloads for ops logs.
 * Avoids dumping full bodies / secrets.
 */

function previewText(value, max = 80) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (!text) return "";
  return text.length > max ? `${text.slice(0, max)}…` : text;
}

function summarizeMetaWhatsappPayload(payload) {
  const entry = Array.isArray(payload?.entry) ? payload.entry : [];
  const fields = [];
  const messages = [];
  const statuses = [];
  const templateEvents = [];
  let phoneNumberId = null;
  let displayPhoneNumber = null;
  let wabaId = null;

  for (const item of entry) {
    if (!wabaId && item?.id) wabaId = String(item.id);
    for (const change of item?.changes || []) {
      const field = String(change?.field || "").trim();
      if (field) fields.push(field);
      const value = change?.value || {};
      const meta = value.metadata || {};
      if (!phoneNumberId && meta.phone_number_id) {
        phoneNumberId = String(meta.phone_number_id);
      }
      if (!displayPhoneNumber && meta.display_phone_number) {
        displayPhoneNumber = String(meta.display_phone_number);
      }

      if (field === "messages") {
        for (const msg of value.messages || []) {
          messages.push({
            id: msg.id || null,
            from: msg.from || null,
            type: msg.type || null,
            textPreview:
              msg.type === "text"
                ? previewText(msg.text?.body, 80)
                : undefined,
          });
        }
        for (const st of value.statuses || []) {
          const err0 = Array.isArray(st.errors) && st.errors[0] ? st.errors[0] : null;
          statuses.push({
            id: st.id || null,
            status: st.status || null,
            recipient: st.recipient_id || null,
            // Include Meta failure reason so ops can diagnose undelivered messages
            ...(st.status === "failed" && err0
              ? {
                  errorCode: err0.code ?? null,
                  errorTitle: err0.title || null,
                  errorMessage: err0.message || err0.error_data?.details || null,
                  errorDetails: err0.error_data?.details || null,
                }
              : {}),
          });
        }
      }

      if (field === "message_template_status_update") {
        templateEvents.push({
          event: value.event || null,
          templateId: value.message_template_id
            ? String(value.message_template_id)
            : null,
          templateName: value.message_template_name || null,
          language: value.message_template_language || null,
        });
      }
    }
  }

  return {
    object: payload?.object || null,
    wabaId,
    phoneNumberId,
    displayPhoneNumber,
    fields: [...new Set(fields)],
    messageCount: messages.length,
    statusCount: statuses.length,
    templateEventCount: templateEvents.length,
    messages: messages.slice(0, 5),
    statuses: statuses.slice(0, 5),
    templateEvents: templateEvents.slice(0, 5),
  };
}

function hasFailedDelivery(summary) {
  return (summary?.statuses || []).some((row) => row.status === "failed");
}

function shouldLogMetaWebhook(summary) {
  if ((summary?.messageCount || 0) > 0) return true;
  if ((summary?.templateEventCount || 0) > 0) return true;
  return hasFailedDelivery(summary);
}

function compactInboundLog(summary, extra = {}) {
  const msg = Array.isArray(summary?.messages) ? summary.messages[0] : null;
  return {
    from: msg?.from || null,
    type: msg?.type || null,
    text: msg?.textPreview || null,
    phoneNumberId: summary?.phoneNumberId || null,
    ...extra,
    ...((summary?.messageCount || 0) > 1 ? { messageCount: summary.messageCount } : {}),
  };
}

function compactDeliveryFailedLog(summary, extra = {}) {
  const failed = (summary?.statuses || []).filter((row) => row.status === "failed");
  return {
    phoneNumberId: summary?.phoneNumberId || null,
    ...extra,
    failed: failed.slice(0, 3).map((row) => ({
      recipient: row.recipient || null,
      errorCode: row.errorCode ?? null,
      error: row.errorTitle || row.errorMessage || null,
    })),
  };
}

function compactTemplateLog(summary, extra = {}) {
  return {
    phoneNumberId: summary?.phoneNumberId || null,
    ...extra,
    templates: (summary.templateEvents || []).slice(0, 3),
  };
}

/** One ingress log line per webhook: inbound message, delivery failure, or template update. */
function metaWebhookIngressLog(summary, extra = {}) {
  if ((summary?.messageCount || 0) > 0) {
    return {
      level: "info",
      message: "[MetaWhatsApp] inbound",
      data: compactInboundLog(summary, extra),
    };
  }
  if (hasFailedDelivery(summary)) {
    return {
      level: "warn",
      message: "[MetaWhatsApp] delivery failed",
      data: compactDeliveryFailedLog(summary, extra),
    };
  }
  if ((summary?.templateEventCount || 0) > 0) {
    return {
      level: "info",
      message: "[MetaWhatsApp] template update",
      data: compactTemplateLog(summary, extra),
    };
  }
  return null;
}

/** @deprecated use metaWebhookIngressLog */
function compactMetaWebhookLog(summary, extra = {}) {
  if ((summary?.messageCount || 0) > 0) return compactInboundLog(summary, extra);
  if (hasFailedDelivery(summary)) return compactDeliveryFailedLog(summary, extra);
  return compactTemplateLog(summary, extra);
}

module.exports = {
  summarizeMetaWhatsappPayload,
  shouldLogMetaWebhook,
  compactMetaWebhookLog,
  compactInboundLog,
  compactDeliveryFailedLog,
  metaWebhookIngressLog,
  previewText,
};
