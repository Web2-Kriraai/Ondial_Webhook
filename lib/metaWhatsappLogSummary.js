/**
 * Safe summary of Meta Cloud API WhatsApp webhook payloads for ops logs.
 * Avoids dumping full bodies / secrets.
 */

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
                ? String(msg.text?.body || "").slice(0, 80)
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

module.exports = { summarizeMetaWhatsappPayload };
