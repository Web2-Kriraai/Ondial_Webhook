const { ObjectId } = require("mongodb");
const logger = require("../logger");
const {
  appendSessionHistory,
  getOrCreateSession,
  buildSessionKey,
  findLatestSessionByPhone,
} = require("./whatsappAiSessions");
const { sendWhatsappSessionReply } = require("./sendSessionReply");
const { buildWhatsappAiReplyPayload } = require("./buildWhatsappAiReplyPayload");
const {
  canSendWhatsappReply,
  normalizeWhatsappAiReplyResponse,
} = require("./whatsappAiReplyResponse");
const { applyWhatsappCallbackUpdate } = require("./applyWhatsappCallbackUpdate");

const AI_TIMEOUT_MS = 30_000;

function phoneVariants(phone) {
  const normalized = String(phone || "").replace(/[\s+\-()]/g, "");
  const set = new Set([normalized, `+${normalized}`]);
  if (normalized.startsWith("91") && normalized.length === 12) {
    set.add(normalized.slice(2));
  }
  return [...set];
}

function findWhatsappProfile(user, profileId) {
  const profiles = user?.omniChannelSettings?.whatsappProfiles || [];
  const id = String(profileId || "").trim();
  if (id) {
    const match = profiles.find((p) => String(p._id) === id);
    if (match) return match;
  }
  return (
    profiles.find((p) => p.isDefault && p.verified) ||
    profiles.find((p) => p.usesPlatformAccount) ||
    profiles.find((p) => p.verified) ||
    profiles[0] ||
    null
  );
}

function toObjectIdOrNull(value) {
  if (!value) return null;
  if (value instanceof ObjectId) return value;
  if (ObjectId.isValid(String(value))) return new ObjectId(String(value));
  return null;
}

function alreadyStoredInbound(session, contact, messageId) {
  const id = String(messageId || "").trim();
  if (!id) return false;
  if ((session?.history || []).some((row) => row.role === "user" && String(row.messageId || "") === id)) {
    return true;
  }
  if ((contact?.whatsappHistory || []).some((row) => String(row.messageId || "") === id)) {
    return true;
  }
  return false;
}

function inboundAlreadyReplied(session, messageId) {
  const id = String(messageId || "").trim();
  if (!id) return false;
  const history = session?.history || [];
  const idx = history.findIndex((row) => row.role === "user" && String(row.messageId || "") === id);
  if (idx === -1) return false;
  return history.slice(idx + 1).some((row) => row.role === "assistant");
}

async function findContactsForPhone(db, phoneNorm) {
  const variants = phoneVariants(phoneNorm);
  return db
    .collection("contactprocessings")
    .find({ $or: variants.map((mobileNumber) => ({ mobileNumber })) })
    .sort({ updatedAt: -1 })
    .limit(10)
    .toArray();
}

function pickFollowupContact(contacts, session) {
  if (!contacts.length) return null;
  const active = contacts.find((contact) => isFollowupSessionActive(contact, session));
  return active || contacts[0];
}

async function loadCallAnalysis(db, { session, contact, campaignId }) {
  if (session?.analysisId) {
    const oid = toObjectIdOrNull(session.analysisId);
    if (oid) {
      const byId = await db.collection("call_analysis").findOne({ _id: oid });
      if (byId) return byId;
    }
  }

  if (session?.callId) {
    const byCall = await db.collection("call_analysis").findOne(
      { $or: [{ call_id: String(session.callId) }, { callId: String(session.callId) }] },
      { sort: { created_at: -1, createdAt: -1 } }
    );
    if (byCall) return byCall;
  }

  if (contact?.whatsappPendingAnalysisId) {
    const oid = toObjectIdOrNull(contact.whatsappPendingAnalysisId);
    if (oid) {
      const byPending = await db.collection("call_analysis").findOne({ _id: oid });
      if (byPending) return byPending;
    }
  }

  if (contact?._id) {
    const byContact = await db.collection("call_analysis").findOne(
      {
        $or: [{ contact_id: String(contact._id) }, { contact_id: contact._id }],
      },
      { sort: { created_at: -1, createdAt: -1 } }
    );
    if (byContact) return byContact;
  }

  if (campaignId && contact?.mobileNumber) {
    return db.collection("call_analysis").findOne(
      {
        campaign_id: campaignId,
        $or: [{ phone: contact.mobileNumber }, { mobileNumber: contact.mobileNumber }],
      },
      { sort: { created_at: -1, createdAt: -1 } }
    );
  }

  return null;
}

async function loadCallLog(db, { session, analysis, contact }) {
  const logs = await loadCallLogsForContact(db, { session, analysis, contact });
  return logs.length ? logs[logs.length - 1] : null;
}

async function loadCallLogsForContact(db, { session, analysis, contact }) {
  const collections = [
    process.env.CALLLOGS_COLLECTION || "CallLogs",
    process.env.TESTCALL_COLLECTION || "TestCall",
  ];
  const contactId = contact?._id || session?.contactId || null;

  if (contactId) {
    const oid = toObjectIdOrNull(contactId);
    const contactOr = [{ contact_id: String(contactId) }];
    if (oid) contactOr.push({ contact_id: oid });

    const docs = await db
      .collection(collections[0])
      .find({ $or: contactOr })
      .sort({ createdAt: 1 })
      .limit(20)
      .toArray();
    if (docs.length) return docs;
  }

  const one = await loadCallLogByCallId(db, { session, analysis, collections });
  return one ? [one] : [];
}

async function loadCallLogByCallId(db, { session, analysis, collections }) {
  const callId = String(analysis?.call_id || analysis?.callId || session?.callId || "").trim();
  if (!callId) return null;
  const or = [{ call_id: callId }, { callId: callId }, { call_unique_id: callId }];
  for (const name of collections) {
    const doc = await db
      .collection(name)
      .find({ $or: or })
      .sort({ createdAt: -1 })
      .limit(1)
      .next();
    if (doc) return doc;
  }
  return null;
}

function isFollowupSessionActive(contact, session) {
  if (session?.closedAt) return false;
  const status = String(contact?.whatsappQueueStatus || "").toUpperCase();
  if (status === "WAITING_REPLY" || status === "READY") return true;
  if (contact?.whatsappStatus === "sent" && session?.sessionKey) return true;
  if (session?.callId || session?.analysisId) return true;
  if ((session?.history || []).length > 0) return true;
  return false;
}

function formatFastApiDetail(detail) {
  if (detail == null) return "";
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    return detail
      .map((row) => {
        if (typeof row === "string") return row;
        if (!row || typeof row !== "object") return String(row);
        const loc = Array.isArray(row.loc) ? row.loc.filter((p) => p !== "body").join(".") : "";
        const msg = row.msg || row.message || "";
        return [loc, msg].filter(Boolean).join(": ");
      })
      .filter(Boolean)
      .join("; ");
  }
  if (typeof detail === "object") {
    try {
      return JSON.stringify(detail);
    } catch {
      return String(detail);
    }
  }
  return String(detail);
}

function formatAiHttpError(status, data, rawText) {
  const fromErrors = Array.isArray(data?.errors)
    ? data.errors
        .map((row) => {
          if (!row || typeof row !== "object") return String(row || "");
          return [row.field, row.message].filter(Boolean).join(": ");
        })
        .filter(Boolean)
        .join("; ")
    : "";
  const fromDetail = formatFastApiDetail(data?.detail);
  const fromFields = data?.error || data?.message || data?.msg || "";
  const text = String(fromErrors || fromDetail || fromFields || rawText || "").replace(/\s+/g, " ").trim();
  return text ? `AI HTTP ${status}: ${text.slice(0, 1200)}` : `AI HTTP ${status}`;
}

function summarizeAiRequestPayload(wrapped) {
  const p = wrapped?.payload && typeof wrapped.payload === "object" ? wrapped.payload : wrapped || {};
  return {
    session_id: p.session_id || null,
    campaign_id: p.campaign_id || null,
    contact_id: p.contact_id || null,
    call_id: p.call_id || null,
    service_id: p.service_id || null,
    wizard_service_id: p.wizard_service_id || null,
    sub_service_id: p.sub_service_id || null,
    timezone: p.timezone || null,
    language: p.language || null,
    current_time: p.current_time || null,
    inbound_message: String(p.inbound_message || "").slice(0, 160),
    call_conversation_count: Array.isArray(p.call_conversation) ? p.call_conversation.length : 0,
    whatsapp_history_count: Array.isArray(p.whatsapp_history) ? p.whatsapp_history.length : 0,
    company: p.company?.name || null,
    agent: p.agent?.name || null,
  };
}

async function fetchAiReply({
  phone,
  message,
  session,
  campaign,
  contact,
  analysis,
  callLogs,
}) {
  const url =
    String(campaign?.whatsappSettings?.aiReplyUrl || "").trim() ||
    String(process.env.WHATSAPP_AI_REPLY_URL || "").trim() ||
    `${String(process.env.ONDIAL_APP_URL || "").trim().replace(/\/$/, "")}/api/whatsapp/ai-reply`;

  if (!url || url === "/api/whatsapp/ai-reply") {
    return { ok: false, error: "WHATSAPP_AI_REPLY_URL is not configured" };
  }

  const payload = buildWhatsappAiReplyPayload({
    phone,
    message,
    session,
    campaign,
    contact,
    analysis,
    callLogs,
  });
  const payloadSummary = summarizeAiRequestPayload(payload);

  const headers = { "Content-Type": "application/json", "x-header-key": "1" };
  const secret = String(process.env.WHATSAPP_AI_REPLY_SECRET || "").trim();
  if (secret) headers.Authorization = `Bearer ${secret}`;

  const started = Date.now();
  const fullLog = { maxChars: 250000 };
  logger.info("[WhatsApp] AI request body", {
    url,
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-header-key": "1",
      Authorization: secret ? "Bearer ***" : null,
    },
    body: payload,
  }, fullLog);

  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), AI_TIMEOUT_MS);
    let response;
    try {
      response = await fetch(url, {
        method: "POST",
        headers,
        body: JSON.stringify(payload),
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timer);
    }

    const rawText = await response.text();
    let data = {};
    try {
      data = rawText ? JSON.parse(rawText) : {};
    } catch {
      data = {};
    }
    const durationMs = Date.now() - started;
    const responseBody = Object.keys(data).length ? data : rawText;

    logger.info("[WhatsApp] AI response body", {
      url,
      phone,
      status: response.status,
      ok: response.ok,
      durationMs,
      body: responseBody,
    }, fullLog);

    if (response.status < 200 || response.status >= 300) {
      const error = formatAiHttpError(response.status, data, rawText);
      logger.error("[WhatsApp] AI next message failed", {
        url,
        phone,
        status: response.status,
        durationMs,
        error,
        detail: data?.detail ?? null,
        analysisId: analysis?._id ? String(analysis._id) : null,
      });
      return { ok: false, error, status: response.status, detail: data?.detail ?? null };
    }

    const normalized = normalizeWhatsappAiReplyResponse(data, payload);
    if (!normalized.ok) {
      logger.warn("[WhatsApp] AI next message invalid response", {
        url,
        phone,
        status: response.status,
        durationMs,
        error: normalized.error,
      });
      return normalized;
    }

    logger.info("[WhatsApp] AI next message ok", {
      url,
      phone,
      status: response.status,
      durationMs,
      should_reply: normalized.should_reply !== false,
      reply_text: normalized.reply_text || normalized.reply || "",
      callback_update: normalized.callback_update || null,
    });
    return normalized;
  } catch (error) {
    const durationMs = Date.now() - started;
    const messageText =
      error.name === "AbortError"
        ? "AI request timed out"
        : error.message || "AI request failed";
    logger.error("[WhatsApp] AI next message failed", {
      url,
      phone,
      durationMs,
      error: messageText,
      payload: payloadSummary,
      analysisId: analysis?._id ? String(analysis._id) : null,
    });
    return { ok: false, error: messageText };
  }
}

function isAiRelayEnabled(campaign) {
  if (String(process.env.WHATSAPP_AI_RELAY_ENABLED || "").toLowerCase() === "true") {
    return true;
  }
  return Boolean(campaign?.whatsappSettings?.aiMessageEnabled);
}

function isConversationWindowOpen(replyTimestamp) {
  const sentAt = replyTimestamp instanceof Date ? replyTimestamp : new Date(replyTimestamp);
  if (Number.isNaN(sentAt.getTime())) return false;
  return Date.now() < sentAt.getTime() + 24 * 60 * 60 * 1000;
}

/**
 * Handle inbound WhatsApp message on Ondial_Webhook:
 * persist history, call AI with slim conversation payload, send session reply.
 */
async function relayInboundWhatsAppMessage(db, {
  phone,
  text,
  messageId = "",
  timestamp = new Date(),
  messageType = "text",
}) {
  const phoneNorm = String(phone || "").replace(/[\s+\-()]/g, "");
  if (!phoneNorm || !String(text || "").trim()) {
    return { handled: false, reason: "empty" };
  }

  const contacts = await findContactsForPhone(db, phoneNorm);
  let existingSession = await findLatestSessionByPhone(db, phoneNorm);
  const contact = pickFollowupContact(contacts, existingSession);

  if (!existingSession && contact?.campaignId) {
    existingSession = await db.collection("whatsapp_ai_sessions").findOne({
      sessionKey: buildSessionKey(phoneNorm, contact.campaignId),
    });
  }

  if (!isFollowupSessionActive(contact, existingSession)) {
    console.log("[WhatsApp] inbound skipped — no follow-up session", {
      phone: phoneNorm,
      queueStatus: contact?.whatsappQueueStatus || null,
      hasSession: Boolean(existingSession),
    });
    return { handled: false, reason: "no_followup_session" };
  }

  if (inboundAlreadyReplied(existingSession, messageId)) {
    return { handled: true, success: true, reason: "duplicate_message", kind: "duplicate" };
  }

  const alreadyInbound = alreadyStoredInbound(existingSession, contact, messageId);

  let campaign = null;
  const campaignId = contact?.campaignId || existingSession?.campaignId || null;
  if (campaignId) {
    campaign = await db.collection("campaigns").findOne({
      _id: toObjectIdOrNull(campaignId) || campaignId,
    });
  }

  let user = null;
  if (campaign?.createdBy) {
    const createdBy = String(campaign.createdBy);
    user = await db.collection("users").findOne({
      $or: [
        { email: createdBy },
        ...(ObjectId.isValid(createdBy) ? [{ _id: new ObjectId(createdBy) }] : []),
      ],
    });
  }

  const profile = findWhatsappProfile(user, campaign?.followupWhatsappProfileId);
  const session = await getOrCreateSession(db, {
    phone: phoneNorm,
    campaignId: campaign?._id || existingSession?.campaignId,
    contactId: contact?._id || existingSession?.contactId,
    userId: user?._id || existingSession?.userId,
    userName: "",
    callId: existingSession?.callId || null,
    analysisId: existingSession?.analysisId || null,
  });

  if (!alreadyInbound) {
    await appendSessionHistory(db, session.sessionKey, {
      role: "user",
      text,
      messageId,
      timestamp,
    });
  }

  const fresh = await db.collection("whatsapp_ai_sessions").findOne({
    sessionKey: session.sessionKey,
  });

  if (contact?._id && !alreadyInbound) {
    await db.collection("contactprocessings").updateOne(
      { _id: contact._id },
      {
        $set: {
          lastCustomerWhatsappReplyAt: timestamp,
          conversationWindowOpensUntil: new Date(timestamp.getTime() + 24 * 60 * 60 * 1000),
          whatsappQueueStatus: "WAITING_REPLY",
          updatedAt: new Date(),
        },
        $push: {
          whatsappHistory: {
            direction: "inbound",
            messageId,
            type: messageType || "text",
            text,
            timestamp,
            kind: "inbound",
          },
        },
      }
    );
  }

  if (!isAiRelayEnabled(campaign)) {
    return { handled: true, success: true, reason: "ai_relay_disabled", kind: "persisted_only" };
  }

  if (!isConversationWindowOpen(timestamp)) {
    const err = "conversation_window_closed";
    console.warn("[WhatsApp] inbound after 24h window — free-text reply skipped", {
      phone: phoneNorm,
    });
    await db.collection("whatsapp_ai_sessions").updateOne(
      { sessionKey: session.sessionKey },
      { $set: { lastError: err, updatedAt: new Date() } }
    );
    return { handled: true, success: false, error: err, kind: "window_closed" };
  }

  const analysis = await loadCallAnalysis(db, {
    session: fresh || session,
    contact,
    campaignId: campaign?._id,
  });
  const callLogs = await loadCallLogsForContact(db, {
    session: fresh || session,
    analysis,
    contact,
  });

  const aiResult = await fetchAiReply({
    phone: phoneNorm,
    message: text,
    session: fresh || session,
    campaign,
    contact,
    analysis,
    callLogs,
  });

  if (!aiResult.ok) {
    await db.collection("whatsapp_ai_sessions").updateOne(
      { sessionKey: session.sessionKey },
      { $set: { lastError: aiResult.error, updatedAt: new Date() } }
    );
    return { handled: true, success: false, error: aiResult.error };
  }

  const freshContact = contact?._id
    ? (await db.collection("contactprocessings").findOne({ _id: contact._id })) || contact
    : contact;
  let callbackPatch = { applied: false };
  if (aiResult.callback_update) {
    try {
      callbackPatch = await applyWhatsappCallbackUpdate(db, {
        contact: freshContact,
        analysis,
        campaign,
        callbackUpdate: aiResult.callback_update,
      });
    } catch (err) {
      console.warn("[WhatsApp] callback_update apply failed", err?.message || err);
    }
  }

  if (!canSendWhatsappReply(aiResult)) {
    console.log("[WhatsApp] AI next message skipped (should_reply=false)", {
      phone: phoneNorm,
      callbackAction: callbackPatch.action || null,
    });
    return {
      handled: true,
      success: true,
      reply: "",
      kind: "no_reply",
      callbackAction: callbackPatch.action || null,
    };
  }

  console.log("[WhatsApp] AI next message send", {
    phone: phoneNorm,
    campaignId: campaign?._id ? String(campaign._id) : null,
    analysisId: analysis?._id ? String(analysis._id) : null,
    replyLen: (aiResult.reply || aiResult.reply_text || "").length,
    callbackAction: callbackPatch.action || null,
  });

  const replyText = aiResult.reply || aiResult.reply_text || "";

  const sendResult = await sendWhatsappSessionReply(db, {
    user,
    profile,
    phone: phoneNorm,
    text: replyText,
    campaignId: campaign?._id,
    contactId: contact?._id,
    conversationWindowOpensUntil: new Date(timestamp.getTime() + 24 * 60 * 60 * 1000),
  });

  if (!sendResult.success) {
    await db.collection("whatsapp_ai_sessions").updateOne(
      { sessionKey: session.sessionKey },
      { $set: { lastError: sendResult.error, updatedAt: new Date() } }
    );
    return { handled: true, success: false, error: sendResult.error };
  }

  await appendSessionHistory(db, session.sessionKey, {
    role: "assistant",
    text: replyText,
    messageId: sendResult.messageId || "",
    isAiGenerated: true,
  });

  if (contact?._id) {
    await db.collection("contactprocessings").updateOne(
      { _id: contact._id },
      {
        $set: {
          lastWhatsappSentAt: new Date(),
          whatsappMessageId: sendResult.messageId,
          whatsappQueueStatus: "WAITING_REPLY",
          updatedAt: new Date(),
        },
        $push: {
          whatsappHistory: {
            direction: "outbound",
            messageId: sendResult.messageId,
            type: "text",
            text: replyText,
            timestamp: new Date(),
            isAiGenerated: true,
            kind: "ai_next_message",
          },
        },
      }
    );
  }

  return {
    handled: true,
    success: true,
    reply: replyText,
    messageId: sendResult.messageId,
    via: sendResult.via,
    kind: "ai_next_message",
    callbackAction: callbackPatch.action || null,
  };
}

module.exports = {
  relayInboundWhatsAppMessage,
  buildWhatsappAiReplyPayload,
};
