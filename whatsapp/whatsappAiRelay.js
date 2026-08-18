const { ObjectId } = require("mongodb");
const logger = require("../logger");
const {
  appendSessionHistory,
  getOrCreateSession,
  findLatestOpenSessionByPhone,
  findSessionByCampaign,
} = require("./whatsappAiSessions");
const { sendWhatsappSessionReply } = require("./sendSessionReply");
const { buildWhatsappAiReplyPayload } = require("./buildWhatsappAiReplyPayload");
const {
  canSendWhatsappReply,
  normalizeWhatsappAiReplyResponse,
} = require("./whatsappAiReplyResponse");
const { applyWhatsappCallbackUpdate } = require("./applyWhatsappCallbackUpdate");
const { previewText } = require("../lib/metaWhatsappLogSummary");

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

function timeMs(value) {
  if (!value) return 0;
  const date = value instanceof Date ? value : new Date(value);
  const ms = date.getTime();
  return Number.isNaN(ms) ? 0 : ms;
}

function isConversationWindowOpenAt(contact, nowMs = Date.now()) {
  const until = timeMs(contact?.conversationWindowOpensUntil);
  if (until > nowMs) return true;
  const sent = Math.max(timeMs(contact?.lastWhatsappSentAt), timeMs(contact?.lastTemplateSentAt));
  return sent > 0 && nowMs - sent < 24 * 60 * 60 * 1000;
}

function isContactFollowupEligible(contact, nowMs = Date.now()) {
  if (!contact) return false;
  const status = String(contact?.whatsappQueueStatus || "").toUpperCase();
  if (status === "WAITING_REPLY" || status === "READY") return true;
  if (contact?.whatsappStatus === "sent" && isConversationWindowOpenAt(contact, nowMs)) return true;
  return isConversationWindowOpenAt(contact, nowMs);
}

function lastWhatsappTouchMs(contact) {
  return Math.max(
    timeMs(contact?.lastWhatsappSentAt),
    timeMs(contact?.lastTemplateSentAt),
    timeMs(contact?.lastCustomerWhatsappReplyAt)
  );
}

async function findContactsForPhone(db, phoneNorm) {
  const variants = phoneVariants(phoneNorm);
  return db
    .collection("contactprocessings")
    .find({ $or: variants.map((mobileNumber) => ({ mobileNumber })) })
    .sort({ lastWhatsappSentAt: -1, updatedAt: -1 })
    .limit(25)
    .toArray();
}

/**
 * Same customer can sit in multiple omni campaigns on one WhatsApp number.
 * Meta is one chat, so inbound attaches to the campaign that last messaged them
 * (open 24h window / WAITING_REPLY), not the first matching contact.
 */
function pickFollowupContact(contacts, session, nowMs = Date.now()) {
  if (!Array.isArray(contacts) || !contacts.length) return null;
  const sessionCampaignId = session?.campaignId ? String(session.campaignId) : "";
  const eligible = contacts.filter((contact) => isContactFollowupEligible(contact, nowMs));
  const pool = eligible.length ? eligible : contacts;
  const scored = [...pool].sort((a, b) => {
    const touchDiff = lastWhatsappTouchMs(b) - lastWhatsappTouchMs(a);
    if (touchDiff) return touchDiff;
    if (sessionCampaignId) {
      const aMatch = String(a.campaignId || "") === sessionCampaignId ? 1 : 0;
      const bMatch = String(b.campaignId || "") === sessionCampaignId ? 1 : 0;
      if (aMatch !== bMatch) return bMatch - aMatch;
    }
    return timeMs(b.updatedAt) - timeMs(a.updatedAt);
  });
  return scored[0] || null;
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

function isFollowupSessionActive(contact, session, nowMs = Date.now()) {
  if (session?.closedAt) return false;
  if (isContactFollowupEligible(contact, nowMs)) return true;
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
    campaign_id: p.campaign_id || null,
    contact_id: p.contact_id || null,
    call_id: p.call_id || null,
    language: p.language || null,
    inbound: previewText(p.inbound_message, 120),
    calls: Array.isArray(p.call_conversation) ? p.call_conversation.length : 0,
    history: Array.isArray(p.whatsapp_history) ? p.whatsapp_history.length : 0,
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

    if (response.status < 200 || response.status >= 300) {
      const error = formatAiHttpError(response.status, data, rawText);
      logger.error("[WhatsApp] AI reply failed", {
        phone,
        status: response.status,
        durationMs,
        error,
        ...payloadSummary,
      });
      return { ok: false, error, status: response.status, durationMs };
    }

    const normalized = normalizeWhatsappAiReplyResponse(data, payload);
    if (!normalized.ok) {
      logger.warn("[WhatsApp] AI reply invalid", {
        phone,
        status: response.status,
        durationMs,
        error: normalized.error,
        ...payloadSummary,
      });
      return { ...normalized, durationMs };
    }

    return { ...normalized, durationMs };
  } catch (error) {
    const durationMs = Date.now() - started;
    const messageText =
      error.name === "AbortError"
        ? "AI request timed out"
        : error.message || "AI request failed";
    logger.error("[WhatsApp] AI reply failed", {
      phone,
      durationMs,
      error: messageText,
      ...payloadSummary,
    });
    return { ok: false, error: messageText, durationMs };
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
  phoneNumberId = "",
}) {
  const phoneNorm = String(phone || "").replace(/[\s+\-()]/g, "");
  if (!phoneNorm || !String(text || "").trim()) {
    return { handled: false, reason: "empty" };
  }

  const contacts = await findContactsForPhone(db, phoneNorm);
  const latestOpenSession = await findLatestOpenSessionByPhone(db, phoneNorm);
  const contact = pickFollowupContact(contacts, latestOpenSession);
  let existingSession = null;
  if (contact?.campaignId) {
    existingSession = await findSessionByCampaign(db, phoneNorm, contact.campaignId);
  }
  if (
    !existingSession &&
    latestOpenSession &&
    String(latestOpenSession.campaignId || "") === String(contact?.campaignId || "")
  ) {
    existingSession = latestOpenSession;
  }

  if (!isFollowupSessionActive(contact, existingSession)) {
    logger.info("[WhatsApp] inbound skipped", {
      phone: phoneNorm,
      reason: "no_followup_session",
      campaignId: contact?.campaignId ? String(contact.campaignId) : null,
    });
    return { handled: false, reason: "no_followup_session" };
  }

  const otherCampaignIds = contacts
    .map((row) => String(row.campaignId || ""))
    .filter((id) => id && id !== String(contact?.campaignId || ""));
  logger.info("[WhatsApp] inbound", {
    phone: phoneNorm,
    campaignId: contact?.campaignId ? String(contact.campaignId) : null,
    text: previewText(text, 120),
    type: messageType || "text",
    ...(phoneNumberId ? { phoneNumberId } : {}),
    ...(otherCampaignIds.length ? { otherCampaigns: otherCampaignIds } : {}),
  });

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
    logger.warn("[WhatsApp] inbound skipped", {
      phone: phoneNorm,
      reason: "conversation_window_closed",
      campaignId: campaign?._id ? String(campaign._id) : null,
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
      logger.warn("[WhatsApp] callback update failed", {
        phone: phoneNorm,
        error: err?.message || String(err),
      });
    }
  }

  if (!canSendWhatsappReply(aiResult)) {
    logger.info("[WhatsApp] no reply", {
      phone: phoneNorm,
      campaignId: campaign?._id ? String(campaign._id) : null,
      inbound: previewText(text, 120),
      durationMs: aiResult.durationMs || null,
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
    logger.error("[WhatsApp] send failed", {
      phone: phoneNorm,
      campaignId: campaign?._id ? String(campaign._id) : null,
      error: sendResult.error,
      reply: previewText(replyText, 120),
    });
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

  logger.info("[WhatsApp] reply sent", {
    phone: phoneNorm,
    campaignId: campaign?._id ? String(campaign._id) : null,
    inbound: previewText(text, 120),
    reply: previewText(replyText, 120),
    durationMs: aiResult.durationMs || null,
    via: sendResult.via || null,
    callbackAction: callbackPatch.action || null,
  });

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
  pickFollowupContact,
  isFollowupSessionActive,
  isContactFollowupEligible,
};
