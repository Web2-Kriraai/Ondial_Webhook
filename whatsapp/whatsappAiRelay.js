const { ObjectId } = require("mongodb");
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

  const headers = { "Content-Type": "application/json", "x-header-key": "1" };
  const secret = String(process.env.WHATSAPP_AI_REPLY_SECRET || "").trim();
  if (secret) headers.Authorization = `Bearer ${secret}`;

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

    const data = await response.json().catch(() => ({}));
    if (response.status < 200 || response.status >= 300) {
      return {
        ok: false,
        error: data?.error || data?.message || `AI HTTP ${response.status}`,
      };
    }
    const normalized = normalizeWhatsappAiReplyResponse(data, payload);
    if (!normalized.ok) return normalized;
    return normalized;
  } catch (error) {
    return {
      ok: false,
      error:
        error.name === "AbortError"
          ? "AI request timed out"
          : error.message || "AI request failed",
    };
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
    console.warn("[WhatsApp] AI next message failed", {
      phone: phoneNorm,
      error: aiResult.error,
      analysisId: analysis?._id ? String(analysis._id) : null,
    });
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
