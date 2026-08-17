const { ObjectId } = require("mongodb");
const {
  appendSessionHistory,
  getOrCreateSession,
  historyForAi,
  buildSessionKey,
  findLatestSessionByPhone,
} = require("./whatsappAiSessions");
const { sendWhatsappSessionReply } = require("./sendSessionReply");

const AI_TIMEOUT_MS = 30_000;
const SCRIPT_EXCERPT_MAX = 4000;

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

function summarizeAnalysis(analysis) {
  if (!analysis) return null;
  const data = analysis.analysis_data || analysis.analysisData || {};
  const nextAction = data.Next_Action || data.next_action || analysis.Next_Action || null;
  return {
    analysisId: analysis._id ? String(analysis._id) : null,
    callId: analysis.call_id || analysis.callId || null,
    category: data.Category || data.category || analysis.classification?.Category || null,
    summary:
      data.Conversation_Summary ||
      data.conversation_summary ||
      data.Summary ||
      data.summary ||
      null,
    nextAction,
    sentiment: data.Sentiment || data.sentiment || null,
    appointment:
      data.Appointment_Date ||
      data.appointment_date ||
      data.Appointment ||
      data.appointment ||
      null,
    rawHints: {
      product: data.Product || data.product || null,
      interest: data.Interest_Level || data.interest_level || null,
    },
  };
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

async function loadCampaignScriptExcerpt(db, campaignId) {
  if (!campaignId) return null;
  const script = await db.collection("campaign_scripts").findOne({
    $or: [{ campaignId }, { campaignId: String(campaignId) }],
  });
  if (!script) return null;
  const text = String(script.generatedScript || "").trim();
  if (!text) return null;
  return text.length > SCRIPT_EXCERPT_MAX
    ? `${text.slice(0, SCRIPT_EXCERPT_MAX)}…`
    : text;
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
  scriptExcerpt,
}) {
  const url =
    String(campaign?.whatsappSettings?.aiReplyUrl || "").trim() ||
    String(process.env.WHATSAPP_AI_REPLY_URL || "").trim();

  if (!url) {
    return { ok: false, error: "WHATSAPP_AI_REPLY_URL is not configured" };
  }

  const analysisSummary = summarizeAnalysis(analysis);
  const payload = {
    phone,
    message,
    sessionId: session?.sessionKey || buildSessionKey(phone, campaign?._id),
    campaignId: campaign?._id ? String(campaign._id) : null,
    contactId: contact?._id ? String(contact._id) : null,
    callId: analysisSummary?.callId || session?.callId || null,
    analysisId:
      analysisSummary?.analysisId ||
      (session?.analysisId ? String(session.analysisId) : null),
    history: historyForAi(session),
    analysis: analysisSummary,
    analysisData: analysis?.analysis_data || analysis?.analysisData || null,
    nextAction: analysisSummary?.nextAction || null,
    conversationSummary: analysisSummary?.summary || null,
    dialScriptExcerpt: scriptExcerpt || null,
  };

  const headers = { "Content-Type": "application/json" };
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
    const reply = String(data.reply ?? data.message ?? data.text ?? data.response ?? "").trim();
    if (!reply) return { ok: false, error: "AI returned empty reply" };
    return { ok: true, reply };
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
 * Handle inbound WhatsApp message: persist history, call AI, send Meta free-text reply.
 */
async function relayInboundWhatsAppMessage(db, {
  phone,
  text,
  messageId = "",
  timestamp = new Date(),
}) {
  const phoneNorm = String(phone || "").replace(/[\s+\-()]/g, "");
  if (!phoneNorm || !String(text || "").trim()) {
    return { handled: false, reason: "empty" };
  }

  const variants = phoneVariants(phoneNorm);
  const contact = await db.collection("contactprocessings").findOne(
    { $or: variants.map((mobileNumber) => ({ mobileNumber })) },
    { sort: { updatedAt: -1 } }
  );

  let existingSession = await findLatestSessionByPhone(db, phoneNorm);
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

  let campaign = null;
  const campaignId = contact?.campaignId || existingSession?.campaignId || null;
  if (campaignId) {
    campaign = await db.collection("campaigns").findOne({
      _id: toObjectIdOrNull(campaignId) || campaignId,
    });
  }

  if (!isAiRelayEnabled(campaign)) {
    return { handled: false, reason: "ai_relay_disabled" };
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

  await appendSessionHistory(db, session.sessionKey, {
    role: "user",
    text,
    messageId,
    timestamp,
  });

  const fresh = await db.collection("whatsapp_ai_sessions").findOne({
    sessionKey: session.sessionKey,
  });

  if (contact?._id) {
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
            type: "text",
            text,
            timestamp,
          },
        },
      }
    );
  }

  if (!isConversationWindowOpen(timestamp)) {
    const err = "conversation_window_closed";
    console.warn("[WhatsApp] inbound after 24h window — template re-engagement not on webhook", {
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
  const scriptExcerpt = await loadCampaignScriptExcerpt(db, campaign?._id);

  const aiResult = await fetchAiReply({
    phone: phoneNorm,
    message: text,
    session: fresh || session,
    campaign,
    contact,
    analysis,
    scriptExcerpt,
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

  console.log("[WhatsApp] AI next message send", {
    phone: phoneNorm,
    campaignId: campaign?._id ? String(campaign._id) : null,
    analysisId: analysis?._id ? String(analysis._id) : null,
    replyLen: aiResult.reply.length,
  });

  const sendResult = await sendWhatsappSessionReply(db, {
    user,
    profile,
    phone: phoneNorm,
    text: aiResult.reply,
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
    text: aiResult.reply,
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
            text: aiResult.reply,
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
    reply: aiResult.reply,
    messageId: sendResult.messageId,
    via: sendResult.via,
    kind: "ai_next_message",
  };
}

module.exports = {
  relayInboundWhatsAppMessage,
};
