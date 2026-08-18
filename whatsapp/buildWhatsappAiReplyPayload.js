/**
 * Canonical WhatsApp session-reply AI payload built on Ondial_Webhook.
 * All production calls for the contact (oldest → newest) + WhatsApp history.
 */

const MAX_CALLS_FOR_AI = 20;

function toIso(value) {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString();
}

function conversationTurn(row) {
  if (!row || typeof row !== "object") return null;
  const rawRole = String(row.role || row.speaker || row.direction || "").toLowerCase();
  const userText = row.User ?? row.user ?? row.Customer ?? row.customer ?? row.Caller ?? row.caller;
  const aiText = row.AI ?? row.ai ?? row.Agent ?? row.agent ?? row.Assistant ?? row.assistant;
  const text = String(row.text || row.message || row.content || userText || aiText || "").trim();
  if (!text) return null;

  let role = "user";
  if (
    rawRole === "assistant" ||
    rawRole === "bot" ||
    rawRole === "agent" ||
    rawRole === "ai" ||
    rawRole === "outbound" ||
    rawRole === "out" ||
    (aiText != null && String(aiText).trim() && userText == null)
  ) {
    role = "assistant";
  } else if (!rawRole && aiText != null && String(aiText).trim()) {
    role = "assistant";
  }

  const timestamp = toIso(row.timestamp || row.createdAt || row.time || row.at);
  return timestamp ? { role, text, timestamp } : { role, text };
}

function turnKey(row) {
  return `${row.role}|${row.text}`;
}

function historyForAi(session, limit = 100) {
  return (session?.history || []).slice(-limit);
}

function conversationForAi(session, contact) {
  const fromSession = historyForAi(session, 100).map(conversationTurn).filter(Boolean);
  const fromContact = (contact?.whatsappHistory || []).map(conversationTurn).filter(Boolean);
  if (!fromContact.length) return fromSession;
  const seen = new Set(fromSession.map(turnKey));
  const extra = fromContact.filter((row) => !seen.has(turnKey(row)));
  const merged = [...fromSession, ...extra];
  merged.sort((a, b) => String(a.timestamp || "").localeCompare(String(b.timestamp || "")));
  return merged;
}

function rawCallTurns(callLog) {
  if (!callLog || typeof callLog !== "object") return [];
  const direct = Array.isArray(callLog.conversation?.turns) ? callLog.conversation.turns : [];
  const twilio = Array.isArray(callLog.twilio?.conversation?.turns)
    ? callLog.twilio.conversation.turns
    : [];
  const telnyx = Array.isArray(callLog.telnyx?.conversation?.turns)
    ? callLog.telnyx.conversation.turns
    : [];
  const pool = Array.isArray(callLog.pool?.conversation?.turns)
    ? callLog.pool.conversation.turns
    : [];
  return direct.length ? direct : twilio.length ? twilio : telnyx.length ? telnyx : pool;
}

function isTestCallLog(log) {
  if (!log || typeof log !== "object") return true;
  if (log.isTestCall === true || log.is_test === true || log.is_test_call === true) return true;
  try {
    const cp = log.custom_parameters;
    const parsed = typeof cp === "string" ? JSON.parse(cp) : cp;
    if (parsed?.is_test_call === true) return true;
  } catch {
    /* ignore */
  }
  return false;
}

function callIdFromLog(callLog) {
  const id = callLog?.call_id || callLog?.callId || callLog?.call_unique_id || null;
  return id ? String(id) : null;
}

function callTimesFromLog(callLog) {
  if (!callLog || typeof callLog !== "object") {
    return { callStartedAt: null, callEndedAt: null };
  }
  const started =
    callLog.startedAt ||
    callLog.started_at ||
    callLog.callStartedAt ||
    callLog.conversation?.startedAt ||
    callLog.createdAt ||
    null;
  const ended =
    callLog.endedAt ||
    callLog.ended_at ||
    callLog.completedAt ||
    callLog.hangupAt ||
    callLog.conversation?.endedAt ||
    null;
  return {
    callStartedAt: toIso(started),
    callEndedAt: toIso(ended),
  };
}

function callGroupFromLog(callLog) {
  const turns = rawCallTurns(callLog).map(conversationTurn).filter(Boolean);
  if (!turns.length) return null;
  const times = callTimesFromLog(callLog);
  return {
    callId: callIdFromLog(callLog),
    callStartedAt: times.callStartedAt,
    callEndedAt: times.callEndedAt,
    turns,
  };
}

function callTurnsFromAnalysis(analysis) {
  const data = analysis?.analysis_data || analysis?.analysisData || {};
  const qa = data.questions_analysis || data.Questions_Analysis || data.questionsAnalysis || [];
  if (!Array.isArray(qa)) return [];
  const turns = [];
  for (const row of qa) {
    if (!row || typeof row !== "object") continue;
    const q = String(row.agent_question ?? row.Agent_Question ?? row.question ?? "").trim();
    const a = String(row.user_answer ?? row.User_Answer ?? row.answer ?? "").trim();
    if (q) turns.push({ role: "assistant", text: q });
    if (a && a.toLowerCase() !== "not answered") turns.push({ role: "user", text: a });
  }
  return turns;
}

function isGroupedCallConversation(raw) {
  return Boolean(raw && typeof raw === "object" && (Array.isArray(raw.turns) || raw.callId));
}

function normalizeExplicitCallConversation(raw) {
  if (!Array.isArray(raw) || !raw.length) return [];
  if (isGroupedCallConversation(raw[0])) {
    return raw
      .map((group) => ({
        callId: group.callId ? String(group.callId) : null,
        callStartedAt: toIso(group.callStartedAt),
        callEndedAt: toIso(group.callEndedAt),
        turns: (group.turns || []).map(conversationTurn).filter(Boolean),
      }))
      .filter((group) => group.turns.length);
  }
  const turns = raw.map(conversationTurn).filter(Boolean);
  return turns.length ? [{ callId: null, callStartedAt: null, callEndedAt: null, turns }] : [];
}

function callConversationForAi({ callLogs, callLog, analysis, callConversation } = {}) {
  const explicit = normalizeExplicitCallConversation(callConversation);
  if (explicit.length) return explicit;

  const logs = Array.isArray(callLogs) && callLogs.length ? callLogs : callLog ? [callLog] : [];
  const groups = logs
    .filter((log) => !isTestCallLog(log))
    .map(callGroupFromLog)
    .filter(Boolean)
    .slice(-MAX_CALLS_FOR_AI);
  if (groups.length) return groups;

  const fromAnalysis = callTurnsFromAnalysis(analysis);
  if (!fromAnalysis.length) return [];
  return [
    {
      callId: analysis?.call_id || analysis?.callId ? String(analysis.call_id || analysis.callId) : null,
      callStartedAt: null,
      callEndedAt: null,
      turns: fromAnalysis,
    },
  ];
}

function buildSessionKey(phone, campaignId) {
  const p = String(phone || "").replace(/[\s+\-()]/g, "");
  const c = campaignId ? String(campaignId) : "none";
  return `wa:${p}:campaign:${c}`;
}

function buildWhatsappAiReplyPayload({
  phone,
  message,
  session,
  campaign,
  contact,
  analysis,
  callLog,
  callLogs,
  callConversation,
  callStartedAt,
  callEndedAt,
} = {}) {
  const grouped = callConversationForAi({ callLogs, callLog, analysis, callConversation });
  const latest = grouped.length ? grouped[grouped.length - 1] : null;
  return {
    task: "whatsapp_session_reply",
    phone: String(phone || "").trim(),
    message: String(message || "").trim(),
    sessionId: session?.sessionKey || buildSessionKey(phone, campaign?._id),
    campaignId: campaign?._id ? String(campaign._id) : null,
    contactId: contact?._id
      ? String(contact._id)
      : session?.contactId
        ? String(session.contactId)
        : null,
    callId:
      analysis?.call_id || analysis?.callId || session?.callId || latest?.callId || null,
    analysisId: analysis?._id
      ? String(analysis._id)
      : session?.analysisId
        ? String(session.analysisId)
        : null,
    callStartedAt: toIso(callStartedAt) || latest?.callStartedAt || null,
    callEndedAt: toIso(callEndedAt) || latest?.callEndedAt || null,
    callConversation: grouped,
    history: conversationForAi(session, contact),
  };
}

module.exports = {
  buildWhatsappAiReplyPayload,
  conversationForAi,
  callConversationForAi,
  MAX_CALLS_FOR_AI,
};
