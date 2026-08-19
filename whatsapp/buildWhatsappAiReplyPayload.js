/**
 * Canonical WhatsApp session-reply AI payload built on Ondial_Webhook.
 * All production calls for the contact (oldest → newest) + WhatsApp history.
 */

const { resolveCampaignIntlTimeZoneId } = require("./campaignIntlTimeZone");

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
  return Boolean(raw && typeof raw === "object" && (Array.isArray(raw.turns) || raw.callId || raw.call_id));
}

function normalizeExplicitCallConversation(raw) {
  if (!Array.isArray(raw) || !raw.length) return [];
  if (isGroupedCallConversation(raw[0])) {
    return raw
      .map((group) => ({
        callId: String(group.callId || group.call_id || "").trim() || null,
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

function withTurnTimestamps(turns, startIso) {
  const parsed = Date.parse(String(startIso || ""));
  const base = Number.isNaN(parsed) ? Date.now() : parsed;
  return (turns || []).map((turn, index) => {
    const timestamp = toIso(turn.timestamp) || new Date(base + index * 4000).toISOString();
    return {
      role: turn.role,
      text: turn.text,
      timestamp,
    };
  });
}

function pythonCallConversation(groups) {
  return (groups || [])
    .map((group, index) => {
      const callId = String(group.call_id || group.callId || "").trim() || `call-${index + 1}`;
      const start = group.callStartedAt || group.call_started_at || group.turns?.[0]?.timestamp;
      const turns = withTurnTimestamps(group.turns, start);
      if (!turns.length) return null;
      return { call_id: callId, turns };
    })
    .filter(Boolean);
}

function pythonWhatsappHistory(history) {
  return withTurnTimestamps(history || [], new Date().toISOString());
}

function matchingCallId(preferred, pythonCalls) {
  const ids = (pythonCalls || []).map((row) => String(row.call_id || "").trim()).filter(Boolean);
  const want = String(preferred || "").trim();
  if (want && ids.includes(want)) return want;
  return ids.length ? ids[ids.length - 1] : want || null;
}

function resolveCompanyName(campaign, knowledgeBase) {
  const named = asTrimmed(
    campaign?.companyName || campaign?.selectedCompanyName || campaign?.company?.name,
    120
  );
  if (named) return named;
  const kb = String(knowledgeBase || campaign?.knowledgeBaseSummarized || "").trim();
  if (kb) {
    const beforeIs = kb.split(/\s+is\s+/i)[0].trim();
    if (beforeIs && beforeIs.length <= 80) return beforeIs;
    const words = kb.split(/\s+/).slice(0, 3).join(" ").trim();
    if (words) return words;
  }
  return "OnDial";
}

function buildSessionKey(phone, campaignId) {
  const p = String(phone || "").replace(/[\s+\-()]/g, "");
  const c = campaignId ? String(campaignId) : "none";
  return `wa:${p}:campaign:${c}`;
}

function asTrimmed(value, max = 0) {
  const text = String(value ?? "").replace(/\s+/g, " ").trim();
  if (!text) return "";
  if (max > 0 && text.length > max) return `${text.slice(0, max)}…`;
  return text;
}

function weekdayHours(businessHours) {
  if (!businessHours || typeof businessHours !== "object") return "";
  const days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"];
  return days
    .map((day) => {
      const row = businessHours[day];
      if (!row || row.closed) return `${day}: Closed`;
      const open = String(row.open || "").trim();
      const close = String(row.close || "").trim();
      if (!open && !close) return `${day}: Closed`;
      return `${day}: ${open || "?"}-${close || "?"}`;
    })
    .join("; ");
}

function formatZonedDateTime(value, timeZone) {
  const date = value instanceof Date ? value : value ? new Date(value) : new Date();
  if (Number.isNaN(date.getTime())) return null;
  const tz = resolveCampaignIntlTimeZoneId(timeZone);
  try {
    const parts = Object.fromEntries(
      new Intl.DateTimeFormat("en-US", {
        timeZone: tz,
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hourCycle: "h23",
      })
        .formatToParts(date)
        .filter((part) => part.type !== "literal")
        .map((part) => [part.type, part.value])
    );
    const asUtc = Date.UTC(
      Number(parts.year),
      Number(parts.month) - 1,
      Number(parts.day),
      Number(parts.hour),
      Number(parts.minute),
      Number(parts.second)
    );
    const offsetMin = Math.round((asUtc - date.getTime()) / 60000);
    const sign = offsetMin >= 0 ? "+" : "-";
    const abs = Math.abs(offsetMin);
    const hh = String(Math.floor(abs / 60)).padStart(2, "0");
    const mm = String(abs % 60).padStart(2, "0");
    return `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:${parts.second}${sign}${hh}:${mm}`;
  } catch {
    return date.toISOString();
  }
}

function firstName(value) {
  return asTrimmed(value).split(/\s+/)[0] || "";
}

function contactNameFromDoc(contact) {
  if (!contact || typeof contact !== "object") return "";
  const data = contact.contactData && typeof contact.contactData === "object" ? contact.contactData : {};
  for (const key of Object.keys(data)) {
    const k = key.toLowerCase().replace(/[\s_-]/g, "");
    if (k === "name" || k === "fullname" || k === "firstname" || k === "customername") {
      return firstName(data[key]);
    }
  }
  return firstName(contact.name || contact.userName || "");
}

function analysisDataOf(analysis) {
  if (!analysis || typeof analysis !== "object") return {};
  const data = analysis.analysis_data || analysis.analysisData || analysis;
  return data && typeof data === "object" && !Array.isArray(data) ? data : {};
}

function callAnalysisBlock(analysis) {
  const data = analysisDataOf(analysis);
  const summary = asTrimmed(
    data.conversation_summary ||
      data.Conversation_Summary ||
      data.summary ||
      data.Summary ||
      analysis?.summary ||
      "",
    1200
  );
  const callback = data.callback_requested || data.Callback_Requested || data.callbackRequested || {};
  const status = callback?.status === true || callback?.Status === true;
  const window = asTrimmed(
    callback.window || callback.Window || callback.scheduled_at || callback.scheduledAt || callback.Scheduled_At || ""
  );
  return {
    summary,
    callback_requested: {
      status,
      window: status ? window || null : null,
    },
  };
}

function languageCode(value) {
  const raw = String(value || "en").trim();
  if (!raw) return "en";
  return raw.split(/[-_]/)[0].toLowerCase() || "en";
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
  const pythonCalls = pythonCallConversation(grouped);
  const timezone = resolveCampaignIntlTimeZoneId(campaign?.timezone || "Asia/Kolkata");
  const campaignId = campaign?._id ? String(campaign._id) : null;
  const phoneNorm = String(phone || "").trim();
  const callAnalysis = callAnalysisBlock(analysis);
  const channels = Array.isArray(campaign?.followupChannels) ? campaign.followupChannels : [];
  const followup = campaign?.followup === true;
  const wizardServiceId = String(
    (Array.isArray(campaign?.selectedServices) && campaign.selectedServices[0]) || ""
  );
  const subServiceId = String(campaign?.campaignServiceSubId || "");
  const knowledgeBase = asTrimmed(campaign?.knowledgeBaseSummarized, 1500);

  return {
    payload: {
      session_id: session?.sessionKey || buildSessionKey(phoneNorm, campaignId),
      campaign_id: campaignId,
      contact_id: contact?._id
        ? String(contact._id)
        : session?.contactId
          ? String(session.contactId)
          : null,
      call_id: matchingCallId(
        analysis?.call_id || analysis?.callId || session?.callId,
        pythonCalls
      ),
      wizard_service_id: wizardServiceId,
      sub_service_id: subServiceId,
      current_time: formatZonedDateTime(new Date(), timezone),
      timezone,
      language: languageCode(campaign?.primaryLanguage),
      contact: {
        name: contactNameFromDoc(contact) || firstName(session?.userName) || "there",
        mobile: phoneNorm,
      },
      company: {
        name: resolveCompanyName(campaign, knowledgeBase),
        description: asTrimmed(campaign?.companyDescription, 800),
        business_hours: asTrimmed(
          weekdayHours(campaign?.businessHours) || weekdayHours(campaign?.callingHours?.schedule),
          400
        ),
      },
      agent: {
        name: asTrimmed(campaign?.agentName, 80) || "our team",
      },
      knowledge_base_summary: knowledgeBase,
      features_enabled: {
        is_followup_enabled: followup,
        whatsapp_followup: { status: followup && channels.includes("whatsapp") },
        callback_scheduling: {
          status:
            (followup && channels.includes("call")) ||
            campaign?.appointmentsDemosEnabled === true ||
            callAnalysis.callback_requested.status === true,
        },
      },
      call_analysis: callAnalysis,
      inbound_message: String(message || "").trim(),
      call_conversation: pythonCalls,
      whatsapp_history: pythonWhatsappHistory(conversationForAi(session, contact)),
    },
  };
}

module.exports = {
  buildWhatsappAiReplyPayload,
  conversationForAi,
  callConversationForAi,
  formatZonedDateTime,
  MAX_CALLS_FOR_AI,
};
