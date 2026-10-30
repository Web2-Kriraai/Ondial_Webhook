/**
 * Inbound caller memory: one rolling `overall_summary` per caller for an agent, carried from call to call.
 *
 * - The analysis request at call end sends `previous_overall_summary` (+ `previous_calls_count`) for a
 *   returning caller; the analysis returns `overall_summary` (previous + this call merged), stored here and
 *   sent by Ondial's validate on the caller's next call.
 * - Until the analysis returns `overall_summary`, this call's `conversationSummary` is appended to the stored
 *   one as a dated line (capped).
 *
 * Scope: same owner, same agent lineage, same business (company profile), same caller number.
 * CommonJS copy of Ondial lib/inbound/callerMemory.pure.js — keep the two in step (same collection, keys,
 * line format and cap). Pure: no DB.
 */

const INBOUND_CALLER_MEMORY_COLLECTION = "inbound_caller_memory";
const CALLER_MEMORY_SEED_CALLS = 3;
const CALLER_MEMORY_WINDOW_DAYS = 90;
const CALLER_MEMORY_MAX_CHARS = 2000;

function asString(value) {
  return value == null ? "" : String(value).trim();
}

function clip(text, max) {
  const s = asString(text);
  return s.length > max ? `${s.slice(0, max - 1).trimEnd()}…` : s;
}

/** The ways one caller number can be stored (+918200773061, 918200773061, 8200773061). */
function callerNumberVariants(number) {
  const raw = asString(number);
  const digits = raw.replace(/\D/g, "");
  if (digits.length < 7) return [];
  const out = new Set([raw, digits, `+${digits}`]);
  if (digits.length > 10) {
    const last10 = digits.slice(-10);
    out.add(last10);
    out.add(`+${digits.slice(0, digits.length - 10)}${last10}`);
  }
  if (digits.length === 10) {
    out.add(`91${digits}`);
    out.add(`+91${digits}`);
  }
  return [...out].filter(Boolean);
}

/** One key per caller whatever the format: the last 10 digits ('' when withheld / invalid). */
function callerNumberKey(number) {
  const digits = asString(number).replace(/\D/g, "");
  if (digits.length < 7) return "";
  return digits.length > 10 ? digits.slice(-10) : digits;
}

/** Filter for this caller's memory record on this agent, or null when there is nothing to key on. */
function callerMemoryFilter(config, callerNumber) {
  const callerKey = callerNumberKey(callerNumber);
  if (!config || !callerKey) return null;
  const lineage = asString(config.lineageId) || asString(config._id);
  if (!lineage) return null;
  const business = config.companyId
    ? `company:${asString(config.companyId)}`
    : `category:${asString(config.category) || "custom"}`;
  return {
    userId: asString(config.userId) || null,
    lineageKey: lineage,
    businessKey: business,
    callerKey,
  };
}

function callerMemorySince(now = new Date(), days = CALLER_MEMORY_WINDOW_DAYS) {
  return new Date(new Date(now).getTime() - days * 24 * 60 * 60 * 1000);
}

function formatIst(value) {
  const d = value ? new Date(value) : null;
  if (!d || Number.isNaN(d.getTime())) return "";
  return d.toLocaleString("en-IN", {
    timeZone: "Asia/Kolkata",
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: true,
  });
}

function datedLine(when, summary) {
  const at = formatIst(when);
  const text = asString(summary).replace(/\s+/g, " ");
  return at ? `${at} IST: ${text}` : text;
}

function capLines(lines, max = CALLER_MEMORY_MAX_CHARS) {
  const kept = [...lines];
  while (kept.length > 1 && kept.join("\n").length > max) kept.shift();
  return clip(kept.join("\n"), max);
}

/** Overall summary for a caller with no memory record yet: their latest analysed calls, oldest first. */
function seedOverallFromConversations(rows = [], { limit = CALLER_MEMORY_SEED_CALLS } = {}) {
  const usable = (Array.isArray(rows) ? rows : [])
    .filter((r) => r && asString(r.conversationSummary))
    .map((r) => ({ r, t: new Date(r.startedAt || r.createdAt || 0).getTime() || 0 }))
    .sort((a, b) => b.t - a.t)
    .slice(0, limit)
    .reverse();
  if (usable.length === 0) return null;
  const last = usable[usable.length - 1].r;
  return {
    summary: capLines(usable.map(({ r }) => datedLine(r.startedAt || r.createdAt, r.conversationSummary))),
    callsCount: usable.length,
    lastCallAt: last.startedAt || last.createdAt || null,
  };
}

/** The overall summary as it stood BEFORE this call (a retried analysis reads the saved snapshot). */
function previousOverallFor(record, callId) {
  if (!record || typeof record !== "object") return null;
  const sameCall = asString(callId) && asString(record.lastCallId) === asString(callId);
  const summary = asString(sameCall ? record.previousOverallSummary : record.overallSummary);
  if (!summary) return null;
  const count = Number(sameCall ? record.previousCallsCount : record.callsCount);
  return {
    summary,
    callsCount: Number.isFinite(count) && count > 0 ? count : 1,
    lastCallAt: sameCall ? record.previousLastCallAt || null : record.lastCallAt || null,
  };
}

/** Analysis request fields for a returning caller; {} for a first call (fields omitted). */
function analysisMemoryFields(previous) {
  if (!previous || !previous.summary) return {};
  return {
    previous_overall_summary: previous.summary,
    previous_calls_count: previous.callsCount,
  };
}

function readOverallSummary(analysisData) {
  const d = analysisData && typeof analysisData === "object" ? analysisData : {};
  const inner = d.analysis && typeof d.analysis === "object" ? d.analysis : {};
  return asString(
    d.overall_summary != null
      ? d.overall_summary
      : d.overallSummary != null
        ? d.overallSummary
        : inner.overall_summary != null
          ? inner.overall_summary
          : inner.overallSummary
  );
}

/** The caller's new overall summary after this call, or null when there is nothing to store. */
function nextOverallSummary({ previous = null, analysisData = null, callStartedAt = null } = {}) {
  const fromAnalysis = readOverallSummary(analysisData);
  if (fromAnalysis) return { summary: clip(fromAnalysis, CALLER_MEMORY_MAX_CHARS * 2), source: "analysis" };
  const thisCall = asString(
    analysisData && (analysisData.conversationSummary != null ? analysisData.conversationSummary : analysisData.conversation_summary)
  );
  if (!thisCall) return null;
  const line = datedLine(callStartedAt, thisCall);
  const lines = previous && previous.summary ? [...previous.summary.split("\n").filter(Boolean), line] : [line];
  return { summary: capLines(lines), source: "fallback" };
}

/** $set / $setOnInsert after this call's analysis; keeps the pre-call state for retries. */
function callerMemoryUpdate({ previous, next, callId, callerNumber, configId, callStartedAt, now = new Date() }) {
  const callsCount = ((previous && previous.callsCount) || 0) + 1;
  return {
    $set: {
      overallSummary: next.summary,
      source: next.source,
      callsCount,
      lastCallId: asString(callId) || null,
      lastCallAt: callStartedAt ? new Date(callStartedAt) : now,
      lastConfigId: asString(configId) || null,
      callerNumber: asString(callerNumber) || null,
      previousOverallSummary: (previous && previous.summary) || "",
      previousCallsCount: (previous && previous.callsCount) || 0,
      previousLastCallAt: (previous && previous.lastCallAt) || null,
      updatedAt: now,
    },
    $setOnInsert: { createdAt: now },
  };
}

module.exports = {
  INBOUND_CALLER_MEMORY_COLLECTION,
  CALLER_MEMORY_SEED_CALLS,
  CALLER_MEMORY_WINDOW_DAYS,
  CALLER_MEMORY_MAX_CHARS,
  callerNumberVariants,
  callerNumberKey,
  callerMemoryFilter,
  callerMemorySince,
  seedOverallFromConversations,
  previousOverallFor,
  analysisMemoryFields,
  readOverallSummary,
  nextOverallSummary,
  callerMemoryUpdate,
};
