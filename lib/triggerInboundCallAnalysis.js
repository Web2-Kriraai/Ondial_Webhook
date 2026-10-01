/**
 * POST inbound Analysis API after inbound conversation is stored / hangup finalized.
 * Persists on InboundConversation + call_analysis (direction: inbound).
 */
const logger = require("../logger");
const { getDb } = require("../db");
const { getRedis } = require("../redis");
const { ObjectId } = require("mongodb");
const { callLogDocIsTestCall } = require("./shouldSkipCreditDeduction");
const {
  buildInboundCallAnalysisPayload,
  shouldSkipInboundAnalysis,
} = require("./inbound/buildInboundCallAnalysisPayload");
const {
  inboundAnalysisEnabled,
  buildInboundAnalysisUrl,
} = require("./inbound/inboundAnalysisEnv");
const {
  createAnalysisRequestId,
  logAnalysisRequest,
  logAnalysisResponse,
  logAnalysisResponseFailed,
  logAnalysisRequestError,
} = require("./analysisApiLogger");

const ANALYSIS_API_MAX_ATTEMPTS = Number(process.env.ANALYSIS_API_MAX_ATTEMPTS || 4);
const ANALYSIS_API_RETRY_MS = Number(process.env.ANALYSIS_API_RETRY_MS || 4000);
const ANALYSIS_API_INITIAL_DELAY_MS = Number(process.env.ANALYSIS_API_INITIAL_DELAY_MS || 2000);
const ANALYSIS_TRIGGER_LOCK_SEC = Number(process.env.ANALYSIS_TRIGGER_LOCK_SEC || 120);
const INBOUND_COLL = process.env.INBOUNDCALLLOG_COLLECTION || "InboundConversation";
const RETRYABLE_HTTP = new Set([500, 502, 503, 504]);

function toObjectIdOrNull(value) {
  try {
    return value ? new ObjectId(String(value)) : null;
  } catch {
    return null;
  }
}

function pickString(...values) {
  for (const val of values) {
    if (val != null && String(val).trim() !== "") return String(val).trim();
  }
  return "";
}

async function acquireInboundAnalysisLock(callId) {
  try {
    const redis = getRedis();
    const key = `inbound_analysis:trigger:${callId}`;
    const ok = await redis.set(key, "1", "EX", ANALYSIS_TRIGGER_LOCK_SEC, "NX");
    return ok === "OK";
  } catch (err) {
    logger.warn("[InboundAnalysis] Redis lock unavailable — proceeding without dedupe", {
      callId,
      error: err.message,
    });
    return true;
  }
}

function normalizeAnalysisResponse(raw, opts = {}) {
  if (!raw || typeof raw !== "object") return null;
  const data = raw.analysis && typeof raw.analysis === "object" ? raw.analysis : raw;

  const conversationSummary =
    data.conversationSummary || data.conversation_summary || data.summary || null;
  const humanCallbackNeeded =
    data.humanCallbackNeeded ?? data.human_callback_needed ?? null;
  const questionAnalysisRaw =
    data.questionAnalysis || data.questions_analysis || null;
  const userQuestionsRaw = data.userQuestions || data.user_questions || null;

  const analyzedAtRaw =
    data.analyzedAt || data.analyzed_at || opts.analyzedAt || null;
  let analyzedAt = null;
  if (analyzedAtRaw != null) {
    const d = analyzedAtRaw instanceof Date ? analyzedAtRaw : new Date(analyzedAtRaw);
    if (!Number.isNaN(d.getTime())) analyzedAt = d.toISOString();
  }

  const {
    conversation_summary: _dropConversationSummary,
    human_callback_needed: _dropHumanCallbackNeeded,
    questions_analysis: _dropQuestionsAnalysis,
    user_questions: _dropUserQuestions,
    summary: _dropSummary,
    analyzed_at: _dropAnalyzedAtSnake,
    conversationSummary: _dropCsCamel,
    humanCallbackNeeded: _dropHcnCamel,
    questionAnalysis: _dropQaCamel,
    userQuestions: _dropUqCamel,
    analyzedAt: _dropAnalyzedAtCamel,
    ...rest
  } = data;

  const out = { ...rest };

  if (conversationSummary) out.conversationSummary = conversationSummary;
  if (humanCallbackNeeded != null) out.humanCallbackNeeded = Boolean(humanCallbackNeeded);
  if (analyzedAt) out.analyzedAt = analyzedAt;
  if (Array.isArray(questionAnalysisRaw)) {
    out.questionAnalysis = questionAnalysisRaw.map((q) => {
      if (!q || typeof q !== "object") return q;
      return {
        question: q.question ?? q.Question ?? null,
        userAnswer: q.userAnswer ?? q.user_answer ?? q.User_Answer ?? q.answer ?? null,
        aiInsights:
          q.aiInsights ?? q.ai_insights ?? q.aiAnalysis ?? q.AI_Analysis ?? q.ai_analysis ?? null,
      };
    });
  } else if (questionAnalysisRaw != null) {
    out.questionAnalysis = questionAnalysisRaw;
  }
  if (Array.isArray(userQuestionsRaw)) {
    out.userQuestions = userQuestionsRaw;
  } else if (userQuestionsRaw != null) {
    out.userQuestions = userQuestionsRaw;
  }

  return out;
}

async function loadInboundConversationDoc(callId) {
  const db = getDb();
  const coll = db.collection(INBOUND_COLL);
  const id = String(callId || "").trim();
  if (!id) return null;

  const or = [{ call_id: id }, { call_unique_id: id }, { call_sid: id }, { callSid: id }];
  if (/^[a-f0-9]{24}$/i.test(id)) {
    try {
      or.push({ _id: new ObjectId(id) });
    } catch {
      /* ignore */
    }
  }

  // Prefer docs that already have turns.
  const docs = await coll
    .find({ $or: or })
    .sort({ startedAt: -1, updatedAt: -1, _id: -1 })
    .limit(8)
    .toArray();
  if (!docs.length) return null;
  docs.sort((a, b) => {
    const ta = Array.isArray(a?.conversation?.turns) ? a.conversation.turns.length : 0;
    const tb = Array.isArray(b?.conversation?.turns) ? b.conversation.turns.length : 0;
    return tb - ta;
  });
  return docs[0];
}

async function loadInboundConfigAndCompany(configId) {
  const db = getDb();
  const oid = toObjectIdOrNull(configId);
  if (!oid) return { config: null, company: null };
  const config = await db.collection("inboundconfigs").findOne({ _id: oid });
  if (!config) return { config: null, company: null };
  let company = null;
  const companyOid = toObjectIdOrNull(config.companyId);
  if (companyOid) {
    company = await db.collection("companies").findOne({ _id: companyOid });
  }
  return { config, company };
}

/**
 * @param {string} callId — call_id / call_sid / conversation _id
 * @param {object} [opts]
 * @param {boolean} [opts.isTestCall]
 * @param {boolean} [opts.deferIfNoTurns]
 */
async function triggerInboundCallAnalysis(callId, opts = {}) {
  if (!inboundAnalysisEnabled()) {
    return { triggered: false, skipped: true, reason: "disabled" };
  }

  const id = String(callId || "").trim();
  if (!id) {
    return { triggered: false, reason: "call_id_missing" };
  }

  const url = buildInboundAnalysisUrl();
  if (!url) {
    logger.warn("[InboundAnalysis] ANALYSIS_API_URL / INBOUND_ANALYSIS_URL missing — skip");
    return { triggered: false, skipped: true, reason: "analysis_api_url_missing" };
  }

  const locked = await acquireInboundAnalysisLock(id);
  if (!locked) {
    logger.info("[InboundAnalysis] Skip — inflight lock held", { callId: id });
    return { triggered: false, skipped: true, reason: "inflight_lock" };
  }

  const conversation = await loadInboundConversationDoc(id);
  if (!conversation) {
    return { triggered: false, reason: "conversation_not_found" };
  }

  const isTest =
    opts.isTestCall === true || callLogDocIsTestCall(conversation) === true;

  const skip = shouldSkipInboundAnalysis({
    conversation,
    isTestCall: isTest,
  });
  if (skip.skip) {
    if (opts.deferIfNoTurns && skip.reason === "conversation_text_empty") {
      return { triggered: false, deferred: true, reason: skip.reason };
    }
    return { triggered: false, skipped: true, reason: skip.reason };
  }

  const configId = pickString(
    conversation.config_id,
    conversation.configId,
    conversation.inboundConfigId
  );
  const { config, company } = await loadInboundConfigAndCompany(configId);
  if (!config) {
    return { triggered: false, reason: "config_not_found" };
  }

  const built = buildInboundCallAnalysisPayload({
    config,
    company,
    conversation,
  });
  if (!built.ok) {
    if (opts.deferIfNoTurns && built.reason === "conversation_text_empty") {
      return { triggered: false, deferred: true, reason: built.reason };
    }
    return { triggered: false, reason: built.reason };
  }

  const db = getDb();
  const coll = db.collection(INBOUND_COLL);
  const persistCallId = pickString(
    built.payload.callId,
    conversation.call_id,
    conversation._id
  );

  const claim = await coll.findOneAndUpdate(
    {
      _id: conversation._id,
      $or: [
        { analysisStatus: { $exists: false } },
        { analysisStatus: null },
        { analysisStatus: { $nin: ["completed", "in_progress"] } },
      ],
      analysis_data: { $exists: false },
    },
    {
      $set: {
        analysisStatus: "in_progress",
        analysisStartedAt: new Date(),
        updatedAt: new Date().toISOString(),
      },
    },
    { returnDocument: "after" }
  );

  // Mongo driver may return doc directly or { value }
  const claimed = claim && (claim.value !== undefined ? claim.value : claim);
  if (!claimed) {
    return { triggered: false, skipped: true, reason: "already_analyzed_or_inflight" };
  }

  if (ANALYSIS_API_INITIAL_DELAY_MS > 0) {
    await new Promise((r) => setTimeout(r, ANALYSIS_API_INITIAL_DELAY_MS));
  }

  const fetchOptions = {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      "x-header-key": String(process.env.ANALYSIS_API_HEADER_KEY || "1"),
    },
    body: JSON.stringify(built.payload),
  };

  let lastFailure = null;
  for (let attempt = 1; attempt <= ANALYSIS_API_MAX_ATTEMPTS; attempt++) {
    const requestId = createAnalysisRequestId();
    const requestStartedAt = Date.now();
    logAnalysisRequest({
      requestId,
      callId: id,
      attempt,
      url,
      method: "POST",
      headers: fetchOptions.headers,
      body: fetchOptions.body,
    });

    try {
      const res = await fetch(url, fetchOptions);
      const durationMs = Date.now() - requestStartedAt;
      const responseText = await res.text();

      if (res.ok) {
        logAnalysisResponse({
          requestId,
          callId: id,
          attempt,
          url,
          status: res.status,
          durationMs,
          body: responseText,
        });

        let parsed = null;
        try {
          parsed = JSON.parse(responseText);
        } catch {
          parsed = null;
        }
        const analysisData = normalizeAnalysisResponse(parsed, {
          analyzedAt: new Date().toISOString(),
        });
        if (!analysisData) {
          await coll.updateOne(
            { _id: conversation._id },
            {
              $set: {
                analysisStatus: "failed",
                analysisError: "invalid_json_response",
                updatedAt: new Date().toISOString(),
              },
            }
          );
          return { triggered: false, reason: "invalid_json_response" };
        }

        const setFields = {
          analysisStatus: "completed",
          analysis_data: analysisData,
          analysisError: null,
          analysisCompletedAt: new Date(),
          updatedAt: new Date().toISOString(),
        };
        if (analysisData.conversationSummary) {
          setFields.conversationSummary = analysisData.conversationSummary;
        }
        if (analysisData.humanCallbackNeeded != null) {
          setFields.humanCallbackNeeded = analysisData.humanCallbackNeeded;
        }

        await coll.updateOne({ _id: conversation._id }, { $set: setFields });

        const userIdObj =
          toObjectIdOrNull(conversation.userId) || toObjectIdOrNull(config.userId);

        await db.collection("call_analysis").updateOne(
          { call_id: String(persistCallId) },
          {
            $set: {
              call_id: String(persistCallId),
              internalCallId: String(persistCallId),
              input_call_id: String(persistCallId),
              direction: "inbound",
              inboundConfigId: toObjectIdOrNull(config._id),
              config_id: toObjectIdOrNull(config._id),
              userId: userIdObj,
              organizationId: config.organizationId || null,
              analysis_data: analysisData,
              analysisStatus: "completed",
              followUpError: null,
              updated_at: new Date(),
            },
            $setOnInsert: { created_at: new Date(), followUpProcessed: false },
          },
          { upsert: true }
        );

        if (userIdObj) {
          try {
            const { notifyTenantInboundCallAnalysis } = require("./notifyTenantCallStatus");
            void notifyTenantInboundCallAnalysis({
              userId: userIdObj,
              callId: persistCallId,
              inboundConfigId: config._id,
            });
          } catch (notifyErr) {
            logger.warn("[InboundAnalysis] Tenant notify failed", {
              callId: id,
              error: notifyErr.message,
            });
          }
        }

        logger.info("[InboundAnalysis] Persisted analysis", {
          callId: persistCallId,
          inputCallId: id,
        });
        return { triggered: true, callId: persistCallId };
      }

      logAnalysisResponseFailed({
        requestId,
        callId: id,
        attempt,
        url,
        status: res.status,
        durationMs,
        body: responseText,
      });
      lastFailure = { status: res.status, body: responseText };
      // Don't retry schema/validation 422s (e.g. empty businessHours).
      // Only retry 422 when API complains about missing conversation_text (race).
      const isConversationRace =
        Number(res.status) === 422 && /conversation_text/i.test(String(responseText || ""));
      if (!RETRYABLE_HTTP.has(Number(res.status)) && !isConversationRace) {
        break;
      }
    } catch (fetchErr) {
      logAnalysisRequestError({
        requestId,
        callId: id,
        attempt,
        url,
        error: fetchErr,
      });
      lastFailure = { error: fetchErr.message };
    }

    if (attempt < ANALYSIS_API_MAX_ATTEMPTS) {
      await new Promise((r) => setTimeout(r, ANALYSIS_API_RETRY_MS));
    }
  }

  await coll.updateOne(
    { _id: conversation._id },
    {
      $set: {
        analysisStatus: "failed",
        analysisError: lastFailure
          ? JSON.stringify(lastFailure).slice(0, 500)
          : "request_failed",
        updatedAt: new Date().toISOString(),
      },
    }
  );

  return {
    triggered: false,
    reason: lastFailure?.status
      ? `http_${lastFailure.status}`
      : lastFailure?.error || "request_failed",
  };
}

module.exports = {
  triggerInboundCallAnalysis,
  buildInboundAnalysisUrl,
  inboundAnalysisEnabled,
};
