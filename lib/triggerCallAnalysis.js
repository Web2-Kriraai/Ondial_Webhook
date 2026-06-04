/**
 * POST to analysis service so Mongo `call_analysis` is populated for campaign analytics UI.
 *
 * Default: legacy URL — POST /analyze/call/{callId} (no JSON body; API reads Mongo).
 * Optional body payload (Postman spec) — set ANALYSIS_USE_BODY_PAYLOAD=1 in .env.
 */
const logger = require("../logger");
const { getDb } = require("../db");
const { getRedis } = require("../redis");
const { ObjectId } = require("mongodb");

const ANALYSIS_API_MAX_ATTEMPTS = Number(process.env.ANALYSIS_API_MAX_ATTEMPTS || 5);
const ANALYSIS_API_RETRY_MS = Number(process.env.ANALYSIS_API_RETRY_MS || 3000);
const ANALYSIS_API_INITIAL_DELAY_MS = Number(process.env.ANALYSIS_API_INITIAL_DELAY_MS || 2000);
const ANALYSIS_LOG_PAYLOAD = process.env.ANALYSIS_LOG_PAYLOAD === "1";
const ANALYSIS_USE_BODY_PAYLOAD = process.env.ANALYSIS_USE_BODY_PAYLOAD === "1";
const ANALYSIS_TRIGGER_LOCK_SEC = Number(process.env.ANALYSIS_TRIGGER_LOCK_SEC || 120);
const UUID_CALL_ID_RE =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const TWILIO_CALL_SID_RE = /^CA[a-f0-9]{32}$/i;
const ANALYSIS_DEFAULT_SERVICE_NAME = String(process.env.ANALYSIS_DEFAULT_SERVICE_NAME || "sales").trim();
const ANALYSIS_DEFAULT_REASON = String(
    process.env.ANALYSIS_DEFAULT_REASON || "Analyze the call conversation."
).trim();

/** Legacy: http://host:5000/analyze/call/{callId} */
function buildAnalysisUrl(callId) {
    const raw = String(process.env.ANALYSIS_API_URL || "http://72.60.221.48:5000").replace(/\/$/, "");
    const id = encodeURIComponent(String(callId));
    if (raw.includes("{CALL_ID}")) {
        return raw.replaceAll("{CALL_ID}", id);
    }
    if (/\/analyze\/call\/[^/]+$/i.test(raw)) {
        return raw;
    }
    if (raw.endsWith("/analyze/call")) {
        return `${raw}/${id}`;
    }
    if (raw.includes("/analyze/call")) {
        return `${raw}/${id}`;
    }
    return `${raw}/analyze/call/${id}`;
}

function pickString(...values) {
    for (const val of values) {
        if (val != null && String(val).trim() !== "") {
            return String(val).trim();
        }
    }
    return "";
}

function safeStringify(payload) {
    try {
        return JSON.stringify(payload || {});
    } catch (err) {
        return JSON.stringify({ stringify_error: err.message });
    }
}

// Body payload builders (Postman spec) — used only when ANALYSIS_USE_BODY_PAYLOAD=1
function normalizeConversationTurns(callLogDoc) {
    const directTurns = Array.isArray(callLogDoc?.conversation?.turns) ? callLogDoc.conversation.turns : [];
    const twilioTurns = Array.isArray(callLogDoc?.twilio?.conversation?.turns)
        ? callLogDoc.twilio.conversation.turns
        : [];
    const sourceTurns = directTurns.length ? directTurns : twilioTurns;

    return sourceTurns
        .map((turn) => (turn && typeof turn === "object" ? turn : null))
        .filter(Boolean)
        .map((turn) => {
            const role = pickString(turn.role, turn.speaker);
            const text = pickString(turn.text, turn.message, turn.content);
            if (role && text) {
                const label =
                    /^(ai|agent|assistant|bot)$/i.test(role) ? "Agent" : "User";
                return { [label]: text };
            }
            const entries = Object.entries(turn).filter(
                ([k, v]) => String(k || "").trim() !== "" && v != null && String(v).trim() !== ""
            );
            if (!entries.length) return null;
            const [speaker, message] = entries[0];
            return { [String(speaker).trim()]: String(message).trim() };
        })
        .filter(Boolean);
}

function pickRecruitmentConfig(campaignDoc = {}, callLogDoc = {}) {
    return (
        callLogDoc?.recruitment_config ||
        callLogDoc?.recruitmentConfig ||
        campaignDoc?.recruitment_config ||
        campaignDoc?.recruitmentConfig ||
        null
    );
}

function buildAnalysisPayload({ callLogDoc, campaignDoc }) {
    const serviceName = pickString(
        callLogDoc?.service_name,
        callLogDoc?.serviceName,
        campaignDoc?.service_name,
        campaignDoc?.serviceName,
        campaignDoc?.type,
        ANALYSIS_DEFAULT_SERVICE_NAME
    );
    const reasonForCalling = pickString(
        callLogDoc?.reason_for_calling,
        callLogDoc?.reasonForCalling,
        campaignDoc?.reason_for_calling,
        campaignDoc?.reasonForCalling,
        campaignDoc?.call_reason,
        ANALYSIS_DEFAULT_REASON
    );

    const turns = normalizeConversationTurns(callLogDoc);
    if (!turns.length) return null;

    const payload = {
        service_name: serviceName,
        reason_for_calling: reasonForCalling,
        conversation: { turns },
    };

    const questions = callLogDoc?.questions || campaignDoc?.questions;
    if (Array.isArray(questions) && questions.length) {
        payload.questions = questions
            .filter((q) => q && typeof q === "object" && pickString(q.question))
            .map((q) => ({
                question: pickString(q.question),
                expectedAnswer: pickString(q.expectedAnswer) || "text",
            }));
    }

    const classifications = callLogDoc?.classifications || campaignDoc?.classifications;
    if (Array.isArray(classifications) && classifications.length) {
        payload.classifications = classifications
            .filter((c) => c && typeof c === "object" && pickString(c.name))
            .map((c) => {
                const item = { name: pickString(c.name) };
                const description = pickString(c.description);
                if (description) item.description = description;
                return item;
            });
    }

    if (String(serviceName).toLowerCase() === "recruitment") {
        const recruitmentConfig = pickRecruitmentConfig(campaignDoc, callLogDoc);
        if (recruitmentConfig && typeof recruitmentConfig === "object") {
            payload.recruitment_config = recruitmentConfig;
        }
    }

    return payload;
}

function isUuidCallId(value) {
    return UUID_CALL_ID_RE.test(String(value || "").trim());
}

function isTwilioCallSid(value) {
    return TWILIO_CALL_SID_RE.test(String(value || "").trim());
}

function buildCallLogLookupFilter(callId) {
    const id = String(callId || "").trim();
    const clauses = [{ lead_id: id }, { call_id: id }];
    if (id) {
        clauses.push({ lead_id: `twilio:${id}` }, { "twilio.call_sid": id });
    }
    return { $or: clauses };
}

async function loadCallLogDoc(callId) {
    const db = getDb();
    const collectionName = process.env.CALLLOGS_COLLECTION || "CallLogs";
    return db.collection(collectionName).findOne(buildCallLogLookupFilter(callId));
}

/** Legacy analysis API indexes CallLogs by dialer call_unique_id (UUID), not Twilio CallSid. */
function resolveLegacyAnalysisCallId(callLogDoc, fallbackId) {
    const candidates = [
        callLogDoc?.call_unique_id,
        callLogDoc?.twilio?.external_call_id,
        isUuidCallId(callLogDoc?.lead_id) ? callLogDoc.lead_id : null,
        isUuidCallId(callLogDoc?.call_id) ? callLogDoc.call_id : null,
        callLogDoc?.twilio?.external_lead_id,
    ];
    for (const candidate of candidates) {
        const resolved = pickString(candidate);
        if (resolved) return resolved;
    }
    return String(fallbackId || "").trim();
}

async function acquireAnalysisTriggerLock(callId) {
    try {
        const redis = getRedis();
        const key = `analysis:trigger:${callId}`;
        const ok = await redis.set(key, "1", "EX", ANALYSIS_TRIGGER_LOCK_SEC, "NX");
        return ok === "OK";
    } catch (err) {
        logger.warn("[Analysis] Redis lock unavailable — proceeding without dedupe", {
            callId,
            error: err.message,
        });
        return true;
    }
}

async function resolveAnalysisPayload(callId) {
    const callLogDoc = await loadCallLogDoc(callId);

    if (!callLogDoc) {
        return { payload: null, reason: "call_log_not_found" };
    }

    let campaignDoc = null;
    const campaignId = pickString(callLogDoc?.campaign_id);
    if (campaignId && /^[a-f0-9]{24}$/i.test(campaignId)) {
        campaignDoc = await db.collection("campaigns").findOne({ _id: new ObjectId(campaignId) });
    }

    const payload = buildAnalysisPayload({ callLogDoc, campaignDoc: campaignDoc || {} });
    if (!payload) {
        return { payload: null, reason: "conversation_turns_missing", callLogDoc };
    }
    return { payload, reason: null, callLogDoc };
}

async function triggerCallAnalysis(callId, options = {}) {
    const { deferIfNoTurns = false } = options;

    if (process.env.ONDIAL_TRIGGER_ANALYSIS_ENABLED === "0") {
        return { triggered: false, reason: "disabled" };
    }

    const id = String(callId || "").trim();
    if (!id || id.startsWith("call_")) {
        return { triggered: false, reason: "invalid_call_id" };
    }

    const lockKey = id;
    if (!(await acquireAnalysisTriggerLock(lockKey))) {
        logger.info("[Analysis] Trigger skipped — already in progress or recently done", { callId: id });
        return { triggered: false, reason: "analysis_trigger_locked" };
    }

    let analysisCallId = id;
    let analysisUrl = buildAnalysisUrl(id);

    try {
        let fetchOptions = {
            method: "POST",
            headers: { "Content-Type": "application/json" },
        };

        if (ANALYSIS_USE_BODY_PAYLOAD) {
            const { payload, reason, callLogDoc } = await resolveAnalysisPayload(id);
            if (!payload) {
                const turnCount =
                    (Array.isArray(callLogDoc?.conversation?.turns) ? callLogDoc.conversation.turns.length : 0) ||
                    (Array.isArray(callLogDoc?.twilio?.conversation?.turns)
                        ? callLogDoc.twilio.conversation.turns.length
                        : 0);
                if (deferIfNoTurns && reason === "conversation_turns_missing") {
                    logger.info("[Analysis] Deferred until conversation.turns are stored", {
                        callId: id,
                        turnCount,
                    });
                    return { triggered: false, reason, deferred: true };
                }
                logger.warn("[Analysis] Skipping analysis trigger: payload unresolved", {
                    callId: id,
                    reason,
                    turnCount,
                });
                return { triggered: false, reason: reason || "payload_unresolved" };
            }
            if (ANALYSIS_LOG_PAYLOAD) {
                logger.info("[Analysis] Request payload", {
                    callId: id,
                    payload: safeStringify(payload),
                });
            }
            fetchOptions.body = JSON.stringify(payload);
        } else {
            const callLogDoc = await loadCallLogDoc(id);
            analysisCallId = resolveLegacyAnalysisCallId(callLogDoc, id);
            analysisUrl = buildAnalysisUrl(analysisCallId);

            if (isTwilioCallSid(analysisCallId) && !isUuidCallId(analysisCallId)) {
                logger.warn("[Analysis] Legacy API cannot use Twilio CallSid alone", {
                    inputCallId: id,
                    analysisCallId,
                    hint: "Send call_unique_id via /api/twilio-mapping or /twilio/conversation, or set ANALYSIS_USE_BODY_PAYLOAD=1",
                });
                return { triggered: false, reason: "twilio_sid_not_supported_by_legacy_api" };
            }

            if (analysisCallId !== id) {
                logger.info("[Analysis] Resolved legacy analysis call id", {
                    inputCallId: id,
                    analysisCallId,
                });
            }
        }

        if (ANALYSIS_API_INITIAL_DELAY_MS > 0) {
            await new Promise((r) => setTimeout(r, ANALYSIS_API_INITIAL_DELAY_MS));
        }

        for (let attempt = 1; attempt <= ANALYSIS_API_MAX_ATTEMPTS; attempt++) {
            logger.info("[Analysis] Triggering analysis API", {
                callId: id,
                analysisCallId,
                attempt,
                analysisUrl,
                mode: ANALYSIS_USE_BODY_PAYLOAD ? "body_payload" : "legacy_url",
            });
            const res = await fetch(analysisUrl, fetchOptions);

            if (res.ok) {
                logger.info("[Analysis] Analysis API OK", { callId: id, attempt });
                return { triggered: true, attempt };
            }

            const errText = await res.text();
            const notReady =
                res.status === 404 ||
                res.status === 503 ||
                res.status === 502 ||
                /not\s*found/i.test(errText);

            if (notReady && attempt < ANALYSIS_API_MAX_ATTEMPTS) {
                await new Promise((r) => setTimeout(r, ANALYSIS_API_RETRY_MS));
                continue;
            }

            logger.warn("[Analysis] Analysis API failed", {
                callId: id,
                status: res.status,
                errText: errText.slice(0, 200),
            });
            return { triggered: false, reason: "api_error", status: res.status };
        }
    } catch (err) {
        logger.error("[Analysis] Analysis API error", { callId: id, error: err.message });
        return { triggered: false, reason: err.message };
    }

    return { triggered: false, reason: "exhausted_retries" };
}

module.exports = { triggerCallAnalysis };
