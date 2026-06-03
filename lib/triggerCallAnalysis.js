/**
 * POST to analysis service so Mongo `call_analysis` is populated for campaign analytics UI.
 */
const logger = require("../logger");
const { getDb } = require("../db");
const { ObjectId } = require("mongodb");

const ANALYSIS_API_MAX_ATTEMPTS = Number(process.env.ANALYSIS_API_MAX_ATTEMPTS || 5);
const ANALYSIS_API_RETRY_MS = Number(process.env.ANALYSIS_API_RETRY_MS || 3000);
const ANALYSIS_API_INITIAL_DELAY_MS = Number(process.env.ANALYSIS_API_INITIAL_DELAY_MS || 2000);
const ANALYSIS_LOG_PAYLOAD = process.env.ANALYSIS_LOG_PAYLOAD === "1";
const ANALYSIS_DEFAULT_SERVICE_NAME = String(process.env.ANALYSIS_DEFAULT_SERVICE_NAME || "sales").trim();
const ANALYSIS_DEFAULT_REASON = String(
    process.env.ANALYSIS_DEFAULT_REASON || "Analyze the call conversation."
).trim();

function buildAnalysisUrl(callId) {
    const raw = String(process.env.ANALYSIS_API_URL || "http://72.60.221.48:5000").replace(/\/$/, "");
    if (raw.includes("{CALL_ID}")) {
        return raw.replaceAll("{CALL_ID}", encodeURIComponent(String(callId)));
    }
    if (/\/analyze\/call\/[^/]+$/i.test(raw)) {
        return raw;
    }
    if (/\/analyze\/call$/i.test(raw)) {
        return raw;
    }
    if (raw.includes("/analyze/call")) {
        return `${raw}/${encodeURIComponent(String(callId))}`;
    }
    return `${raw}/analyze/call`;
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
            const entries = Object.entries(turn).filter(
                ([k, v]) => String(k || "").trim() !== "" && v != null && String(v).trim() !== ""
            );
            if (!entries.length) return null;
            const [speaker, text] = entries[0];
            return { [String(speaker).trim()]: String(text).trim() };
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

async function resolveAnalysisPayload(callId) {
    const db = getDb();
    const collectionName = process.env.CALLLOGS_COLLECTION || "CallLogs";
    const callLogDoc = await db
        .collection(collectionName)
        .findOne({ $or: [{ lead_id: callId }, { call_id: callId }] });

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

async function triggerCallAnalysis(callId) {
    if (process.env.ONDIAL_TRIGGER_ANALYSIS_ENABLED === "0") {
        return { triggered: false, reason: "disabled" };
    }

    const id = String(callId || "").trim();
    if (!id || id.startsWith("call_")) {
        return { triggered: false, reason: "invalid_call_id" };
    }

    const analysisUrl = buildAnalysisUrl(id);

    try {
        const { payload, reason, callLogDoc } = await resolveAnalysisPayload(id);
        if (!payload) {
            logger.warn("[Analysis] Skipping analysis trigger: payload unresolved", {
                callId: id,
                reason,
                hasConversation: Array.isArray(callLogDoc?.conversation?.turns),
            });
            return { triggered: false, reason: reason || "payload_unresolved" };
        }
        if (ANALYSIS_LOG_PAYLOAD) {
            logger.info("[Analysis] Request payload", {
                callId: id,
                payload: safeStringify(payload),
            });
        }

        if (ANALYSIS_API_INITIAL_DELAY_MS > 0) {
            await new Promise((r) => setTimeout(r, ANALYSIS_API_INITIAL_DELAY_MS));
        }

        for (let attempt = 1; attempt <= ANALYSIS_API_MAX_ATTEMPTS; attempt++) {
            logger.info("[Analysis] Triggering analysis API", { callId: id, attempt, analysisUrl });
            const res = await fetch(analysisUrl, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });

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
