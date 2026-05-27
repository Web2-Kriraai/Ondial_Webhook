/**
 * POST to analysis service so Mongo `call_analysis` is populated for campaign analytics UI.
 */
const logger = require("../logger");

const ANALYSIS_API_MAX_ATTEMPTS = Number(process.env.ANALYSIS_API_MAX_ATTEMPTS || 5);
const ANALYSIS_API_RETRY_MS = Number(process.env.ANALYSIS_API_RETRY_MS || 3000);
const ANALYSIS_API_INITIAL_DELAY_MS = Number(process.env.ANALYSIS_API_INITIAL_DELAY_MS || 2000);

function buildAnalysisUrl(callId) {
    const raw = String(process.env.ANALYSIS_API_URL || "http://72.60.221.48:5000").replace(/\/$/, "");
    if (raw.includes("/analyze/call")) {
        return `${raw}/${encodeURIComponent(String(callId))}`;
    }
    return `${raw}/analyze/call/${encodeURIComponent(String(callId))}`;
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
        if (ANALYSIS_API_INITIAL_DELAY_MS > 0) {
            await new Promise((r) => setTimeout(r, ANALYSIS_API_INITIAL_DELAY_MS));
        }

        for (let attempt = 1; attempt <= ANALYSIS_API_MAX_ATTEMPTS; attempt++) {
            logger.info("[Analysis] Triggering analysis API", { callId: id, attempt, analysisUrl });
            const res = await fetch(analysisUrl, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
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
