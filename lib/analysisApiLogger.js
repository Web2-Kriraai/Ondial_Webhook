const { randomUUID } = require("crypto");

const ANALYSIS_LOG_HTTP = process.env.ANALYSIS_LOG_HTTP !== "0";
const MAX_BODY_CHARS = Math.max(
    500,
    Number(process.env.ANALYSIS_LOG_MAX_BODY_CHARS || 12000)
);

function truncateText(value, maxChars = MAX_BODY_CHARS) {
    const text = String(value ?? "");
    if (text.length <= maxChars) return text;
    return `${text.slice(0, maxChars)}...[truncated ${text.length - maxChars} chars]`;
}

function sanitizeHeaders(headers = {}) {
    const out = {};
    for (const [key, value] of Object.entries(headers || {})) {
        const lower = String(key).toLowerCase();
        if (lower === "authorization" || lower === "x-api-key" || lower === "cookie") {
            out[key] = "[REDACTED]";
        } else {
            out[key] = value;
        }
    }
    return out;
}

function parseBodyForLog(body) {
    if (body == null || body === "") return null;
    if (typeof body === "object") {
        const serialized = safeStringify(body);
        if (serialized.length <= MAX_BODY_CHARS) {
            return body;
        }
        return truncateText(serialized);
    }
    const raw = String(body);
    try {
        const parsed = JSON.parse(raw);
        const serialized = safeStringify(parsed);
        if (serialized.length <= MAX_BODY_CHARS) {
            return parsed;
        }
        return truncateText(serialized);
    } catch {
        return truncateText(raw);
    }
}

function safeStringify(value) {
    try {
        return JSON.stringify(value);
    } catch (err) {
        return JSON.stringify({ stringify_error: err.message });
    }
}

function writeEvent(event, fields) {
    if (!ANALYSIS_LOG_HTTP) return;
    const line = JSON.stringify({
        ts: new Date().toISOString(),
        service: "ondial-webhook-analysis",
        event,
        ...fields,
    });
    if (event === "analysis_response_failed" || event === "analysis_request_error") {
        console.error(line);
    } else {
        console.log(line);
    }
}

function createAnalysisRequestId() {
    return randomUUID();
}

function logAnalysisRequest({ requestId, callId, attempt, url, method, headers, body }) {
    writeEvent("analysis_request", {
        requestId,
        callId: callId || null,
        attempt: attempt ?? null,
        method: method || "POST",
        url,
        headers: sanitizeHeaders(headers),
        body: parseBodyForLog(body),
    });
}

function logAnalysisResponse({ requestId, callId, attempt, url, status, durationMs, body }) {
    writeEvent("analysis_response", {
        requestId,
        callId: callId || null,
        attempt: attempt ?? null,
        url,
        status,
        durationMs,
        body: parseBodyForLog(body),
    });
}

function logAnalysisResponseFailed({ requestId, callId, attempt, url, status, durationMs, body, error }) {
    writeEvent("analysis_response_failed", {
        requestId,
        callId: callId || null,
        attempt: attempt ?? null,
        url,
        status: status ?? null,
        durationMs: durationMs ?? null,
        error: error || null,
        body: parseBodyForLog(body),
    });
}

function logAnalysisRequestError({ requestId, callId, attempt, url, durationMs, error }) {
    writeEvent("analysis_request_error", {
        requestId,
        callId: callId || null,
        attempt: attempt ?? null,
        url,
        durationMs: durationMs ?? null,
        error: error || null,
    });
}

module.exports = {
    createAnalysisRequestId,
    logAnalysisRequest,
    logAnalysisResponse,
    logAnalysisResponseFailed,
    logAnalysisRequestError,
};
