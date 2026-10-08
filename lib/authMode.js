/**
 * Wave 6 — AUTH_MODE_<GROUP>=off|log_only|enforce
 * Groups: sse, twilio_status, mapping, conversation, inbound_mapping, telnyx_webhooks, frejun_status
 * Default: off (no behavior change). Staging examples use log_only.
 */
const crypto = require("crypto");
const logger = require("../logger");

const GROUPS = [
    "sse",
    "twilio_status",
    "mapping",
    "conversation",
    "inbound_mapping",
    "telnyx_webhooks",
    "frejun_status",
];

function getAuthMode(group) {
    const key = `AUTH_MODE_${String(group || "").toUpperCase()}`;
    const v = String(process.env[key] || "off").trim().toLowerCase();
    if (v === "log_only" || v === "logonly" || v === "log") return "log_only";
    if (v === "enforce" || v === "on" || v === "1") return "enforce";
    return "off";
}

function hasMatchingApiKey(req, envNames = []) {
    const sent = String(req.headers["x-api-key"] || req.headers["x-apikey"] || "").trim();
    if (!sent) return false;
    for (const name of envNames) {
        const expected = String(process.env[name] || "").trim();
        if (expected && sent === expected) return true;
    }
    return false;
}

/**
 * Extend verify result: also accept CS1 X-API-Key headers.
 * Does not enforce by itself — callers use applyAuthModeGate.
 */
function verifyIngressAuthWithApiKeys(verifyIngressAuthFn, req, opts = {}) {
    if (typeof verifyIngressAuthFn === "function" && verifyIngressAuthFn(req, opts)) {
        return { ok: true, via: "ingress" };
    }
    const apiKeyEnvs = opts.apiKeyEnvs || [
        "OUTBOUND_CALL_MAPPING_API_KEY",
        "TWILIO_MAPPING_API_KEY",
        "TELNYX_MAPPING_API_KEY",
        "FREJUN_MAPPING_API_KEY",
        "WEBHOOK_SHARED_SECRET",
    ];
    if (hasMatchingApiKey(req, apiKeyEnvs)) {
        return { ok: true, via: "x-api-key" };
    }
    return { ok: false, reason: "no_valid_auth" };
}

/**
 * @returns {boolean} true if request should be rejected (enforce mode only)
 */
function applyAuthModeGate({ group, ok, reason, req, res }) {
    const mode = getAuthMode(group);
    if (mode === "off") return false;
    if (ok) return false;
    const payload = {
        group,
        mode,
        reason: reason || "would_deny",
        path: req?.path || req?.url || null,
        method: req?.method || null,
    };
    if (mode === "log_only") {
        logger.warn("[AuthMode] would deny", payload);
        return false;
    }
    // enforce
    logger.warn("[AuthMode] deny", payload);
    if (res && !res.headersSent) {
        res.status(401).json({ error: "unauthorized", group, reason: reason || "auth_failed" });
    }
    return true;
}

function signSseToken({ campaignId, userId, secret, ttlSec = 300 }) {
    const exp = Math.floor(Date.now() / 1000) + ttlSec;
    const body = Buffer.from(
        JSON.stringify({ campaignId: String(campaignId || ""), userId: String(userId || ""), exp }),
        "utf8"
    ).toString("base64url");
    const sig = crypto.createHmac("sha256", secret).update(body).digest("base64url");
    return `${body}.${sig}`;
}

function verifySseToken(token, { secret, campaignId }) {
    try {
        const [body, sig] = String(token || "").split(".");
        if (!body || !sig) return { ok: false, reason: "malformed" };
        const expect = crypto.createHmac("sha256", secret).update(body).digest("base64url");
        if (expect !== sig) return { ok: false, reason: "bad_sig" };
        const payload = JSON.parse(Buffer.from(body, "base64url").toString("utf8"));
        if (!payload.exp || payload.exp < Math.floor(Date.now() / 1000)) {
            return { ok: false, reason: "expired" };
        }
        if (campaignId && String(payload.campaignId) !== String(campaignId)) {
            return { ok: false, reason: "campaign_mismatch" };
        }
        if (!payload.campaignId) return { ok: false, reason: "empty_campaign" };
        return { ok: true, payload };
    } catch (err) {
        return { ok: false, reason: err?.message || "verify_error" };
    }
}

module.exports = {
    GROUPS,
    getAuthMode,
    hasMatchingApiKey,
    verifyIngressAuthWithApiKeys,
    applyAuthModeGate,
    signSseToken,
    verifySseToken,
};
