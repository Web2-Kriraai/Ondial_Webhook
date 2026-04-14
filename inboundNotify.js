const https = require("https");
const http = require("http");
const { URL } = require("url");
const { getRedis } = require("./redis");
const { normalizeCallId } = require("./callMapping");
const logger = require("./logger");

/** Outbound notify target (POST JSON body: `{ "call_id": "<string>" }` only). */
const DEFAULT_URL = "https://www.ondial.ai/api/inbound-webhook";

const DEDUPE_TTL_SEC = Math.ceil(
    Number(process.env.ONDIAL_INBOUND_NOTIFY_DEDUPE_TTL_MS || 24 * 60 * 60 * 1000) / 1000
);
const TIMEOUT_MS = Number(process.env.ONDIAL_INBOUND_NOTIFY_TIMEOUT_MS || 15000);
const MAX_SOCKETS = Number(process.env.ONDIAL_INBOUND_NOTIFY_MAX_SOCKETS || 50);

const httpsAgent = new https.Agent({
    keepAlive: true,
    maxSockets: MAX_SOCKETS,
});

const httpAgent = new http.Agent({
    keepAlive: true,
    maxSockets: MAX_SOCKETS,
});

function isDisabled() {
    const v = process.env.ONDIAL_INBOUND_NOTIFY_ENABLED;
    return v === "0" || v === "false";
}

function resolveTargetUrl() {
    return (process.env.ONDIAL_INBOUND_NOTIFY_URL || DEFAULT_URL).trim();
}

function allowHttp() {
    return process.env.ONDIAL_INBOUND_ALLOW_HTTP === "1" || process.env.ONDIAL_INBOUND_ALLOW_HTTP === "true";
}

/**
 * Bearer token for `Authorization: Bearer <token>` (set in env; never logged).
 * Use the raw token only — do not include the "Bearer " prefix in the value.
 */
function resolveBearerToken() {
    const raw =
        process.env.ONDIAL_INBOUND_NOTIFY_BEARER_TOKEN || process.env.ONDIAL_INBOUND_BEARER_TOKEN || "";
    const t = String(raw).trim();
    if (!t) return null;
    if (/^bearer\s+/i.test(t)) {
        return t.replace(/^bearer\s+/i, "").trim() || null;
    }
    return t;
}

/**
 * POST only `{ call_id }` as JSON. No other fields.
 */
function buildPayload(call_id) {
    return JSON.stringify({ call_id: String(call_id) });
}

function postJson(urlString, bodyString) {
    return new Promise((resolve, reject) => {
        const url = new URL(urlString);
        const isHttps = url.protocol === "https:";
        const lib = isHttps ? https : http;
        const agent = isHttps ? httpsAgent : httpAgent;

        const headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Content-Length": Buffer.byteLength(bodyString),
            "User-Agent": "Ondial-Webhook/inbound-notify",
            Connection: "keep-alive",
        };

        const bearer = resolveBearerToken();
        if (bearer) {
            headers.Authorization = `Bearer ${bearer}`;
        }

        const req = lib.request(
            {
                hostname: url.hostname,
                port: url.port || (isHttps ? 443 : 80),
                path: url.pathname + url.search,
                method: "POST",
                agent,
                headers,
                timeout: TIMEOUT_MS,
            },
            (res) => {
                res.resume();
                res.on("end", () => resolve(res.statusCode || 0));
            }
        );

        req.on("timeout", () => {
            req.destroy(new Error("Request timeout"));
        });
        req.on("error", reject);
        req.write(bodyString);
        req.end();
    });
}

async function releaseDedupeKey(key) {
    if (!key) return;
    try {
        await getRedis().del(key);
    } catch {
        /* ignore */
    }
}

/**
 * Notify ondial.ai once per normalized call_id when an inbound telephony webhook is processed.
 * Idempotent via Redis SET NX; failed HTTP attempts release the key for retry on a later event/CDR.
 */
async function notifyOndialInboundWebhook(callIdRaw) {
    let dedupeRedisKey = null;

    try {
        if (isDisabled()) return;

        const call_id = normalizeCallId(callIdRaw);
        if (!call_id) return;

        const urlString = resolveTargetUrl();
        let parsed;
        try {
            parsed = new URL(urlString);
        } catch {
            logger.error("[InboundNotify] Invalid ONDIAL_INBOUND_NOTIFY_URL");
            return;
        }

        if (parsed.protocol === "http:" && !allowHttp()) {
            logger.error("[InboundNotify] Refusing non-HTTPS URL (set ONDIAL_INBOUND_ALLOW_HTTP=1 for dev only)");
            return;
        }

        dedupeRedisKey = `notify:ondial:inbound:${call_id}`;
        const redis = getRedis();
        const reserved = await redis.set(dedupeRedisKey, "1", "EX", DEDUPE_TTL_SEC, "NX");
        if (reserved !== "OK") return;

        const bodyString = buildPayload(call_id);

        const status = await postJson(urlString, bodyString);
        if (status < 200 || status >= 300) {
            logger.warn("[InboundNotify] Remote returned non-success status", {
                status,
                call_id,
            });
            await releaseDedupeKey(dedupeRedisKey);
            return;
        }

        logger.info("[InboundNotify] POST ok", { call_id, status });
    } catch (err) {
        logger.error("[InboundNotify] Failed", { error: err.message });
        await releaseDedupeKey(dedupeRedisKey);
    }
}

module.exports = { notifyOndialInboundWebhook };
