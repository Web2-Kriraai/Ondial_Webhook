require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { connectDB } = require("./db");
const { connectRedis } = require("./redis");
const { registerCallMapping, normalizePhone } = require("./callMapping");
const { createCallLog, INBOUNDCALLLOG_COLLECTION } = require("./callLogs");
const logger = require("./logger");
const callEvents = require("./events");
const { enqueueWebhook, startWebhookWorkers, closeWebhookWorkers, getQueueLagSnapshot } = require("./webhookQueue");
const { logMissingCallMapping, previewPayload } = require("./errorLog");
const crypto = require("crypto");

const app = express();
const PORT = process.env.PORT || 9000;
const MAX_WEBHOOK_SKEW_MS = Number(process.env.WEBHOOK_MAX_SKEW_MS || 5 * 60 * 1000);

app.use(cors());
app.use(express.json({
    limit: process.env.REQUEST_BODY_LIMIT || "1mb",
    verify: (req, _res, buf) => {
        req.rawBody = Buffer.from(buf || Buffer.alloc(0));
    }
}));
app.use(express.urlencoded({ 
    extended: true, 
    limit: process.env.REQUEST_BODY_LIMIT || "1mb",
    verify: (req, _res, buf) => {
        if (!req.rawBody) req.rawBody = Buffer.from(buf || Buffer.alloc(0));
    }
}));

function timingSafeEqualHex(a, b) {
    try {
        const aa = Buffer.from(String(a || ""), "hex");
        const bb = Buffer.from(String(b || ""), "hex");
        if (!aa.length || aa.length !== bb.length) return false;
        return crypto.timingSafeEqual(aa, bb);
    } catch {
        return false;
    }
}

function hasValidSharedSecret(req, secret, { allowBearer = true } = {}) {
    const direct = req.headers["x-webhook-secret"];
    const bearer = allowBearer ? (req.headers["authorization"] || "").replace(/^Bearer\s+/i, "") : null;

    if (!direct && !bearer) {
        logger.debug("[Auth] SharedSecret check failed: No secret headers found");
        return false;
    }

    const check = (val, type) => {
        if (!val) return false;
        try {
            const hashA = crypto.createHash("sha256").update(String(val)).digest();
            const hashB = crypto.createHash("sha256").update(String(secret)).digest();
            const match = crypto.timingSafeEqual(hashA, hashB);
            if (!match) {
                logger.warn(`[Auth] SharedSecret mismatch for ${type}`, {
                    providedLength: val.length,
                    expectedLength: secret.length
                });
            }
            return match;
        } catch (err) {
            logger.error(`[Auth] SharedSecret comparison error for ${type}`, { error: err.message });
            return false;
        }
    };

    const isDirectMatch = check(direct, "x-webhook-secret");
    if (isDirectMatch) return true;

    const isBearerMatch = allowBearer ? check(bearer, "Authorization Bearer") : false;
    return isBearerMatch;
}

function hasValidHmac(req, hmacSecret) {
    const tsRaw = req.headers["x-webhook-timestamp"];
    const sigRaw = req.headers["x-webhook-signature"];
    if (!tsRaw || !sigRaw) {
        logger.debug("[Auth] HMAC check failed: Missing timestamp or signature headers");
        return false;
    }
    const tsMs = Number(tsRaw);
    if (!Number.isFinite(tsMs)) {
        logger.warn("[Auth] HMAC check failed: Invalid timestamp", { tsRaw });
        return false;
    }
    const skew = Math.abs(Date.now() - tsMs);
    if (skew > MAX_WEBHOOK_SKEW_MS) {
        logger.warn("[Auth] HMAC check failed: Timestamp skew too large", { skew, max: MAX_WEBHOOK_SKEW_MS });
        return false;
    }
    const rawBody = req.rawBody ? req.rawBody.toString("utf8") : JSON.stringify(req.body || {});
    const expected = crypto.createHmac("sha256", hmacSecret).update(`${tsMs}.${rawBody}`).digest("hex");
    const match = timingSafeEqualHex(String(sigRaw), expected);
    if (!match) {
        logger.warn("[Auth] HMAC signature mismatch");
    }
    return match;
}

function verifyIngressAuth(req, { allowHmac = true, allowBearer = true, secretEnv = "WEBHOOK_SHARED_SECRET" } = {}) {
    const sharedSecret = process.env[secretEnv] || "";
    const hmacSecret = process.env.WEBHOOK_HMAC_SECRET || "";
    const isProd = process.env.NODE_ENV === "production";

    // In development, allow running without configured secrets.
    if (!isProd && !sharedSecret && !hmacSecret) {
        logger.info("[Auth] Development mode: No secrets configured, allowing access");
        return true;
    }

    if (sharedSecret && hasValidSharedSecret(req, sharedSecret, { allowBearer })) {
        console.log(">>> [AUTH SUCCESS] Valid Shared Secret");
        return true;
    }
    if (allowHmac && hmacSecret && hasValidHmac(req, hmacSecret)) {
        console.log(">>> [AUTH SUCCESS] Valid HMAC Signature");
        return true;
    }

    console.log(">>> [AUTH FAILED] No valid authentication found");
    logger.warn("[Auth] All ingress authentication methods failed", {
        headers: {
            "x-webhook-secret": req.headers["x-webhook-secret"] ? "present" : "missing",
            "authorization": req.headers["authorization"] ? "present" : "missing",
            "x-webhook-signature": req.headers["x-webhook-signature"] ? "present" : "missing"
        }
    });
    return false;
}

// ─── FLOW 1: Server-Sent Events (SSE) Endpoint ──────────────────────────────
app.get("/api/v1/sse/listen", (req, res) => {
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders();

    const campaignId = req.query.campaignId;

    const onCallUpdate = (data) => {
        if (campaignId && data.campaign_id && data.campaign_id !== campaignId) {
            return;
        }
        res.write(`data: ${JSON.stringify(data)}\n\n`);
    };

    callEvents.on("call_update", onCallUpdate);

    // Keep-alive ping every 30 seconds
    const pingInterval = setInterval(() => {
        res.write(": ping\n\n");
    }, 30000);

    req.on("close", () => {
        callEvents.off("call_update", onCallUpdate);
        clearInterval(pingInterval);
    });
});


// ─── FLOW 2: Call Mapping Endpoint ───────────────────────────────────────────
// Receives: { lead_id, call_id, campaign_id, contact_id }
// Called automatically by the telephony server ~1-2s after call is initiated.
app.post("/api/call-mapping", async (req, res) => {
    // if (!verifyIngressAuth(req, { allowHmac: false, secretEnv: "WEBHOOK_INTERNAL_SECRET" })) {
    //     return res.status(401).json({ received: false, error: "unauthorized_mapping_ingress" });
    // }
    res.status(200).json({ received: true });

    const { lead_id, call_id, campaign_id, contact_id } = req.body;

    logger.info("Call mapping received", { lead_id, call_id, campaign_id, contact_id });

    if (!call_id || !contact_id) {
        logger.warn("[CallMapping] Missing call_id or contact_id — skipping", req.body);
        await logMissingCallMapping({
            source: "call_mapping_endpoint",
            reason: "missing_call_id_or_contact_id",
            lead_id: lead_id ?? null,
            call_id: call_id ?? null,
            contact_id: contact_id ?? null,
            campaign_id: campaign_id ?? null,
            body_preview: previewPayload(req.body),
        });
        return;
    }

    try {
        await registerCallMapping({ lead_id, call_id, campaign_id, contact_id });
        // Create calllogs document immediately so events can be appended as they arrive
        await createCallLog({ lead_id, call_id, campaign_id, contact_id });
    } catch (err) {
        logger.error("[CallMapping] Failed to store mapping", { error: err.message });
    }
});

// ─── FLOW 3: Telephony Webhook Endpoint ──────────────────────────────────────
// Receives all webhook events from telephony provider.
app.post("/api/v1/webhooks/receiver", async (req, res) => {
    console.log(`\n--- Incoming Webhook Request from ${req.ip} ---`);
    if (!verifyIngressAuth(req, { allowHmac: false, allowBearer: false, secretEnv: "WEBHOOK_SHARED_SECRET" })) {
        console.log("--- [WEBHOOK REJECTED] 401 Unauthorized ---\n");
        return res.status(401).json({ received: false, queued: false, error: "unauthorized_webhook" });
    }
    console.log("--- [WEBHOOK AUTHORIZED] Processing... ---\n");
    const body = req.body;

    logger.info("Webhook received", {
        method: req.method,
        url: req.originalUrl,
        ip: req.ip,
        payloadType: body && body.event ? "event" : body && body.Call_UniqueId ? "summary" : "unknown",
    });

    try {
        const result = await enqueueWebhook(body, {
            eventId: req.headers["x-webhook-event-id"] || null,
            timestamp: req.headers["x-webhook-timestamp"] || null,
            requestId: req.headers["x-request-id"] || null,
        });
        return res.status(200).json({
            received: true,
            queued: true,
            duplicate: result?.duplicate === true,
        });
    } catch (err) {
        logger.error("Webhook enqueue error", { error: err.message });
        return res.status(503).json({
            received: false,
            queued: false,
            error: "webhook_queue_unavailable",
        });
    }
});

// Inbound: tie webhooks to Mongo by normalized call_id only (no contactprocessings lookup).
// Optional body fields: campaign_id, contact_id, from_number (phone index + CRM updates if set).
app.post("/api/inbound-mapping", async (req, res) => {
    // if (!verifyIngressAuth(req, { allowHmac: false, secretEnv: "WEBHOOK_INTERNAL_SECRET" })) {
    //     return res.status(401).json({ received: false, error: "unauthorized_inbound_mapping" });
    // }
    res.status(200).json({ received: true });

    const { call_type, call_id, from_number, campaign_id, contact_id } = req.body;

    logger.info("Inbound mapping received", {
        call_type,
        call_id,
        campaign_id,
        has_contact_id: contact_id != null && String(contact_id).trim() !== "",
    });

    if (call_type != null && String(call_type).toLowerCase() !== "inbound") {
        logger.info("[InboundMapping] Skipping — not an inbound call_type", { call_type });
        return;
    }

    if (!call_id || String(call_id).trim() === "") {
        logger.warn("[InboundMapping] Missing call_id — skipping", req.body);
        await logMissingCallMapping({
            source: "inbound_mapping_endpoint",
            reason: "missing_call_id",
            body_preview: previewPayload(req.body),
        });
        return;
    }

    const cid =
        contact_id != null && String(contact_id).trim() !== "" ? String(contact_id).trim() : "";
    const camp = campaign_id != null ? String(campaign_id) : "";

    try {
        await registerCallMapping({
            lead_id: "",
            call_id,
            campaign_id: camp,
            contact_id: cid,
            phone: normalizePhone(from_number) || "",
            collectionName: INBOUNDCALLLOG_COLLECTION,
        });
        await createCallLog({
            call_id,
            campaign_id: camp,
            contact_id: cid,
            collectionName: INBOUNDCALLLOG_COLLECTION,
        });
        logger.info("[InboundMapping] Stored mapping (call_id → inbound log)", {
            call_id,
            contact_id: cid || "(none)",
        });
    } catch (err) {
        logger.error("[InboundMapping] Failed to store mapping", { error: err.message });
    }
});


// ─── Health Check ─────────────────────────────────────────────────────────────
app.get("/health", (req, res) => {
    res.json({ status: "ok", time: new Date().toISOString() });
});

app.get("/health/slo", async (req, res) => {
    try {
        const queue = await getQueueLagSnapshot();
        return res.json({
            status: "ok",
            time: new Date().toISOString(),
            webhookQueue: queue,
        });
    } catch (error) {
        return res.status(500).json({
            status: "error",
            error: error.message,
        });
    }
});

// ─── Start Server ─────────────────────────────────────────────────────────────
async function start() {
    try {
        // if (process.env.NODE_ENV === "production") {
        //     const hasIngressSecret = Boolean(process.env.WEBHOOK_SHARED_SECRET || process.env.WEBHOOK_HMAC_SECRET);
        //     const hasInternalSecret = Boolean(process.env.WEBHOOK_INTERNAL_SECRET);
        //     if (!hasIngressSecret || !hasInternalSecret) {
        //         throw new Error("Missing webhook security secrets (WEBHOOK_SHARED_SECRET/WEBHOOK_HMAC_SECRET and WEBHOOK_INTERNAL_SECRET)");
        //     }
        // }
        await connectDB();
        await connectRedis();
        startWebhookWorkers().catch((err) => {
            logger.error("Failed to start webhook workers", { error: err.message });
            process.exit(1);
        });
        app.listen(PORT, "0.0.0.0", () => {
            logger.info(`Server started on port ${PORT}`);
        });
    } catch (err) {
        logger.error("Failed to start server", { error: err.message });
        process.exit(1);
    }
}

start();
setupShutdownHandlers();

function setupShutdownHandlers() {
    const shutdown = async (signal) => {
        logger.info("Shutdown signal received", { signal });
        try {
            await closeWebhookWorkers();
        } catch (err) {
            logger.error("Error closing webhook workers", { error: err.message });
        } finally {
            process.exit(0);
        }
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
}