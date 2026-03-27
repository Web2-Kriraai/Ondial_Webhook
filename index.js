require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { connectDB } = require("./db");
const { handleWebhook } = require("./webhookHandler");
const { registerCallMapping } = require("./callMapping");
const { createCallLog } = require("./callLogs");
const logger = require("./logger");
const callEvents = require("./events");

const app = express();
const PORT = process.env.PORT || 9000;

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

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
    res.status(200).json({ received: true });

    const { lead_id, call_id, campaign_id, contact_id } = req.body;

    logger.info("Call mapping received", { lead_id, call_id, campaign_id, contact_id });

    if (!call_id || !contact_id) {
        logger.warn("[CallMapping] Missing call_id or contact_id — skipping", req.body);
        return;
    }

    try {
        registerCallMapping({ lead_id, call_id, campaign_id, contact_id });
        // Create calllogs document immediately so events can be appended as they arrive
        createCallLog({ lead_id, call_id, campaign_id, contact_id }).catch((err) => {
            logger.error("[CallLog] createCallLog failed", { error: err.message });
        });
    } catch (err) {
        logger.error("[CallMapping] Failed to store mapping", { error: err.message });
    }
});

// ─── FLOW 3: Telephony Webhook Endpoint ──────────────────────────────────────
// Receives all webhook events from telephony provider.
app.post("/api/v1/webhooks/receiver", async (req, res) => {
    res.status(200).json({ received: true });

    const body = req.body;

    logger.info("Webhook received", {
        method: req.method,
        url: req.originalUrl,
        ip: req.ip,
        body,
    });

    handleWebhook(body).catch((err) => {
        logger.error("Webhook handler error", { error: err.message, body });
    });
});

// ─── Health Check ─────────────────────────────────────────────────────────────
app.get("/health", (req, res) => {
    res.json({ status: "ok", time: new Date().toISOString() });
});

// ─── Start Server ─────────────────────────────────────────────────────────────
async function start() {
    try {
        await connectDB();
        app.listen(PORT, "0.0.0.0", () => {
            logger.info(`Server started on port ${PORT}`);
        });
    } catch (err) {
        logger.error("Failed to start server", { error: err.message });
        process.exit(1);
    }
}

start();