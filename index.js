require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { connectDB } = require("./db");
const { connectRedis } = require("./redis");
const { registerCallMapping } = require("./callMapping");
const { createCallLog, INBOUNDCALLLOG_COLLECTION } = require("./callLogs");
const logger = require("./logger");
const callEvents = require("./events");
const { enqueueWebhook, startWebhookWorkers, closeWebhookWorkers } = require("./webhookQueue");
const { logMissingCallMapping, previewPayload } = require("./errorLog");
const { resolveInboundMapping } = require("./inboundMapping");
const crypto = require("crypto");

const app = express();
const PORT = process.env.PORT || 9000;

app.use(cors());
app.use(express.json({ limit: process.env.REQUEST_BODY_LIMIT || "1mb" }));
app.use(express.urlencoded({ extended: true, limit: process.env.REQUEST_BODY_LIMIT || "1mb" }));

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

    res.status(200).json({ received: true });

    const body = req.body;

    logger.info("Webhook received", {
        method: req.method,
        url: req.originalUrl,
        ip: req.ip,
        payloadType: body && body.event ? "event" : body && body.Call_UniqueId ? "summary" : "unknown",
    });

    enqueueWebhook(body).catch((err) => {
        logger.error("Webhook enqueue error", { error: err.message });
    });
});

// Inbound: call_id + campaign_id required; identify contact via contact_id and/or lead_id and/or from_number
app.post("/api/inbound-mapping", async (req, res) => {
    res.status(200).json({ received: true });

    const {
        call_type,
        call_id,
        from_number,
        to_number,
        campaign_id,
        lead_id,
        contact_id,
    } = req.body;

    const hasContactId = contact_id != null && String(contact_id).trim() !== "";
    const hasFrom = from_number != null && String(from_number).trim() !== "";
    const hasLead = lead_id != null && String(lead_id).trim() !== "";

    logger.info("Inbound mapping received", {
        call_type,
        call_id,
        campaign_id,
        hasContactId,
        hasFrom,
        hasLead,
    });

    if (call_type != null && String(call_type).toLowerCase() !== "inbound") {
        logger.info("[InboundMapping] Skipping — not an inbound call_type", { call_type });
        return;
    }

    if (!call_id || !campaign_id || (!hasContactId && !hasFrom && !hasLead)) {
        logger.warn(
            "[InboundMapping] Missing call_id, campaign_id, or any of contact_id / from_number / lead_id — skipping",
            req.body
        );
        await logMissingCallMapping({
            source: "inbound_mapping_endpoint",
            reason: "missing_call_id_campaign_or_identity",
            body_preview: previewPayload(req.body),
        });
        return;
    }

    try {
        const resolved = await resolveInboundMapping({
            lead_id,
            contact_id,
            from_number,
            campaign_id,
        });
        if (!resolved) {
            logger.warn("[InboundMapping] No contactprocessings match for from_number + campaign_id", {
                from_number,
                campaign_id,
            });
            await logMissingCallMapping({
                source: "inbound_mapping_endpoint",
                reason: "no_contact_for_caller_and_campaign",
                call_id,
                from_number,
                to_number: to_number ?? null,
                campaign_id,
                body_preview: previewPayload(req.body),
            });
            return;
        }

        await registerCallMapping({
            lead_id: "",
            call_id,
            campaign_id,
            contact_id: resolved.contact_id,
            phone: resolved.normalizedPhone,
            collectionName: INBOUNDCALLLOG_COLLECTION,
        });
        await createCallLog({
            call_id,
            campaign_id,
            contact_id: resolved.contact_id,
            collectionName: INBOUNDCALLLOG_COLLECTION,
        });
        logger.info("[InboundMapping] Stored mapping", {
            call_id,
            contact_id: resolved.contact_id,
        });
    } catch (err) {
        logger.error("[InboundMapping] Failed to store mapping", { error: err.message });
    }
});


// ─── Health Check ─────────────────────────────────────────────────────────────
app.get("/health", (req, res) => {
    res.json({ status: "ok", time: new Date().toISOString() });
});

// ─── Start Server ─────────────────────────────────────────────────────────────
async function start() {
    try {
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