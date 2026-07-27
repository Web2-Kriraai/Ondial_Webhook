require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { ObjectId } = require("mongodb");
const { connectDB, getDb } = require("./db");
const { connectRedis } = require("./redis");
const {
    registerCallMapping,
    lookupMapping,
    normalizePhone,
    normalizeCallId,
    registerTwilioCallSidMapping,
    lookupTwilioCallSidMapping,
    normalizeTwilioCallSid,
    registerTelnyxCallControlMapping,
    lookupTelnyxCallControlMapping,
    normalizeTelnyxCallControlId,
} = require("./callMapping");
const {
    createCallLog,
    INBOUNDCALLLOG_COLLECTION,
    resolveCollection,
    resolveOutboundCollection,
    buildTwilioStatusEvent,
    mergeTwilioStatusIntoCallLog,
    upsertTwilioAnchoredCallLog,
    buildTelnyxStatusEvent,
    mergeTelnyxStatusIntoCallLog,
    upsertTelnyxAnchoredCallLog,
} = require("./callLogs");
const logger = require("./logger");
const { subscribeCampaignDelta } = require("./lib/campaignDeltaRedisBus");
const { emitCallUpdateSse } = require("./events");
const { enqueueWebhook, startWebhookWorkers, closeWebhookWorkers, getQueueLagSnapshot } = require("./webhookQueue");
const { enqueueAisensyInbound, closeAisensyInboundQueue } = require("./aisensyInboundQueue");
const { verifyAisensySignature } = require("./lib/aisensySignature");
const { processAisensyMarketingWebhookSafe } = require("./lib/aisensyMarketingWebhook");
const { logMissingCallMapping, previewPayload } = require("./errorLog");
const { triggerCallAnalysis } = require("./lib/triggerCallAnalysis");
const { inferIsTestCallFromWebhookBody } = require("./lib/inferTestCall");
const { pickNonEmpty } = require("./lib/customParameters");
const { maybeDeductTwilioCallCredits } = require("./lib/twilioCallBilling");
const { maybeDeductTelnyxCallCredits } = require("./lib/telnyxCallBilling");
const { hangupCallControl, isTelnyxConfigured } = require("./lib/telnyxClient");
const { hangupTwilioCall, isTwilioConfigured } = require("./lib/twilioClient");
const { resolveTelephonyProvider } = require("./lib/resolveTelephonyProvider");
const {
    enrichBodyWithCarrierIds,
    pickDialerCallId,
    hasCarrierId,
} = require("./lib/resolveCarrierFromCallId");
const {
    mapTwilioCallStatusToReceiveStatus,
    resolveTwilioContactId,
    syncTwilioContactFromCall,
} = require("./lib/twilioContactSync");
const {
    mapTelnyxEventToStatus,
    resolveTelnyxContactId,
    syncTelnyxContactFromCall,
} = require("./lib/telnyxContactSync");
const {
    parseTelnyxWebhookBody,
    verifyTelnyxWebhookSignature,
    isTelnyxSignatureRequired,
    mapTelnyxEventToCallStatus,
    preferTelnyxStatus,
    isInformationalTelnyxEvent,
    durationSecFromTelnyxPayload,
    extractTelnyxRecordingUrl,
} = require("./lib/telnyxWebhookParse");
const { getRedis } = require("./redis");
const crypto = require("crypto");

const app = express();
const PORT = process.env.PORT || 9000;
const MAX_WEBHOOK_SKEW_MS = Number(process.env.WEBHOOK_MAX_SKEW_MS || 5 * 60 * 1000);

if (process.env.TRUST_PROXY === "1") {
    app.set("trust proxy", 1);
}

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

function cloneJsonSafe(value) {
    try {
        return JSON.parse(JSON.stringify(value ?? {}));
    } catch (err) {
        return { clone_error: err.message };
    }
}

function formatJsonPretty(value) {
    try {
        return JSON.stringify(value ?? {}, null, 2);
    } catch (err) {
        return JSON.stringify({ stringify_error: err.message });
    }
}

function extractTwilioCallSidFromBody(body) {
    return (
        normalizeTwilioCallSid(body?.CallSid || body?.call_sid || body?.twilio_call_sid) || null
    );
}

/** Logs Twilio webhook as nested JSON object + pretty multi-line block in PM2 output. */
function logTwilioWebhookEvent(req, label, body) {
    const event = cloneJsonSafe(body);
    const callSid = extractTwilioCallSidFromBody(body);
    const envelope = {
        method: req.method,
        url: req.originalUrl,
        ip: req.ip,
        callSid,
        event,
    };

    logger.info(label, envelope);
    console.log(`\n${label}\n${formatJsonPretty(envelope)}\n`);
}

function logTwilioEventData(label, data) {
    const event = cloneJsonSafe(data);
    logger.info(label, { event });
    console.log(`\n${label}\n${formatJsonPretty(event)}\n`);
}

function normalizeTwilioConversationTurn(raw, idx) {
    if (!raw || typeof raw !== "object") return null;
    const roleRaw = raw.role || raw.speaker || raw.from || "unknown";
    const role = String(roleRaw).trim().toLowerCase() || "unknown";
    const textRaw = raw.text || raw.content || raw.message || raw.utterance || "";
    const text = String(textRaw || "").trim();
    if (!text) return null;
    const tsRaw = raw.timestamp || raw.ts || raw.time || null;
    const ts = tsRaw ? new Date(tsRaw) : null;
    return {
        role,
        text,
        timestamp:
            ts && !Number.isNaN(ts.getTime()) ? ts.toISOString() : new Date(Date.now() + idx).toISOString(),
    };
}

function normalizeTwilioConversationPayload(body) {
    const turnsRaw = Array.isArray(body?.turns)
        ? body.turns
        : Array.isArray(body?.conversation)
          ? body.conversation
          : Array.isArray(body?.messages)
            ? body.messages
            : null;

    let turns = Array.isArray(turnsRaw)
        ? turnsRaw
              .map((row, idx) => normalizeTwilioConversationTurn(row, idx))
              .filter(Boolean)
        : [];

    const transcriptRaw =
        typeof body?.transcript === "string"
            ? body.transcript
            : typeof body?.conversation_text === "string"
              ? body.conversation_text
              : "";
    const transcript = String(transcriptRaw || "").trim();
    if (turns.length === 0 && transcript) {
        turns = [
            {
                role: "transcript",
                text: transcript,
                timestamp: new Date().toISOString(),
            },
        ];
    }
    return {
        turns: turns.slice(0, 500),
        transcript,
    };
}

function buildLegacyConversationShape({ turns, transcript, startTime, endTime }) {
    const legacyTurns = Array.isArray(turns)
        ? turns
              .map((t) => {
                  const role = String(t?.role || "").trim().toLowerCase();
                  const text = String(t?.text || "").trim();
                  if (!text) return null;
                  const speaker =
                      role === "assistant" || role === "ai" || role === "agent" || role === "bot"
                          ? "Agent"
                          : role === "customer" || role === "user" || role === "human"
                            ? "User"
                            : role || "User";
                  return { [speaker]: text };
              })
              .filter(Boolean)
        : [];

    const transcriptText =
        String(transcript || "").trim() ||
        legacyTurns
            .map((row) => {
                const [speaker] = Object.keys(row);
                return `${speaker}: ${row[speaker]}`;
            })
            .join(" ");

    const out = {
        turns: legacyTurns,
        transcript: transcriptText,
    };
    if (startTime) out.start_time = startTime;
    if (endTime) out.end_time = endTime;
    return out;
}

// ─── FLOW 1: Server-Sent Events (SSE) Endpoint ──────────────────────────────
app.get("/api/v1/sse/listen", (req, res) => {
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no");
    res.flushHeaders();

    const campaignId = String(req.query.campaignId || "").trim();

    res.write(`data: ${JSON.stringify({
        type: "sse.connected",
        channel: "call_update",
        ts: Date.now(),
    })}\n\n`);

    const onCallUpdate = (data) => {
        const eventCampaignId = String(
            data?.campaign_id || data?.campaignId || data?.campaign?._id || ""
        ).trim();
        if (campaignId && eventCampaignId && eventCampaignId !== campaignId) {
            return;
        }
        try {
            res.write(`data: ${JSON.stringify(data)}\n\n`);
        } catch {
            // client disconnected
        }
    };

    const unsubscribe = subscribeCampaignDelta(onCallUpdate);

    const pingInterval = setInterval(() => {
        try {
            res.write(": ping\n\n");
        } catch {
            // ignore
        }
    }, 25000);

    req.on("close", () => {
        unsubscribe();
        clearInterval(pingInterval);
    });
});


// NOTE: Legacy /api/call-mapping removed — use /api/outbound-call-mapping from the dialer worker.
// Telephony webhooks still carry customParameters when the provider echoes them; this endpoint
// seeds Redis when hangup/completed events omit contact_id.

// ─── Outbound call_unique_id mapping (worker → Redis) ─────────────────────────
app.post("/api/outbound-call-mapping", async (req, res) => {
    res.status(200).json({ received: true });

    const body = req.body || {};
    const callKey = normalizeCallId(body.call_unique_id || body.call_id);
    const contactId = body.contact_id != null ? String(body.contact_id).trim() : "";
    const campaignId = body.campaign_id != null ? String(body.campaign_id) : "";
    const phone = normalizePhone(body.phone || body.to || body.mobile);

    if (!callKey || !contactId) {
        logger.warn("[OutboundMapping] Missing call_unique_id or contact_id — skipping", {
            call_unique_id: body.call_unique_id || body.call_id || null,
            contact_id: contactId || null,
        });
        return;
    }

    const collectionName = await resolveOutboundCollection();

    await registerCallMapping({
        call_id: callKey,
        lead_id: body.lead_id != null ? String(body.lead_id) : callKey,
        campaign_id: campaignId,
        contact_id: contactId,
        phone: phone || "",
        collectionName,
        is_test_call: body?.is_test_call === true || body?.is_test_call === 'true',
    });

    logger.info("[OutboundMapping] Stored Redis mapping", {
        call_id: callKey,
        contact_id: contactId,
        campaign_id: campaignId || null,
        collection: collectionName,
    });
});

// ─── FLOW 2B: Twilio SID Mapping Endpoint ───────────────────────────────────
// Receives: { call_sid | twilio_call_sid | CallSid, call_id?, lead_id?, campaign_id, contact_id }
// Called by the Twilio call-creation service right after Twilio returns CallSid.
app.post("/api/twilio-mapping", async (req, res) => {
    // if (!verifyIngressAuth(req, { allowHmac: false, secretEnv: "WEBHOOK_INTERNAL_SECRET" })) {
    //     return res.status(401).json({ received: false, error: "unauthorized_twilio_mapping_ingress" });
    // }

    res.status(200).json({ received: true });

    const body = req.body || {};
    logTwilioWebhookEvent(req, "[Twilio] Mapping webhook payload", body);
    const { call_sid, twilio_call_sid, CallSid, call_id, lead_id, campaign_id, contact_id } = body;
    const sid = normalizeTwilioCallSid(call_sid || twilio_call_sid || CallSid);
    const callIdNorm = normalizeCallId(call_id);
    const leadIdStr = lead_id != null ? String(lead_id).trim() : "";

    logger.info("Twilio mapping received", {
        call_sid: sid || null,
        call_id: callIdNorm || null,
        lead_id: leadIdStr || null,
        campaign_id: campaign_id || null,
        contact_id: contact_id || null,
    });

    if (!sid) {
        logger.warn("[TwilioMapping] Missing twilio_call_sid — skipping", req.body);
        await logMissingCallMapping({
            source: "twilio_mapping_endpoint",
            reason: "missing_twilio_call_sid",
            contact_id: contact_id ?? null,
            campaign_id: campaign_id ?? null,
            body_preview: previewPayload(req.body),
        });
        return;
    }

    const contactId = contact_id != null ? String(contact_id) : "";
    const campaignId = campaign_id != null ? String(campaign_id) : "";
    const targetCollection = resolveCollection({ contact_id: contactId });

    try {
        await registerTwilioCallSidMapping({
            twilio_call_sid: sid,
            call_id: callIdNorm || "",
            lead_id: leadIdStr,
            campaign_id: campaignId,
            contact_id: contactId,
            collectionName: targetCollection,
            is_test_call: body?.is_test_call === true || body?.is_test_call === "true",
        });

        const setPayload = {
            "twilio.call_sid": sid,
            "twilio.mappedAt": new Date().toISOString(),
        };
        if (campaignId) setPayload["twilio.campaign_id"] = campaignId;
        if (contactId) setPayload["twilio.contact_id"] = contactId;

        const mappingEvent = {
            timestamp: new Date().toISOString(),
            event_type: "twilio_mapping_received",
            data: {
                twilio_call_sid: sid,
                campaign_id: campaignId || null,
                contact_id: contactId || null,
                lead_id: leadIdStr || null,
                call_id: callIdNorm || null,
            },
        };
        const anchored = await upsertTwilioAnchoredCallLog({
            collectionName: targetCollection,
            twilioCallSid: sid,
            twilioSetFields: setPayload,
            eventDoc: mappingEvent,
            rootFromMapping: {
                campaign_id: campaignId,
                contact_id: contactId,
                lead_id: leadIdStr,
                call_id: callIdNorm || "",
            },
        });

        logger.info("[TwilioMapping] Stored mapping", {
            twilio_call_sid: sid,
            contact_id: contactId || null,
            collection: targetCollection,
            callLogPerCallSid: anchored,
        });
    } catch (err) {
        logger.error("[TwilioMapping] Failed to store mapping", { error: err.message, twilio_call_sid: sid });
    }
});

// ─── Twilio Call Status Update Endpoint ──────────────────────────────────────
app.post("/twilio/call-status", async (req, res) => {
    // if (!verifyIngressAuth(req, { allowHmac: false, allowBearer: true, secretEnv: "WEBHOOK_SHARED_SECRET" })) {
    //     return res.status(401).json({ received: false, error: "unauthorized" });
    // }

    const body = req.body || {};
    logTwilioWebhookEvent(req, "[Twilio] Status webhook payload", body);

    const { CallSid, CallStatus, CallDuration, Timestamp } = body;
    const missingFields = [];
    if (!CallSid) missingFields.push("CallSid");
    if (!CallStatus) missingFields.push("CallStatus");
    if (!Timestamp) missingFields.push("Timestamp");
    if (missingFields.length) {
        return res.status(400).json({
            received: false,
            error: "missing_required_fields",
            missing: missingFields,
        });
    }

    const status = String(CallStatus).trim();
    const callSid = String(CallSid).trim();
    const timestampValue = (() => {
        if (typeof Timestamp === "number") return new Date(Timestamp);
        const raw = String(Timestamp).trim();
        if (/^[0-9]+$/.test(raw)) {
            const numeric = Number(raw);
            return new Date(numeric > 1e12 ? numeric : numeric * 1000);
        }
        return new Date(raw);
    })();

    if (!timestampValue || Number.isNaN(timestampValue.getTime())) {
        return res.status(400).json({
            received: false,
            error: "invalid_timestamp",
            details: "Timestamp must be a valid ISO 8601 string or Unix seconds/milliseconds value.",
        });
    }

    const duration = CallDuration == null ? null : Number(CallDuration);
    if (CallDuration != null && (!Number.isFinite(duration) || duration < 0)) {
        return res.status(400).json({
            received: false,
            error: "invalid_duration",
            details: "CallDuration must be a non-negative number.",
        });
    }

    const normalizedCallSid = callSid;
    const twilioMapping = await lookupTwilioCallSidMapping(normalizedCallSid);

    const twilioSetFields = {
        "twilio.call_sid": normalizedCallSid,
        "twilio.status": status,
        "twilio.timestamp": timestampValue.toISOString(),
        "twilio.updatedAt": new Date().toISOString(),
    };
    if (duration != null) {
        twilioSetFields["twilio.duration"] = duration;
    }
    if (status.toLowerCase() === "completed") {
        twilioSetFields["twilio.completedAt"] = timestampValue.toISOString();
    }
    if (inferIsTestCallFromWebhookBody(body) || twilioMapping?.is_test_call === true) {
        twilioSetFields.isTestCall = true;
    }

    const eventDoc = buildTwilioStatusEvent({
        CallSid: normalizedCallSid,
        CallStatus: status,
        CallDuration: duration,
        timestampIso: timestampValue.toISOString(),
    });

    const CALLLOGS_COLLECTION = process.env.CALLLOGS_COLLECTION || "CallLogs";
    const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";
    const INBOUNDCALLLOG_COLLECTION = process.env.INBOUNDCALLLOG_COLLECTION || "InboundConversation";
    const allCollections = [CALLLOGS_COLLECTION, TESTCALL_COLLECTION, INBOUNDCALLLOG_COLLECTION];

    const mappedCallId = twilioMapping?.call_id ? String(twilioMapping.call_id).trim() : "";
    const mappedCollection =
        twilioMapping?.collectionName && allCollections.includes(String(twilioMapping.collectionName))
            ? String(twilioMapping.collectionName)
            : null;
    const primaryCollection =
        mappedCollection || resolveCollection({ contact_id: twilioMapping?.contact_id }) || CALLLOGS_COLLECTION;
    const collectionsOrdered = [...new Set([primaryCollection, ...allCollections])];

    /** One doc per CallSid — never merge Twilio events into Indian CRM lead_id rows. */
    const filtersForCollection = () => [{ "twilio.call_sid": normalizedCallSid }];

    let updated = false;
    for (const collectionName of collectionsOrdered) {
        for (const filter of filtersForCollection()) {
            const ok = await mergeTwilioStatusIntoCallLog(
                collectionName,
                filter,
                twilioSetFields,
                eventDoc
            );
            if (ok) {
                updated = true;
                break;
            }
        }
        if (updated) break;
    }

    if (!updated) {
        const fallbackContactId = body.contact_id != null ? String(body.contact_id).trim() : "";
        const fallbackCampaignId = body.campaign_id != null ? String(body.campaign_id).trim() : "";
        const fallbackLeadId = body.lead_id != null ? String(body.lead_id).trim() : "";
        const fallbackCallId = body.call_id != null ? String(body.call_id).trim() : "";
        updated = await upsertTwilioAnchoredCallLog({
            collectionName: primaryCollection,
            twilioCallSid: normalizedCallSid,
            twilioSetFields,
            eventDoc,
            rootFromMapping: {
                campaign_id: twilioMapping?.campaign_id || fallbackCampaignId,
                contact_id: twilioMapping?.contact_id || fallbackContactId,
                lead_id: twilioMapping?.lead_id || fallbackLeadId,
                call_id: mappedCallId || fallbackCallId,
            },
        });
        if (updated) {
            logTwilioEventData("[Twilio] Call status anchored via upsert (no prior doc)", {
                CallSid: normalizedCallSid,
                CallStatus: status,
                CallDuration: duration,
                hadMapping: !!twilioMapping,
            });
        }
    }

    if (!updated) {
        logger.warn("[Twilio] Call status update could not be stored", {
            CallSid: normalizedCallSid,
            CallStatus: status,
            Timestamp,
            CallDuration,
        });
        return res.status(404).json({
            received: false,
            error: "call_record_not_found",
            callSid: normalizedCallSid,
        });
    }

    const contactForSync = resolveTwilioContactId({ twilioMapping, body });
    const contactSyncResult = contactForSync
        ? await syncTwilioContactFromCall({
              contactIdRaw: contactForSync,
              twilioStatus: status,
              callSid: normalizedCallSid,
              source: "twilio_call_status",
          })
        : { outcome: "skip_no_contact_id" };

    let creditResult = null;
    const statusCampaignId = pickNonEmpty(
        twilioMapping?.campaign_id,
        body?.campaign_id
    );
    // Conversation webhook usually carries campaign_id; avoid noisy no_campaign_id on status-only.
    if (
        status.toLowerCase() === "completed" &&
        duration != null &&
        duration > 0 &&
        statusCampaignId
    ) {
        creditResult = await maybeDeductTwilioCallCredits({
            callSid: normalizedCallSid,
            durationSec: duration,
            twilioMapping,
            body,
            collectionName: primaryCollection,
        });
    } else if (status.toLowerCase() === "completed" && duration > 0 && !statusCampaignId) {
        creditResult = { outcome: "defer_until_conversation" };
    }

    const statusContactId = pickNonEmpty(
        contactForSync,
        twilioMapping?.contact_id,
        body?.contact_id
    );
    const mappedReceiveStatus = mapTwilioCallStatusToReceiveStatus(status);

    emitCallUpdateSse({
        campaign_id: statusCampaignId || null,
        call_id: mappedCallId || normalizedCallSid,
        contact_id: statusContactId || null,
        status: mappedReceiveStatus,
        twilio_status: status,
        event: `twilio_${status.toLowerCase()}`,
        provider: "twilio",
    });

    logTwilioEventData("[Twilio] Call status updated", {
        CallSid: normalizedCallSid,
        CallStatus: status,
        CallDuration: duration,
        Timestamp: timestampValue.toISOString(),
        twilioSetFields,
        call_data_event: eventDoc,
        credit: creditResult,
        contactSync: contactSyncResult,
    });

    return res.status(200).json({
        received: true,
        updated: true,
        credit: creditResult,
        contactSync: contactSyncResult,
    });
});

// ─── Telnyx Call Control ID Mapping ───────────────────────────────────────────
// Receives: { call_control_id | telnyx_call_control_id | callControlId, call_id?, lead_id?, campaign_id, contact_id }
// Called by the dial worker right after Telnyx returns call_control_id — same contract as /api/twilio-mapping.
app.post("/api/telnyx-mapping", async (req, res) => {
    // Ack fast (Twilio mapping parity) — work continues after response.
    res.status(200).json({ received: true });

    const body = req.body || {};
    const sid = normalizeTelnyxCallControlId(
        body.call_control_id || body.telnyx_call_control_id || body.callControlId
    );
    const callIdNorm = normalizeCallId(body.call_id || body.call_unique_id || body.callUniqueId);
    const leadIdStr = body.lead_id != null ? String(body.lead_id).trim() : "";
    const contactId = body.contact_id != null ? String(body.contact_id).trim() : "";
    const campaignId = body.campaign_id != null ? String(body.campaign_id).trim() : "";

    logger.info("[TelnyxMapping] Mapping received", {
        call_control_id: sid || null,
        call_id: callIdNorm || null,
        lead_id: leadIdStr || null,
        campaign_id: campaignId || null,
        contact_id: contactId || null,
    });

    if (!sid) {
        logger.warn("[TelnyxMapping] Missing call_control_id — skipping", body);
        await logMissingCallMapping({
            source: "telnyx_mapping_endpoint",
            reason: "missing_call_control_id",
            contact_id: contactId || null,
            campaign_id: campaignId || null,
            body_preview: previewPayload(body),
        });
        return;
    }

    const targetCollection =
        (body.collectionName && String(body.collectionName).trim()) ||
        resolveCollection({ contact_id: contactId }) ||
        (await resolveOutboundCollection());

    try {
        // Store call_control_id → dialer call_id (do not invent call_id from sid).
        await registerTelnyxCallControlMapping({
            call_control_id: sid,
            call_id: callIdNorm || "",
            lead_id: leadIdStr,
            campaign_id: campaignId,
            contact_id: contactId,
            collectionName: targetCollection,
            is_test_call: body.is_test_call === true || body.isTestCall === true || body.is_test_call === "true",
        });

        const setPayload = {
            "telnyx.call_control_id": sid,
            "telnyx.status": "mapped",
            "telnyx.mappedAt": new Date().toISOString(),
            "telnyx.updatedAt": new Date().toISOString(),
        };
        if (campaignId) setPayload["telnyx.campaign_id"] = campaignId;
        if (contactId) setPayload["telnyx.contact_id"] = contactId;
        if (callIdNorm) {
            setPayload.call_unique_id = callIdNorm;
            setPayload["telnyx.external_call_id"] = callIdNorm;
        }
        if (inferIsTestCallFromWebhookBody(body) || body.is_test_call === true || body.isTestCall === true) {
            setPayload.isTestCall = true;
        }

        const eventDoc = buildTelnyxStatusEvent({
            callControlId: sid,
            eventType: "mapped",
            hangupCause: null,
            durationSec: null,
            timestampIso: new Date().toISOString(),
        });
        // Include dialer call_id in the event payload for debugging.
        eventDoc.data = {
            ...(eventDoc.data || {}),
            call_id: callIdNorm || null,
            lead_id: leadIdStr || null,
            campaign_id: campaignId || null,
            contact_id: contactId || null,
        };

        const anchored = await upsertTelnyxAnchoredCallLog({
            collectionName: targetCollection,
            callControlId: sid,
            telnyxSetFields: setPayload,
            eventDoc,
            rootFromMapping: {
                campaign_id: campaignId,
                contact_id: contactId,
                lead_id: leadIdStr,
                call_id: callIdNorm || "",
            },
        });

        logger.info("[TelnyxMapping] Stored mapping", {
            call_control_id: sid,
            call_id: callIdNorm || null,
            contact_id: contactId || null,
            collection: targetCollection,
            callLogAnchored: !!anchored,
        });
    } catch (err) {
        logger.error("[TelnyxMapping] Failed to store mapping", {
            error: err.message,
            call_control_id: sid,
        });
    }
});

/**
 * Deduplicate Telnyx webhook event ids (retries / concurrent delivery).
 * @returns {Promise<boolean>} true if this is the first time we see the event
 */
async function claimTelnyxWebhookEventId(eventId) {
    const id = String(eventId || "").trim();
    if (!id) return true;
    try {
        const redis = getRedis();
        const key = `telnyx:webhook:event:${id}`;
        const ttlSec = Math.max(3600, Number(process.env.TELNYX_WEBHOOK_EVENT_TTL_SEC || 86400) || 86400);
        const set = await redis.set(key, "1", "EX", ttlSec, "NX");
        return set === "OK";
    } catch (err) {
        logger.warn("[Telnyx] Event dedupe Redis failed — processing anyway", {
            event_id: id,
            error: err.message,
        });
        return true;
    }
}

/**
 * Process a parsed Telnyx Call Control webhook (CallLog + contact sync + billing).
 * Called after HTTP 2xx ack so Telnyx does not retry on slow work.
 */
async function processTelnyxCallControlWebhook(parsed, body) {
    const {
        eventType,
        eventId,
        occurredAtIso,
        payload,
        callLegId,
        callSessionId,
        connectionId,
        direction,
        hangupCause,
        hangupSource,
        from,
        to,
        deliveryAttempt,
    } = parsed;

    const callControlId = normalizeTelnyxCallControlId(parsed.callControlId);
    if (!callControlId) {
        logger.info("[Telnyx] Ignoring webhook without call_control_id", {
            event_type: eventType || null,
            event_id: eventId || null,
        });
        return { outcome: "skip_no_call_control_id" };
    }

    if (eventId) {
        const claimed = await claimTelnyxWebhookEventId(eventId);
        if (!claimed) {
            logger.info("[Telnyx] Duplicate webhook ignored", {
                event_id: eventId,
                event_type: eventType || null,
                call_control_id: callControlId,
            });
            return { outcome: "duplicate_event" };
        }
    }

    const telnyxMapping = await lookupTelnyxCallControlMapping(callControlId);
    const collectionName = await resolveOutboundCollection();
    const timestampIso = occurredAtIso || new Date().toISOString();

    const db = getDb();
    const existingDoc = await db.collection(collectionName).findOne(
        { "telnyx.call_control_id": callControlId },
        { projection: { "telnyx.answeredAt": 1, "telnyx.status": 1, campaign_id: 1, contact_id: 1 } }
    );

    const mappedStatus = mapTelnyxEventToCallStatus(eventType, { hangupCause });
    const nextStatus = mappedStatus
        ? preferTelnyxStatus(existingDoc?.telnyx?.status, mappedStatus)
        : existingDoc?.telnyx?.status || null;
    const status =
        nextStatus ||
        mappedStatus ||
        (isInformationalTelnyxEvent(eventType)
            ? existingDoc?.telnyx?.status || "unknown"
            : "unknown");

    const answeredAtIso =
        eventType === "call.answered"
            ? timestampIso
            : existingDoc?.telnyx?.answeredAt || null;

    const duration = durationSecFromTelnyxPayload(payload, answeredAtIso);

    const telnyxSetFields = {
        "telnyx.call_control_id": callControlId,
        "telnyx.updatedAt": new Date().toISOString(),
        "telnyx.last_event_type": eventType || null,
        "telnyx.last_event_id": eventId || null,
        "telnyx.timestamp": timestampIso,
    };
    if (status) {
        telnyxSetFields["telnyx.status"] = status;
        telnyxSetFields["telnyx.event_type"] = eventType || null;
    }
    if (callLegId) telnyxSetFields["telnyx.call_leg_id"] = callLegId;
    if (callSessionId) telnyxSetFields["telnyx.call_session_id"] = callSessionId;
    if (connectionId) telnyxSetFields["telnyx.connection_id"] = connectionId;
    if (direction) telnyxSetFields["telnyx.direction"] = direction;
    if (from) telnyxSetFields["telnyx.from"] = from;
    if (to) telnyxSetFields["telnyx.to"] = to;
    if (eventType === "call.answered") {
        telnyxSetFields["telnyx.answeredAt"] = timestampIso;
    }
    if (duration != null) telnyxSetFields["telnyx.duration"] = duration;
    if (hangupCause) telnyxSetFields["telnyx.hangup_cause"] = hangupCause;
    if (hangupSource) telnyxSetFields["telnyx.hangup_source"] = hangupSource;
    if (status === "completed" || status === "busy" || status === "no-answer" || status === "failed" || status === "canceled") {
        telnyxSetFields["telnyx.completedAt"] = timestampIso;
    }
    const recordingUrl = extractTelnyxRecordingUrl(payload);
    if (recordingUrl) {
        telnyxSetFields.recordingUrl = recordingUrl;
        telnyxSetFields["telnyx.recordingUrl"] = recordingUrl;
    }
    if (inferIsTestCallFromWebhookBody(body) || telnyxMapping?.is_test_call === true) {
        telnyxSetFields.isTestCall = true;
    }
    if (deliveryAttempt != null) {
        telnyxSetFields["telnyx.webhook_attempt"] = deliveryAttempt;
    }

    const eventDoc = buildTelnyxStatusEvent({
        callControlId,
        eventType: eventType || status,
        eventId,
        hangupCause,
        durationSec: duration,
        timestampIso,
    });

    const campaignId =
        telnyxMapping?.campaign_id || body.campaign_id || existingDoc?.campaign_id || null;
    const contactId =
        telnyxMapping?.contact_id || body.contact_id || existingDoc?.contact_id || null;
    // Dialer call_id from Redis mapping (Twilio CallSid → call_id parity).
    const mappedCallId = telnyxMapping?.call_id ? String(telnyxMapping.call_id).trim() : "";

    if (mappedCallId) {
        telnyxSetFields.call_unique_id = mappedCallId;
        telnyxSetFields["telnyx.external_call_id"] = mappedCallId;
    }

    await upsertTelnyxAnchoredCallLog({
        collectionName,
        callControlId,
        telnyxSetFields,
        eventDoc,
        rootFromMapping: {
            campaign_id: campaignId,
            contact_id: contactId,
            lead_id: telnyxMapping?.lead_id || "",
            call_id: mappedCallId || "",
        },
    });

    let contactSyncResult = { outcome: "skip_informational" };
    if (contactId && mappedStatus) {
        contactSyncResult = await syncTelnyxContactFromCall({
            contactIdRaw: contactId,
            eventType,
            status: mappedStatus,
            hangupCause,
            callControlId,
            source: "telnyx_webhooks",
        });
    } else if (!contactId) {
        contactSyncResult = { outcome: "skip_no_contact_id" };
    }

    let creditResult = { outcome: "skipped_not_hangup" };
    const isTerminalHangup =
        eventType === "call.hangup" ||
        status === "completed" ||
        status === "busy" ||
        status === "no-answer" ||
        status === "failed" ||
        status === "canceled";
    if (isTerminalHangup && eventType === "call.hangup") {
        creditResult = await maybeDeductTelnyxCallCredits({
            callControlId,
            durationSec: duration || 0,
            telnyxMapping,
            body: { ...body, ...payload, campaign_id: campaignId, contact_id: contactId },
            collectionName,
        });
    }

    const receiveStatus = mapTwilioCallStatusToReceiveStatus(status);
    emitCallUpdateSse({
        campaign_id: campaignId,
        call_id: mappedCallId || callControlId,
        contact_id: contactId,
        status: receiveStatus,
        event: eventType || `telnyx_${status}`,
        provider: "telnyx",
        telnyx_status: status,
        event_id: eventId || null,
    });

    return {
        outcome: "processed",
        event_type: eventType || null,
        status,
        credit: creditResult,
        contactSync: contactSyncResult,
    };
}

// ─── Telnyx Call Control webhooks (Mission Control webhook_event_url) ─────────
// Docs: https://developers.telnyx.com/development/api-fundamentals/webhooks/receiving-webhooks
// Must return 2xx within ~2s; heavy work runs after ack.
app.post("/telnyx/webhooks", async (req, res) => {
    const body = req.body || {};

    if (isTelnyxSignatureRequired()) {
        const sig = verifyTelnyxWebhookSignature(req);
        if (!sig.ok) {
            logger.warn("[Telnyx] Webhook signature rejected", { reason: sig.reason || null });
            // 2xx still? Prefer 401 so forged traffic is not treated as success.
            return res.status(401).json({ received: false, error: "invalid_signature", reason: sig.reason });
        }
    } else {
        const soft = verifyTelnyxWebhookSignature(req);
        if (!soft.skipped && !soft.ok) {
            logger.warn("[Telnyx] Webhook signature soft-fail (verify not required)", {
                reason: soft.reason || null,
            });
        }
    }

    const parsed = parseTelnyxWebhookBody(body);

    logger.info("[Telnyx] Webhook received", {
        event_type: parsed.eventType || null,
        event_id: parsed.eventId || null,
        call_control_id: parsed.callControlId || null,
        attempt: parsed.deliveryAttempt,
        direction: parsed.direction || null,
    });

    // Always ack immediately — Telnyx retries on non-2xx or >2s latency.
    res.status(200).json({
        received: true,
        event_type: parsed.eventType || null,
        event_id: parsed.eventId || null,
    });

    setImmediate(() => {
        processTelnyxCallControlWebhook(parsed, body).catch((err) => {
            logger.error("[Telnyx] Async webhook processing failed", {
                error: err.message,
                event_type: parsed.eventType || null,
                event_id: parsed.eventId || null,
                call_control_id: parsed.callControlId || null,
            });
        });
    });
});

/**
 * Hangup is a destructive control action: an unauthenticated caller who guesses or
 * observes a CallSid / call_control_id could drop live calls. Guarded with the same
 * ingress auth as the other internal endpoints (permissive in dev when no secret is set).
 */
function rejectUnauthorizedControlRequest(req, res) {
    if (verifyIngressAuth(req, { allowHmac: true, secretEnv: "WEBHOOK_INTERNAL_SECRET" })) {
        return false;
    }
    res.status(401).json({ error: "unauthorized" });
    return true;
}

/** Force hangup a Telnyx Call Control leg. */
async function handleTelnyxHangup(req, res) {
    if (rejectUnauthorizedControlRequest(req, res)) return;
    if (!isTelnyxConfigured()) {
        return res.status(503).json({ error: "Telnyx is not configured (TELNYX_API_KEY)" });
    }
    const enriched = await enrichBodyWithCarrierIds(req.body || {});
    if (enriched.error === "call_id_not_mapped") {
        return res.status(404).json({
            error: "call_id_not_mapped",
            details: "No Telnyx call_control_id found for this call_id. Ensure /api/telnyx-mapping ran after dial.",
            call_id: enriched.dialerCallId,
        });
    }
    const body = enriched.body;
    const callControlId = normalizeTelnyxCallControlId(
        body.call_control_id || body.callControlId
    );
    if (!callControlId) {
        return res.status(400).json({
            error: "call_control_id_or_call_id_required",
            details: "Pass call_control_id, or call_id / call_unique_id after mapping exists.",
        });
    }
    try {
        await hangupCallControl(callControlId);
        return res.status(200).json({
            success: true,
            provider: "telnyx",
            call_control_id: callControlId,
            call_id: enriched.dialerCallId || body.call_id || null,
            resolved_from_call_id: enriched.resolved === true,
        });
    } catch (err) {
        logger.error("[Telnyx] Hangup failed", { error: err.message, call_control_id: callControlId });
        return res.status(502).json({ error: err.message || "Hangup failed" });
    }
}

/** Force hangup a Twilio CallSid (parity with /telnyx/hangup). */
async function handleTwilioHangup(req, res) {
    if (rejectUnauthorizedControlRequest(req, res)) return;
    if (!isTwilioConfigured()) {
        return res.status(503).json({
            error: "Twilio is not configured (TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN)",
        });
    }
    const enriched = await enrichBodyWithCarrierIds(req.body || {});
    if (enriched.error === "call_id_not_mapped") {
        return res.status(404).json({
            error: "call_id_not_mapped",
            details: "No Twilio CallSid found for this call_id. Ensure /api/twilio-mapping ran after dial.",
            call_id: enriched.dialerCallId,
        });
    }
    const body = enriched.body;
    const callSid = normalizeTwilioCallSid(
        body.CallSid || body.call_sid || body.twilio_call_sid
    );
    if (!callSid) {
        return res.status(400).json({
            error: "CallSid_or_call_id_required",
            details: "Pass CallSid, or call_id / call_unique_id after mapping exists.",
        });
    }
    try {
        await hangupTwilioCall(callSid);
        return res.status(200).json({
            success: true,
            provider: "twilio",
            CallSid: callSid,
            call_id: enriched.dialerCallId || body.call_id || null,
            resolved_from_call_id: enriched.resolved === true,
        });
    } catch (err) {
        logger.error("[Twilio] Hangup failed", { error: err.message, CallSid: callSid });
        return res.status(502).json({ error: err.message || "Hangup failed" });
    }
}

/**
 * Common hangup — routes by provider / id fields.
 * Preferred body: { call_id } (or call_unique_id). Carrier ids optional.
 * Legacy: { provider?: 'twilio'|'telnyx', CallSid? | call_control_id? }
 */
async function handleCommonHangup(req, res) {
    if (rejectUnauthorizedControlRequest(req, res)) return;
    const enriched = await enrichBodyWithCarrierIds(req.body || {});
    if (enriched.error === "call_id_not_mapped") {
        return res.status(404).json({
            error: "call_id_not_mapped",
            details:
                "No carrier mapping found for this call_id. Ensure /api/twilio-mapping or /api/telnyx-mapping ran after dial.",
            call_id: enriched.dialerCallId,
        });
    }
    req.body = enriched.body;
    const provider = resolveTelephonyProvider(req.body || {});
    if (provider === "telnyx") return handleTelnyxHangup(req, res);
    if (provider === "twilio") return handleTwilioHangup(req, res);
    return res.status(400).json({
        error: "provider_or_call_id_required",
        details:
            "Pass call_id / call_unique_id (preferred), or provider=twilio|telnyx with CallSid / call_control_id.",
    });
}

app.post("/telnyx/hangup", handleTelnyxHangup);
app.post("/twilio/hangup", handleTwilioHangup);
app.post("/hangup", handleCommonHangup);

// ─── Telnyx Conversation Store Endpoint ───────────────────────────────────────
async function handleTelnyxConversation(req, res) {
    const enriched = await enrichBodyWithCarrierIds(req.body || {});
    if (enriched.error === "call_id_not_mapped") {
        return res.status(404).json({
            received: false,
            error: "call_id_not_mapped",
            details: "No Telnyx call_control_id found for this call_id. Ensure /api/telnyx-mapping ran after dial.",
            call_id: enriched.dialerCallId,
        });
    }
    const body = enriched.body;
    const sid = normalizeTelnyxCallControlId(
        body.call_control_id || body.telnyx_call_control_id || body.callControlId
    );
    if (!sid) {
        return res.status(400).json({
            received: false,
            error: "missing_required_fields",
            missing: ["call_control_id_or_call_id"],
            details: "Pass call_control_id, or call_id / call_unique_id after mapping exists.",
        });
    }

    const normalizedConversation = normalizeTwilioConversationPayload(body);
    if (!normalizedConversation.turns.length) {
        return res.status(400).json({
            received: false,
            error: "missing_conversation_payload",
            details: "Provide `turns`, `conversation`, `messages`, or `transcript`.",
        });
    }

    const telnyxMapping = await lookupTelnyxCallControlMapping(sid);
    const eventDoc = {
        timestamp: new Date().toISOString(),
        event_type: "telnyx_conversation_upserted",
        data: {
            call_control_id: sid,
            turn_count: normalizedConversation.turns.length,
        },
    };

    const startTimeRaw =
        typeof body.start_time === "string"
            ? body.start_time
            : typeof body.startTime === "string"
              ? body.startTime
              : null;
    const endTimeRaw =
        typeof body.end_time === "string"
            ? body.end_time
            : typeof body.endTime === "string"
              ? body.endTime
              : null;
    const normalizedStartTime =
        startTimeRaw && !Number.isNaN(new Date(startTimeRaw).getTime())
            ? new Date(startTimeRaw).toISOString()
            : null;
    const normalizedEndTime =
        endTimeRaw && !Number.isNaN(new Date(endTimeRaw).getTime())
            ? new Date(endTimeRaw).toISOString()
            : null;

    const legacyConversation = buildLegacyConversationShape({
        turns: normalizedConversation.turns,
        transcript: normalizedConversation.transcript,
        startTime: normalizedStartTime,
        endTime: normalizedEndTime,
    });

    const dialerCallUniqueId =
        body.call_unique_id != null && String(body.call_unique_id).trim() !== ""
            ? String(body.call_unique_id).trim()
            : body.call_id != null && String(body.call_id).trim() !== ""
              ? String(body.call_id).trim()
              : "";
    const fallbackContactId = body.contact_id != null ? String(body.contact_id).trim() : "";
    const fallbackCampaignId = body.campaign_id != null ? String(body.campaign_id).trim() : "";

    const telnyxSetFields = {
        "telnyx.call_control_id": sid,
        "telnyx.conversation.updatedAt": new Date().toISOString(),
        "telnyx.conversation.turns": normalizedConversation.turns,
        "telnyx.conversation.turnCount": normalizedConversation.turns.length,
        "telnyx.conversation.transcript": legacyConversation.transcript,
        "conversation.turns": legacyConversation.turns,
        "conversation.transcript": legacyConversation.transcript,
    };
    if (fallbackCampaignId) telnyxSetFields.campaign_id = fallbackCampaignId;
    if (fallbackContactId) telnyxSetFields.contact_id = fallbackContactId;
    if (inferIsTestCallFromWebhookBody(body) || telnyxMapping?.is_test_call === true) {
        telnyxSetFields.isTestCall = true;
    }
    if (legacyConversation.start_time) {
        telnyxSetFields["conversation.start_time"] = legacyConversation.start_time;
    }
    if (legacyConversation.end_time) {
        telnyxSetFields["conversation.end_time"] = legacyConversation.end_time;
    }
    if (dialerCallUniqueId) {
        telnyxSetFields.call_unique_id = dialerCallUniqueId;
        telnyxSetFields["telnyx.external_call_id"] = dialerCallUniqueId;
    }

    const CALLLOGS_COLLECTION = process.env.CALLLOGS_COLLECTION || "CallLogs";
    const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";
    const INBOUND_COLL = process.env.INBOUNDCALLLOG_COLLECTION || "InboundConversation";
    const allCollections = [CALLLOGS_COLLECTION, TESTCALL_COLLECTION, INBOUND_COLL];
    const mappedCollection =
        telnyxMapping?.collectionName && allCollections.includes(String(telnyxMapping.collectionName))
            ? String(telnyxMapping.collectionName)
            : null;
    const primaryCollection =
        mappedCollection ||
        resolveCollection({ contact_id: telnyxMapping?.contact_id || fallbackContactId }) ||
        CALLLOGS_COLLECTION;
    const collectionsOrdered = [...new Set([primaryCollection, ...allCollections])];

    let updated = false;
    for (const collectionName of collectionsOrdered) {
        const ok = await mergeTelnyxStatusIntoCallLog(
            collectionName,
            { "telnyx.call_control_id": sid },
            telnyxSetFields,
            eventDoc
        );
        if (ok) {
            updated = true;
            break;
        }
    }

    if (!updated) {
        const fallbackLeadId = body.lead_id != null ? String(body.lead_id).trim() : "";
        updated = await upsertTelnyxAnchoredCallLog({
            collectionName: primaryCollection,
            callControlId: sid,
            telnyxSetFields,
            eventDoc,
            rootFromMapping: {
                campaign_id: telnyxMapping?.campaign_id || fallbackCampaignId,
                contact_id: telnyxMapping?.contact_id || fallbackContactId,
                lead_id: telnyxMapping?.lead_id || fallbackLeadId,
                call_id: telnyxMapping?.call_id || dialerCallUniqueId,
            },
        });
    }

    const db = getDb();
    const storedDoc = await db.collection(primaryCollection).findOne(
        { "telnyx.call_control_id": sid },
        {
            projection: {
                _id: 1,
                createdAt: 1,
                updatedAt: 1,
                campaign_id: 1,
                contact_id: 1,
                call_id: 1,
                call_unique_id: 1,
                "conversation.turns": 1,
                "telnyx.conversation.turnCount": 1,
                "telnyx.status": 1,
                "telnyx.duration": 1,
                isTestCall: 1,
            },
        }
    );

    const contactForSync = resolveTelnyxContactId({
        telnyxMapping,
        body,
        storedDoc,
    });
    const telnyxStatusForContact = String(storedDoc?.telnyx?.status || "").trim();
    let contactSyncResult = { outcome: "skip_no_contact_id" };
    if (contactForSync && telnyxStatusForContact) {
        contactSyncResult = await syncTelnyxContactFromCall({
            contactIdRaw: contactForSync,
            status: telnyxStatusForContact,
            callControlId: sid,
            source: "telnyx_conversation",
        });
    }

    let creditResult = null;
    const billableDuration = Math.max(0, Math.floor(Number(storedDoc?.telnyx?.duration) || 0));
    const telnyxCompleted = String(storedDoc?.telnyx?.status || "").toLowerCase() === "completed";
    if (telnyxCompleted && billableDuration > 0) {
        creditResult = await maybeDeductTelnyxCallCredits({
            callControlId: sid,
            durationSec: billableDuration,
            telnyxMapping,
            body,
            collectionName: primaryCollection,
        });
    } else if (telnyxCompleted && billableDuration <= 0) {
        creditResult = { outcome: "skip_no_duration_on_doc" };
    }

    const conversationReceiveStatus = mapTwilioCallStatusToReceiveStatus(
        storedDoc?.telnyx?.status || ""
    );
    emitCallUpdateSse({
        campaign_id: storedDoc?.campaign_id || telnyxMapping?.campaign_id || null,
        call_id: storedDoc?.call_id || storedDoc?.call_unique_id || sid,
        contact_id: contactForSync || storedDoc?.contact_id || null,
        status: conversationReceiveStatus,
        telnyx_status: storedDoc?.telnyx?.status || null,
        event: "telnyx_conversation",
        provider: "telnyx",
        turnCount: normalizedConversation.turns.length,
    });

    triggerCallAnalysis(sid).catch((err) => {
        logger.warn("[Telnyx] Analysis trigger failed after conversation store", {
            call_control_id: sid,
            error: err.message,
        });
    });

    return res.status(200).json({
        received: true,
        updated: !!updated,
        provider: "telnyx",
        call_control_id: sid,
        turnCount: normalizedConversation.turns.length,
        credit: creditResult,
        contactSync: contactSyncResult,
    });
}

// ─── Twilio Conversation Store Endpoint ───────────────────────────────────────
// Receives: { CallSid?, call_id?, turns?|conversation?|messages?|transcript?, campaign_id?, contact_id? }
async function handleTwilioConversation(req, res) {
    const enriched = await enrichBodyWithCarrierIds(req.body || {});
    if (enriched.error === "call_id_not_mapped") {
        return res.status(404).json({
            received: false,
            error: "call_id_not_mapped",
            details: "No Twilio CallSid found for this call_id. Ensure /api/twilio-mapping ran after dial.",
            call_id: enriched.dialerCallId,
        });
    }
    const body = enriched.body;
    logTwilioWebhookEvent(req, "[Twilio] Conversation webhook payload", body);
    const sid = normalizeTwilioCallSid(body.CallSid || body.call_sid || body.twilio_call_sid);
    if (!sid) {
        return res.status(400).json({
            received: false,
            error: "missing_required_fields",
            missing: ["CallSid_or_call_id"],
            details: "Pass CallSid, or call_id / call_unique_id after mapping exists.",
        });
    }

    const normalizedConversation = normalizeTwilioConversationPayload(body);
    if (!normalizedConversation.turns.length) {
        return res.status(400).json({
            received: false,
            error: "missing_conversation_payload",
            details: "Provide `turns`, `conversation`, `messages`, or `transcript`.",
        });
    }

    const twilioMapping = await lookupTwilioCallSidMapping(sid);
    const eventDoc = {
        timestamp: new Date().toISOString(),
        event_type: "twilio_conversation_upserted",
        data: {
            CallSid: sid,
            turn_count: normalizedConversation.turns.length,
        },
    };

    const startTimeRaw =
        typeof body.start_time === "string"
            ? body.start_time
            : typeof body.startTime === "string"
              ? body.startTime
              : null;
    const endTimeRaw =
        typeof body.end_time === "string"
            ? body.end_time
            : typeof body.endTime === "string"
              ? body.endTime
              : null;
    const normalizedStartTime =
        startTimeRaw && !Number.isNaN(new Date(startTimeRaw).getTime())
            ? new Date(startTimeRaw).toISOString()
            : null;
    const normalizedEndTime =
        endTimeRaw && !Number.isNaN(new Date(endTimeRaw).getTime())
            ? new Date(endTimeRaw).toISOString()
            : null;

    const legacyConversation = buildLegacyConversationShape({
        turns: normalizedConversation.turns,
        transcript: normalizedConversation.transcript,
        startTime: normalizedStartTime,
        endTime: normalizedEndTime,
    });

    const dialerCallUniqueId =
        body.call_unique_id != null && String(body.call_unique_id).trim() !== ""
            ? String(body.call_unique_id).trim()
            : body.call_id != null && String(body.call_id).trim() !== ""
              ? String(body.call_id).trim()
              : "";
    const fallbackContactId = body.contact_id != null ? String(body.contact_id).trim() : "";
    const fallbackCampaignId = body.campaign_id != null ? String(body.campaign_id).trim() : "";

    const twilioSetFields = {
        "twilio.call_sid": sid,
        "twilio.conversation.updatedAt": new Date().toISOString(),
        "twilio.conversation.turns": normalizedConversation.turns,
        "twilio.conversation.turnCount": normalizedConversation.turns.length,
        "twilio.conversation.transcript": legacyConversation.transcript,
        "conversation.turns": legacyConversation.turns,
        "conversation.transcript": legacyConversation.transcript,
    };
    if (fallbackCampaignId) twilioSetFields.campaign_id = fallbackCampaignId;
    if (fallbackContactId) twilioSetFields.contact_id = fallbackContactId;
    if (inferIsTestCallFromWebhookBody(body) || twilioMapping?.is_test_call === true) {
        twilioSetFields.isTestCall = true;
    }
    if (legacyConversation.start_time) {
        twilioSetFields["conversation.start_time"] = legacyConversation.start_time;
    }
    if (legacyConversation.end_time) {
        twilioSetFields["conversation.end_time"] = legacyConversation.end_time;
    }
    if (dialerCallUniqueId) {
        twilioSetFields.call_unique_id = dialerCallUniqueId;
        twilioSetFields["twilio.external_call_id"] = dialerCallUniqueId;
    }

    const CALLLOGS_COLLECTION = process.env.CALLLOGS_COLLECTION || "CallLogs";
    const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";
    const INBOUNDCALLLOG_COLLECTION = process.env.INBOUNDCALLLOG_COLLECTION || "InboundConversation";
    const allCollections = [CALLLOGS_COLLECTION, TESTCALL_COLLECTION, INBOUNDCALLLOG_COLLECTION];
    const mappedCollection =
        twilioMapping?.collectionName && allCollections.includes(String(twilioMapping.collectionName))
            ? String(twilioMapping.collectionName)
            : null;
    const primaryCollection =
        mappedCollection ||
        resolveCollection({ contact_id: twilioMapping?.contact_id || fallbackContactId }) ||
        CALLLOGS_COLLECTION;
    const collectionsOrdered = [...new Set([primaryCollection, ...allCollections])];

    let updated = false;
    for (const collectionName of collectionsOrdered) {
        const ok = await mergeTwilioStatusIntoCallLog(
            collectionName,
            { "twilio.call_sid": sid },
            twilioSetFields,
            eventDoc
        );
        if (ok) {
            updated = true;
            break;
        }
    }

    if (!updated) {
        const fallbackLeadId = body.lead_id != null ? String(body.lead_id).trim() : "";
        const fallbackCallId = dialerCallUniqueId;
        updated = await upsertTwilioAnchoredCallLog({
            collectionName: primaryCollection,
            twilioCallSid: sid,
            twilioSetFields,
            eventDoc,
            rootFromMapping: {
                campaign_id: twilioMapping?.campaign_id || fallbackCampaignId,
                contact_id: twilioMapping?.contact_id || fallbackContactId,
                lead_id: twilioMapping?.lead_id || fallbackLeadId,
                call_id: twilioMapping?.call_id || fallbackCallId,
            },
        });
    }

    const db = getDb();
    const storedDoc = await db.collection(primaryCollection).findOne(
        { "twilio.call_sid": sid },
        {
            projection: {
                _id: 1,
                createdAt: 1,
                updatedAt: 1,
                campaign_id: 1,
                contact_id: 1,
                "conversation.turns": 1,
                "twilio.conversation.turnCount": 1,
                "twilio.status": 1,
                "twilio.duration": 1,
                isTestCall: 1,
            },
        }
    );
    const storedTurnCount =
        (Array.isArray(storedDoc?.conversation?.turns) ? storedDoc.conversation.turns.length : 0) ||
        storedDoc?.twilio?.conversation?.turnCount ||
        0;

    const contactForSync = resolveTwilioContactId({
        twilioMapping,
        body,
        storedDoc,
    });
    const twilioStatusForContact = String(storedDoc?.twilio?.status || "").trim();
    let contactSyncResult = { outcome: "skip_no_contact_id" };
    if (contactForSync && twilioStatusForContact) {
        contactSyncResult = await syncTwilioContactFromCall({
            contactIdRaw: contactForSync,
            twilioStatus: twilioStatusForContact,
            callSid: sid,
            source: "twilio_conversation",
        });
    }

    logTwilioEventData("[Twilio] Conversation stored in CallLogs", {
        CallSid: sid,
        turnCount: normalizedConversation.turns.length,
        storedTurnCount,
        hasConversationInDb: storedTurnCount > 0,
        collection: primaryCollection,
        docId: storedDoc?._id ? String(storedDoc._id) : null,
        createdAt: storedDoc?.createdAt || null,
        updatedAt: storedDoc?.updatedAt || null,
        campaign_id: storedDoc?.campaign_id || null,
        contact_id: storedDoc?.contact_id || null,
        twilioStatus: storedDoc?.twilio?.status || null,
        twilioDuration: storedDoc?.twilio?.duration ?? null,
        hadMapping: !!twilioMapping,
        contactSync: contactSyncResult,
        conversation: legacyConversation,
        call_data_event: eventDoc,
    });

    let creditResult = null;
    const billableDuration = Math.max(
        0,
        Math.floor(Number(storedDoc?.twilio?.duration) || 0)
    );
    const twilioCompleted =
        String(storedDoc?.twilio?.status || "").toLowerCase() === "completed";
    if (twilioCompleted && billableDuration > 0) {
        creditResult = await maybeDeductTwilioCallCredits({
            callSid: sid,
            durationSec: billableDuration,
            twilioMapping,
            body,
            collectionName: primaryCollection,
        });
    } else if (twilioCompleted && billableDuration <= 0) {
        creditResult = { outcome: "skip_no_duration_on_doc" };
    }

    const conversationReceiveStatus = mapTwilioCallStatusToReceiveStatus(
        storedDoc?.twilio?.status || ""
    );
    emitCallUpdateSse({
        campaign_id: storedDoc?.campaign_id || twilioMapping?.campaign_id || null,
        call_id: storedDoc?.call_id || storedDoc?.call_unique_id || sid,
        contact_id: contactForSync || storedDoc?.contact_id || null,
        status: conversationReceiveStatus,
        twilio_status: storedDoc?.twilio?.status || null,
        event: "twilio_conversation",
        provider: "twilio",
        turnCount: normalizedConversation.turns.length,
    });

    // Best-effort analysis trigger for Twilio calls once conversation is available.
    triggerCallAnalysis(sid).catch((err) => {
        logger.warn("[Twilio] Analysis trigger failed after conversation store", {
            CallSid: sid,
            error: err.message,
        });
    });

    return res.status(200).json({
        received: true,
        updated: !!updated,
        provider: "twilio",
        callSid: sid,
        turnCount: normalizedConversation.turns.length,
        credit: creditResult,
        contactSync: contactSyncResult,
    });
}

/**
 * India / pool conversation store — dialer call_id only (no CallSid / call_control_id).
 * Body: { call_id | call_unique_id, turns|conversation|..., campaign_id?, contact_id? }
 */
async function handlePoolConversation(req, res) {
    const body = req.body || {};
    const callKey = normalizeCallId(
        body.call_id || body.call_unique_id || body.callUniqueId || body.Call_UniqueId
    );
    if (!callKey) {
        return res.status(400).json({
            received: false,
            error: "missing_required_fields",
            missing: ["call_id"],
            details: "India/pool conversation requires call_id (or call_unique_id).",
        });
    }

    const normalizedConversation = normalizeTwilioConversationPayload(body);
    if (!normalizedConversation.turns.length) {
        return res.status(400).json({
            received: false,
            error: "missing_conversation_payload",
            details: "Provide `turns`, `conversation`, `messages`, or `transcript`.",
        });
    }

    const mapping = await lookupMapping(callKey);
    const startTimeRaw =
        typeof body.start_time === "string"
            ? body.start_time
            : typeof body.startTime === "string"
              ? body.startTime
              : null;
    const endTimeRaw =
        typeof body.end_time === "string"
            ? body.end_time
            : typeof body.endTime === "string"
              ? body.endTime
              : null;
    const normalizedStartTime =
        startTimeRaw && !Number.isNaN(new Date(startTimeRaw).getTime())
            ? new Date(startTimeRaw).toISOString()
            : null;
    const normalizedEndTime =
        endTimeRaw && !Number.isNaN(new Date(endTimeRaw).getTime())
            ? new Date(endTimeRaw).toISOString()
            : null;

    const legacyConversation = buildLegacyConversationShape({
        turns: normalizedConversation.turns,
        transcript: normalizedConversation.transcript,
        startTime: normalizedStartTime,
        endTime: normalizedEndTime,
    });

    const contactId =
        (body.contact_id != null && String(body.contact_id).trim()) ||
        (mapping?.contact_id != null && String(mapping.contact_id).trim()) ||
        "";
    const campaignId =
        (body.campaign_id != null && String(body.campaign_id).trim()) ||
        (mapping?.campaign_id != null && String(mapping.campaign_id).trim()) ||
        "";
    const leadId =
        (body.lead_id != null && String(body.lead_id).trim()) ||
        (mapping?.lead_id != null && String(mapping.lead_id).trim()) ||
        callKey;

    const eventDoc = {
        timestamp: new Date().toISOString(),
        event_type: "pool_conversation_upserted",
        data: {
            call_id: callKey,
            turn_count: normalizedConversation.turns.length,
            provider: "pool",
        },
    };

    const setFields = {
        call_unique_id: callKey,
        lead_id: leadId,
        provider: "pool",
        "conversation.updatedAt": new Date().toISOString(),
        "conversation.turns": legacyConversation.turns,
        "conversation.transcript": legacyConversation.transcript,
        "pool.conversation.updatedAt": new Date().toISOString(),
        "pool.conversation.turns": normalizedConversation.turns,
        "pool.conversation.turnCount": normalizedConversation.turns.length,
        "pool.conversation.transcript": legacyConversation.transcript,
        updatedAt: new Date(),
    };
    if (campaignId) setFields.campaign_id = campaignId;
    if (contactId) setFields.contact_id = contactId;
    if (inferIsTestCallFromWebhookBody(body) || mapping?.is_test_call === true) {
        setFields.isTestCall = true;
    }
    if (legacyConversation.start_time) {
        setFields["conversation.start_time"] = legacyConversation.start_time;
    }
    if (legacyConversation.end_time) {
        setFields["conversation.end_time"] = legacyConversation.end_time;
    }

    const CALLLOGS_COLLECTION = process.env.CALLLOGS_COLLECTION || "CallLogs";
    const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";
    const mappedCollection =
        mapping?.collectionName && String(mapping.collectionName).trim()
            ? String(mapping.collectionName).trim()
            : null;
    const primaryCollection =
        mappedCollection ||
        resolveCollection({ contact_id: contactId }) ||
        (await resolveOutboundCollection()) ||
        CALLLOGS_COLLECTION;
    const collectionsOrdered = [
        ...new Set([primaryCollection, CALLLOGS_COLLECTION, TESTCALL_COLLECTION]),
    ];

    const filters = [
        { call_unique_id: callKey },
        { lead_id: callKey },
        { call_id: callKey },
    ];

    const db = getDb();
    let updated = false;
    let matchedCollection = primaryCollection;

    for (const collectionName of collectionsOrdered) {
        for (const filter of filters) {
            const result = await db.collection(collectionName).updateOne(filter, {
                $set: setFields,
                $setOnInsert: {
                    createdAt: new Date().toISOString(),
                    call_id: callKey,
                },
                $push: { "call_data.events": eventDoc },
            });
            if (result.matchedCount > 0) {
                updated = true;
                matchedCollection = collectionName;
                break;
            }
        }
        if (updated) break;
    }

    if (!updated) {
        await db.collection(primaryCollection).updateOne(
            { call_unique_id: callKey },
            {
                $set: setFields,
                $setOnInsert: {
                    createdAt: new Date().toISOString(),
                    call_id: callKey,
                    lead_id: leadId,
                },
                $push: { "call_data.events": eventDoc },
            },
            { upsert: true }
        );
        updated = true;
        matchedCollection = primaryCollection;
    }

    const storedDoc = await db.collection(matchedCollection).findOne(
        {
            $or: [
                { call_unique_id: callKey },
                { lead_id: callKey },
                { call_id: callKey },
            ],
        },
        {
            projection: {
                _id: 1,
                campaign_id: 1,
                contact_id: 1,
                call_id: 1,
                call_unique_id: 1,
                "conversation.turns": 1,
                isTestCall: 1,
            },
        }
    );

    emitCallUpdateSse({
        campaign_id: storedDoc?.campaign_id || campaignId || null,
        call_id: storedDoc?.call_unique_id || storedDoc?.call_id || callKey,
        contact_id: storedDoc?.contact_id || contactId || null,
        status: null,
        event: "pool_conversation",
        provider: "pool",
        turnCount: normalizedConversation.turns.length,
    });

    triggerCallAnalysis(callKey).catch((err) => {
        logger.warn("[Pool] Analysis trigger failed after conversation store", {
            call_id: callKey,
            error: err.message,
        });
    });

    logger.info("[Pool] Conversation stored", {
        call_id: callKey,
        turnCount: normalizedConversation.turns.length,
        collection: matchedCollection,
        campaign_id: campaignId || null,
        contact_id: contactId || null,
        hadMapping: !!mapping,
    });

    return res.status(200).json({
        received: true,
        updated: !!updated,
        provider: "pool",
        call_id: callKey,
        turnCount: normalizedConversation.turns.length,
        collection: matchedCollection,
    });
}

/**
 * Common conversation store — routes by provider / id fields.
 * Preferred: { call_id, turns|conversation|... }
 * India/pool: { provider?: "pool", call_id, turns|... } or /pool/conversation
 * Foreign: { provider?: twilio|telnyx, CallSid? | call_control_id?, turns|... }
 */
async function handleCommonConversation(req, res) {
    const rawBody = req.body || {};
    const explicit = String(rawBody.provider || rawBody.telephony_provider || "")
        .trim()
        .toLowerCase();
    if (explicit === "pool" || explicit === "india") {
        return handlePoolConversation(req, res);
    }

    // Carrier ids present → foreign path.
    if (hasCarrierId(rawBody)) {
        const enriched = await enrichBodyWithCarrierIds(rawBody);
        req.body = enriched.body;
        const provider = resolveTelephonyProvider(req.body || {});
        if (provider === "telnyx") return handleTelnyxConversation(req, res);
        if (provider === "twilio") return handleTwilioConversation(req, res);
    }

    const enriched = await enrichBodyWithCarrierIds(rawBody);
    if (enriched.resolved) {
        req.body = enriched.body;
        const provider = resolveTelephonyProvider(req.body || {});
        if (provider === "telnyx") return handleTelnyxConversation(req, res);
        if (provider === "twilio") return handleTwilioConversation(req, res);
    }

    // No Twilio/Telnyx mapping — India/pool dialer call_id conversation.
    const dialerId = enriched.dialerCallId || pickDialerCallId(rawBody);
    if (dialerId) {
        req.body = { ...rawBody, call_id: dialerId };
        return handlePoolConversation(req, res);
    }

    return res.status(400).json({
        received: false,
        error: "provider_or_call_id_required",
        details:
            "Pass call_id (India/pool or mapped foreign call), or provider=twilio|telnyx|pool with carrier ids when needed.",
    });
}

app.post("/pool/conversation", handlePoolConversation);
app.post("/india/conversation", handlePoolConversation);
app.post("/telnyx/conversation", handleTelnyxConversation);
app.post("/twilio/conversation", handleTwilioConversation);
app.post("/conversation", handleCommonConversation);

// ─── FLOW 3: Telephony Webhook Endpoint ──────────────────────────────────────
// Receives all webhook events from telephony provider.
app.post("/api/v1/webhooks/receiver", async (req, res) => {
    console.log(`\n--- Incoming Webhook Request from ${req.ip} ---`);
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
        console.log(">>> [InboundMapping] Call mapping registered for call_id:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::", call_id);
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
/**
 * AiSensy provider ingress (canonical public URL):
 *   Live: https://api.ondial.ai/api/webhook/aisensy
 *   Test: https://dev-api.ondial.ai/api/webhook/aisensy
 * Enqueues to BullMQ `aisensy-inbound` for Calling_system1; updates marketing logs async.
 */
app.post("/api/webhook/aisensy", async (req, res) => {
    const rawBody = req.rawBody || Buffer.from(JSON.stringify(req.body || {}));
    const signature =
        req.headers["x-aisensy-signature"] ||
        req.headers["x-hub-signature-256"] ||
        req.headers["x-signature"];
    const secret = String(process.env.AISENSY_WEBHOOK_SECRET || process.env.WEBHOOK_SECRET || "").trim();
    const requireSecret =
        process.env.NODE_ENV === "production" ||
        String(process.env.AISENSY_WEBHOOK_REQUIRE_SECRET || "").toLowerCase() === "true";

    if (!secret) {
        if (requireSecret) {
            logger.error("[AiSensy] WEBHOOK_SECRET / AISENSY_WEBHOOK_SECRET not configured");
            return res.status(503).json({ error: "Webhook secret not configured" });
        }
        logger.warn("[AiSensy] webhook secret unset — signature verification skipped (dev only)");
    } else if (!verifyAisensySignature(rawBody, signature, secret)) {
        return res.status(401).json({ error: "Invalid signature" });
    }

    const payload = req.body;
    if (!payload || typeof payload !== "object") {
        return res.status(400).json({ error: "Invalid JSON" });
    }

    // Enqueue before ACK so Redis failures are not silently dropped after 200
    try {
        await enqueueAisensyInbound(payload, {
            signaturePresent: Boolean(signature),
            sourceIp: req.ip,
        });
    } catch (err) {
        logger.error("[AiSensy] enqueue failed", { error: err.message });
        return res.status(503).json({ error: "Queue unavailable", detail: err.message });
    }

    res.status(200).json({ received: true });

    processAisensyMarketingWebhookSafe(payload);
});

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
            await closeAisensyInboundQueue();
        } catch (err) {
            logger.error("Error closing webhook workers", { error: err.message });
        } finally {
            process.exit(0);
        }
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
}