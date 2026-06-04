require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { ObjectId } = require("mongodb");
const { connectDB, getDb } = require("./db");
const { connectRedis } = require("./redis");
const {
    registerCallMapping,
    normalizePhone,
    normalizeCallId,
    registerTwilioCallSidMapping,
    lookupTwilioCallSidMapping,
    normalizeTwilioCallSid,
} = require("./callMapping");
const {
    createCallLog,
    INBOUNDCALLLOG_COLLECTION,
    resolveCollection,
    resolveOutboundCollection,
    buildTwilioStatusEvent,
    mergeTwilioStatusIntoCallLog,
    upsertTwilioAnchoredCallLog,
} = require("./callLogs");
const logger = require("./logger");
const callEvents = require("./events");
const { enqueueWebhook, startWebhookWorkers, closeWebhookWorkers, getQueueLagSnapshot } = require("./webhookQueue");
const { logMissingCallMapping, previewPayload } = require("./errorLog");
const { triggerCallAnalysis } = require("./lib/triggerCallAnalysis");
const { inferIsTestCallFromWebhookBody } = require("./lib/inferTestCall");
const { pickNonEmpty } = require("./lib/customParameters");
const { maybeDeductTwilioCallCredits } = require("./lib/twilioCallBilling");
const {
    resolveTwilioContactId,
    syncTwilioContactFromCall,
} = require("./lib/twilioContactSync");
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

// ─── Twilio Conversation Store Endpoint ───────────────────────────────────────
// Receives: { CallSid, turns?|conversation?|messages?|transcript?, campaign_id?, contact_id? }
app.post("/twilio/conversation", async (req, res) => {
    const body = req.body || {};
    logTwilioWebhookEvent(req, "[Twilio] Conversation webhook payload", body);
    const sid = normalizeTwilioCallSid(body.CallSid || body.call_sid || body.twilio_call_sid);
    if (!sid) {
        return res.status(400).json({
            received: false,
            error: "missing_required_fields",
            missing: ["CallSid"],
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
        callSid: sid,
        turnCount: normalizedConversation.turns.length,
        credit: creditResult,
        contactSync: contactSyncResult,
    });
});

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