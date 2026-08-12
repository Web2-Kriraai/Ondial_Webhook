const {
    normalizeCallId,
    normalizeTwilioCallSid,
    normalizeTelnyxCallControlId,
    lookupDialerCallMapping,
} = require("../callMapping");
const { findOneCallLogByIdentity } = require("./findCallLogsByIdentity");

const CALLLOGS_COLLECTION = process.env.CALLLOGS_COLLECTION || "CallLogs";
const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";

function pickDialerCallId(body = {}) {
    return normalizeCallId(
        body.call_id || body.call_unique_id || body.callUniqueId || body.Call_UniqueId
    );
}

function hasCarrierId(body = {}) {
    return Boolean(
        normalizeTelnyxCallControlId(
            body.call_control_id || body.telnyx_call_control_id || body.callControlId
        ) ||
            normalizeTwilioCallSid(body.CallSid || body.call_sid || body.twilio_call_sid)
    );
}

/**
 * Resolve carrier ids from dialer call_id via Redis, then Mongo CallLogs fallback.
 * @returns {Promise<object|null>}
 */
async function resolveCarrierFromDialerCallId(callId) {
    const key = normalizeCallId(callId);
    if (!key) return null;

    const fromRedis = await lookupDialerCallMapping(key);
    if (fromRedis?.provider === "twilio" && fromRedis.twilio_call_sid) {
        return fromRedis;
    }
    if (fromRedis?.provider === "telnyx" && fromRedis.call_control_id) {
        return fromRedis;
    }

    try {
        // Lazy require so unit tests can load helpers without MONGODB_URI.
        const { getDb } = require("../db");
        const db = getDb();
        const collections = [CALLLOGS_COLLECTION, TESTCALL_COLLECTION];
        for (const collectionName of collections) {
            const doc = await findOneCallLogByIdentity(db.collection(collectionName), key, {
                includeCarrier: false,
                limitPerQuery: 1,
            });
            if (!doc) continue;

            const telnyxId = normalizeTelnyxCallControlId(doc?.telnyx?.call_control_id);
            if (telnyxId) {
                return {
                    provider: "telnyx",
                    call_control_id: telnyxId,
                    telnyx_call_control_id: telnyxId,
                    call_id: key,
                    campaign_id: doc.campaign_id != null ? String(doc.campaign_id) : "",
                    contact_id: doc.contact_id != null ? String(doc.contact_id) : "",
                    source: "mongo",
                    collectionName,
                };
            }

            const twilioSid = normalizeTwilioCallSid(doc?.twilio?.call_sid);
            if (twilioSid) {
                return {
                    provider: "twilio",
                    twilio_call_sid: twilioSid,
                    CallSid: twilioSid,
                    call_id: key,
                    campaign_id: doc.campaign_id != null ? String(doc.campaign_id) : "",
                    contact_id: doc.contact_id != null ? String(doc.contact_id) : "",
                    source: "mongo",
                    collectionName,
                };
            }
        }
    } catch {
        // DB may not be ready in unit tests — Redis-only path still works.
    }

    return fromRedis || null;
}

/**
 * Enrich hangup/conversation body so callers can send only `{ call_id, turns? }`.
 * Carrier ids / provider remain optional overrides when already present.
 */
async function enrichBodyWithCarrierIds(body = {}) {
    const out = { ...(body && typeof body === "object" ? body : {}) };
    if (hasCarrierId(out)) {
        if (!out.call_id && !out.call_unique_id) {
            const dialer = pickDialerCallId(out);
            if (dialer) out.call_id = dialer;
        }
        return { body: out, resolved: false, dialerCallId: pickDialerCallId(out) };
    }

    const dialerCallId = pickDialerCallId(out);
    if (!dialerCallId) {
        return { body: out, resolved: false, dialerCallId: null, error: "missing_call_identity" };
    }

    const resolved = await resolveCarrierFromDialerCallId(dialerCallId);
    if (!resolved) {
        return {
            body: out,
            resolved: false,
            dialerCallId,
            error: "call_id_not_mapped",
        };
    }

    out.call_id = out.call_id || dialerCallId;
    out.call_unique_id = out.call_unique_id || dialerCallId;
    if (!out.provider && resolved.provider) out.provider = resolved.provider;

    if (resolved.provider === "twilio") {
        const sid = normalizeTwilioCallSid(resolved.twilio_call_sid || resolved.CallSid);
        if (sid) {
            out.CallSid = out.CallSid || sid;
            out.call_sid = out.call_sid || sid;
            out.twilio_call_sid = out.twilio_call_sid || sid;
        }
    }
    if (resolved.provider === "telnyx") {
        const ccid = normalizeTelnyxCallControlId(
            resolved.call_control_id || resolved.telnyx_call_control_id
        );
        if (ccid) {
            out.call_control_id = out.call_control_id || ccid;
            out.telnyx_call_control_id = out.telnyx_call_control_id || ccid;
            out.callControlId = out.callControlId || ccid;
        }
    }

    if (!out.campaign_id && resolved.campaign_id) out.campaign_id = resolved.campaign_id;
    if (!out.contact_id && resolved.contact_id) out.contact_id = resolved.contact_id;

    return { body: out, resolved: true, dialerCallId, mapping: resolved };
}

module.exports = {
    pickDialerCallId,
    hasCarrierId,
    resolveCarrierFromDialerCallId,
    enrichBodyWithCarrierIds,
};
