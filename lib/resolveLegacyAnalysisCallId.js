/**
 * Legacy analysis URL call id: Twilio → CallSid first; Telnyx → dialer UUID; India → dialer UUID.
 */
const UUID_CALL_ID_RE =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const TWILIO_CALL_SID_RE = /^CA[a-f0-9]{32}$/i;

function pickString(...values) {
    for (const val of values) {
        if (val != null && String(val).trim() !== "") {
            return String(val).trim();
        }
    }
    return "";
}

function isUuidCallId(value) {
    return UUID_CALL_ID_RE.test(String(value || "").trim());
}

function isTwilioCallSid(value) {
    return TWILIO_CALL_SID_RE.test(String(value || "").trim());
}

/** Telnyx Call Control ids are typically `v3:...` (opaque, not CA… / UUID). */
function isTelnyxCallControlId(value) {
    const s = String(value || "").trim();
    if (!s) return false;
    return /^v\d+:/i.test(s);
}

/** Prefer Twilio CallSid so legacy analysis API finds Mongo transcript keyed by CA… */
function resolveTwilioCallSidForAnalysis(callLogDoc, fallbackId) {
    const twilioSid = pickString(callLogDoc?.twilio?.call_sid);
    if (isTwilioCallSid(twilioSid)) return twilioSid;

    const callId = pickString(callLogDoc?.call_id);
    if (isTwilioCallSid(callId)) return callId;

    const leadId = pickString(callLogDoc?.lead_id);
    if (isTwilioCallSid(leadId)) return leadId;
    const leadPrefix = leadId.match(/^twilio:(CA[a-f0-9]{32})$/i);
    if (leadPrefix) return leadPrefix[1];

    const fb = pickString(fallbackId);
    if (isTwilioCallSid(fb)) return fb;

    return "";
}

/** Prefer dialer UUID for Telnyx so analysis keys match worker /conversation call_id. */
function resolveTelnyxDialerCallIdForAnalysis(callLogDoc, fallbackId) {
    const candidates = [
        callLogDoc?.call_unique_id,
        callLogDoc?.telnyx?.external_call_id,
        isUuidCallId(callLogDoc?.lead_id) ? callLogDoc.lead_id : null,
        isUuidCallId(callLogDoc?.call_id) ? callLogDoc.call_id : null,
    ];
    for (const candidate of candidates) {
        const resolved = pickString(candidate);
        if (isUuidCallId(resolved)) return resolved;
    }

    const telnyxCcid = pickString(
        callLogDoc?.telnyx?.call_control_id,
        isTelnyxCallControlId(callLogDoc?.call_id) ? callLogDoc.call_id : null,
        isTelnyxCallControlId(fallbackId) ? fallbackId : null
    );
    return telnyxCcid || "";
}

function resolveLegacyAnalysisCallId(callLogDoc, fallbackId) {
    const twilioSid = resolveTwilioCallSidForAnalysis(callLogDoc, fallbackId);
    if (twilioSid) return twilioSid;

    const telnyxDialerOrCcid = resolveTelnyxDialerCallIdForAnalysis(callLogDoc, fallbackId);
    if (telnyxDialerOrCcid) return telnyxDialerOrCcid;

    const candidates = [
        callLogDoc?.call_unique_id,
        callLogDoc?.twilio?.external_call_id,
        isUuidCallId(callLogDoc?.lead_id) ? callLogDoc.lead_id : null,
        isUuidCallId(callLogDoc?.call_id) ? callLogDoc.call_id : null,
        callLogDoc?.twilio?.external_lead_id,
    ];
    for (const candidate of candidates) {
        const resolved = pickString(candidate);
        if (resolved) return resolved;
    }
    return String(fallbackId || "").trim();
}

module.exports = {
    isUuidCallId,
    isTwilioCallSid,
    isTelnyxCallControlId,
    resolveTwilioCallSidForAnalysis,
    resolveTelnyxDialerCallIdForAnalysis,
    resolveLegacyAnalysisCallId,
};
