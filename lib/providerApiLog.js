/**
 * Structured ingress logs for Twilio / Telnyx / conversation APIs.
 * Shows: which route was hit, which provider, test vs normal, which fields the caller sent.
 */

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

function pick(obj, ...keys) {
    for (const k of keys) {
        const v = obj?.[k];
        if (v != null && String(v).trim() !== "") return v;
    }
    return null;
}

function hasOwn(obj, key) {
    return obj != null && Object.prototype.hasOwnProperty.call(obj, key) && obj[key] != null;
}

function truthyFlag(v) {
    return v === true || v === "true" || v === 1 || v === "1";
}

function falseyFlag(v) {
    return v === false || v === "false" || v === 0 || v === "0";
}

/**
 * Resolve TEST vs NORMAL for easy log scanning.
 * Prefers explicit opts, then body flags / custom_parameters, else UNKNOWN
 * (Telnyx Mission Control events often omit the flag until mapping is applied).
 *
 * @returns {{ call_kind: "TEST"|"NORMAL"|"UNKNOWN", is_test_call: boolean|null, test_signal: string|null }}
 */
function resolveCallKind(body = {}, extra = {}) {
    if (extra.call_kind === "TEST" || extra.call_kind === "NORMAL") {
        return {
            call_kind: extra.call_kind,
            is_test_call: extra.call_kind === "TEST",
            test_signal: extra.test_signal || "extra.call_kind",
        };
    }
    if (truthyFlag(extra.is_test_call) || extra.isTestCall === true) {
        return { call_kind: "TEST", is_test_call: true, test_signal: extra.test_signal || "extra.is_test_call" };
    }
    if (falseyFlag(extra.is_test_call) || extra.isTestCall === false) {
        return { call_kind: "NORMAL", is_test_call: false, test_signal: extra.test_signal || "extra.is_test_call" };
    }

    const b = body && typeof body === "object" ? body : {};
    if (truthyFlag(b.is_test_call) || truthyFlag(b.isTestCall)) {
        return { call_kind: "TEST", is_test_call: true, test_signal: "body.is_test_call" };
    }
    if (falseyFlag(b.is_test_call) || falseyFlag(b.isTestCall)) {
        return { call_kind: "NORMAL", is_test_call: false, test_signal: "body.is_test_call" };
    }

    let cp = b.custom_parameters || b.customParameters || null;
    if (typeof cp === "string") {
        try {
            cp = JSON.parse(cp);
        } catch {
            cp = null;
        }
    }
    if (cp && typeof cp === "object") {
        if (truthyFlag(cp.is_test_call) || truthyFlag(cp.isTestCall)) {
            return { call_kind: "TEST", is_test_call: true, test_signal: "custom_parameters.is_test_call" };
        }
        if (falseyFlag(cp.is_test_call) || falseyFlag(cp.isTestCall)) {
            return { call_kind: "NORMAL", is_test_call: false, test_signal: "custom_parameters.is_test_call" };
        }
    }

    const call = b._raw?.call || b.call || null;
    if (call && typeof call === "object") {
        let ccp = call.custom_parameters || call.customParameters || null;
        if (typeof ccp === "string") {
            try {
                ccp = JSON.parse(ccp);
            } catch {
                ccp = null;
            }
        }
        if (ccp && typeof ccp === "object") {
            if (truthyFlag(ccp.is_test_call) || truthyFlag(ccp.isTestCall)) {
                return { call_kind: "TEST", is_test_call: true, test_signal: "call.custom_parameters" };
            }
            if (falseyFlag(ccp.is_test_call) || falseyFlag(ccp.isTestCall)) {
                return { call_kind: "NORMAL", is_test_call: false, test_signal: "call.custom_parameters" };
            }
        }
    }

    return { call_kind: "UNKNOWN", is_test_call: null, test_signal: null };
}

/** Presence map — which identity / payload fields the client actually sent. */
function summarizeProvidedFields(body = {}) {
    const b = body && typeof body === "object" ? body : {};
    const data = b.data && typeof b.data === "object" ? b.data : null;
    const payload =
        (data && data.payload && typeof data.payload === "object" && data.payload) ||
        (b.payload && typeof b.payload === "object" && b.payload) ||
        null;

    const turns = Array.isArray(b.turns)
        ? b.turns
        : Array.isArray(b.conversation)
          ? b.conversation
          : Array.isArray(b.messages)
            ? b.messages
            : null;

    return {
        // Dialer / Ondial
        call_id: hasOwn(b, "call_id") || hasOwn(b, "call_unique_id") || hasOwn(b, "callUniqueId"),
        campaign_id: hasOwn(b, "campaign_id"),
        contact_id: hasOwn(b, "contact_id"),
        provider: hasOwn(b, "provider") || hasOwn(b, "telephony_provider"),
        is_test_call: hasOwn(b, "is_test_call") || hasOwn(b, "isTestCall"),
        // Twilio
        CallSid: hasOwn(b, "CallSid") || hasOwn(b, "call_sid") || hasOwn(b, "twilio_call_sid"),
        CallStatus: hasOwn(b, "CallStatus"),
        CallDuration: hasOwn(b, "CallDuration"),
        Timestamp: hasOwn(b, "Timestamp"),
        // Telnyx (flat or Mission Control envelope)
        call_control_id:
            hasOwn(b, "call_control_id") ||
            hasOwn(b, "telnyx_call_control_id") ||
            hasOwn(b, "callControlId") ||
            Boolean(payload?.call_control_id),
        event_type: Boolean(
            data?.event_type || b.event_type || b.name || payload?.event_type
        ),
        from: Boolean(payload?.from || b.from),
        to: Boolean(payload?.to || b.to),
        hangup_cause: Boolean(payload?.hangup_cause || b.hangup_cause),
        billed_duration: Boolean(
            payload?.billed_duration_secs ||
                payload?.billable_duration_secs ||
                b.billed_duration_secs
        ),
        // Conversation
        turns: Array.isArray(turns) && turns.length > 0,
        transcript: typeof b.transcript === "string" && b.transcript.trim() !== "",
        turnCount: turns ? turns.length : 0,
        body_keys: Object.keys(b),
    };
}

function extractIdentities(body = {}) {
    const b = body && typeof body === "object" ? body : {};
    const data = b.data && typeof b.data === "object" ? b.data : null;
    const payload =
        (data && data.payload && typeof data.payload === "object" && data.payload) ||
        (b.payload && typeof b.payload === "object" && b.payload) ||
        null;

    return {
        call_id: pick(b, "call_id", "call_unique_id", "callUniqueId"),
        CallSid: pick(b, "CallSid", "call_sid", "twilio_call_sid"),
        call_control_id:
            pick(b, "call_control_id", "telnyx_call_control_id", "callControlId") ||
            payload?.call_control_id ||
            null,
        campaign_id: pick(b, "campaign_id"),
        contact_id: pick(b, "contact_id"),
        provider: pick(b, "provider", "telephony_provider"),
        event_type: data?.event_type || b.event_type || b.name || null,
        event_id: data?.id || b.event_id || b.id || null,
        CallStatus: pick(b, "CallStatus"),
        CallDuration: b.CallDuration != null ? b.CallDuration : null,
        from: payload?.from || b.from || null,
        to: payload?.to || b.to || null,
        hangup_cause: payload?.hangup_cause || b.hangup_cause || null,
        direction: payload?.direction || b.direction || null,
    };
}

/**
 * One consistent log line for Twilio/Telnyx API hits.
 *
 * @param {object} req Express request
 * @param {object} opts
 * @param {string} opts.provider  "twilio" | "telnyx" | "pool" | "unknown"
 * @param {string} opts.api       short name: call-status | webhooks | conversation | hangup | mapping
 * @param {string} [opts.expectedRoute] canonical route for this provider (e.g. /telnyx/webhooks)
 * @param {string} [opts.action]  received | forwarded | routed | processed
 * @param {object} [opts.body]
 * @param {object} [opts.extra]   merged into log (mapping, duration, is_test_call, call_kind)
 * @param {import("../logger")|Console} [opts.logger]
 */
function logProviderApiHit(req, opts = {}) {
    const {
        provider = "unknown",
        api = "unknown",
        expectedRoute = null,
        action = "received",
        body = {},
        extra = {},
        logger: log = console,
    } = opts;

    const hitRoute = req?.originalUrl || req?.url || null;
    const identities = extractIdentities(body);
    const provided = summarizeProvidedFields(body);
    const kind = resolveCallKind(body, extra);
    const misrouted =
        expectedRoute &&
        hitRoute &&
        String(hitRoute).split("?")[0] !== String(expectedRoute).split("?")[0];

    const dumpPayload =
        String(process.env.DEBUG_WEBHOOK_PAYLOAD || "").trim() === "1" ||
        String(process.env.DEBUG_WEBHOOK_PAYLOAD || "")
            .trim()
            .toLowerCase() === "true";

    // Keep call_kind / is_test_call first in the printed object for quick scanning.
    const { is_test_call: _dropIsTest, call_kind: _dropKind, test_signal: _dropSig, ...restExtra } =
        extra || {};

    const envelope = {
        call_kind: kind.call_kind,
        is_test_call: kind.is_test_call,
        test_signal: kind.test_signal,
        provider,
        api,
        action,
        method: req?.method || null,
        hit_route: hitRoute,
        expected_route: expectedRoute,
        misrouted: misrouted || false,
        ip: req?.ip || null,
        ...identities,
        // Keep explicit provider — body.provider is often missing and would overwrite to null.
        provider,
        fields_provided: provided,
        ...restExtra,
    };
    if (dumpPayload) {
        envelope.payload = cloneJsonSafe(body);
    }

    const providerTag = `[${String(provider).charAt(0).toUpperCase()}${String(provider).slice(1)}]`;
    const kindTag = `[${kind.call_kind}]`;
    const message = `${providerTag}${kindTag} API ${action} ${api} → ${hitRoute}${
        misrouted ? ` (expected ${expectedRoute})` : ""
    }`;

    if (typeof log.info === "function") {
        log.info(message, envelope);
    } else {
        console.log(message, envelope);
    }
    console.log(`\n${message}\n${formatJsonPretty(envelope)}\n`);

    return envelope;
}

module.exports = {
    logProviderApiHit,
    summarizeProvidedFields,
    extractIdentities,
    resolveCallKind,
    cloneJsonSafe,
    formatJsonPretty,
};
