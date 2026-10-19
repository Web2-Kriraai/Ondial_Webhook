/**
 * Acefone (India) webhook normalizer.
 * Docs: https://docs.acefone.in/docs/web-hooks
 *
 * Acefone payloads are flat, carry NO event name, and may arrive as JSON, form-encoded or GET query.
 * We infer the event from which fields are present and convert to the legacy flat shape that
 * handleEventWebhook() already understands (event, call_id, to, from, duration, contact_id, ...).
 *
 * Identity: Acefone echoes `ref_id` (the value sent on the dial API request) on every callback, so the
 * dialer must send ref_id = call_unique_id. Fallbacks: a `custom_parameters` JSON / flat
 * call_unique_id|contact_id|campaign_id keys added via Acefone's "Add header" custom params.
 * Optional override: custom param `ondial_event` = call_initiated|call_ringing|call_answered|call_hangup|call_failed.
 */
const { extractCustomParameters, pickNonEmpty } = require("./customParameters");

const VALID_EVENTS = new Set([
    "call_initiated",
    "call_ringing",
    "call_answered",
    "call_transfer",
    "call_hangup",
    "call_failed",
]);

// FreeSWITCH hangup causes → callStatus understood by the hangup handler (BUSY / NO ANSWER / FAILED → status 1).
const BUSY_CAUSES = new Set(["USER_BUSY", "NORMAL_CIRCUIT_CONGESTION"]);
const NO_ANSWER_CAUSES = new Set(["NO_ANSWER", "NO_USER_RESPONSE", "ORIGINATOR_CANCEL", "ALLOTTED_TIMEOUT", "CALL_REJECTED"]);
const FAILED_CAUSES = new Set([
    "UNALLOCATED_NUMBER", "NO_ROUTE_DESTINATION", "NETWORK_OUT_OF_ORDER", "DESTINATION_OUT_OF_ORDER",
    "RECOVERY_ON_TIMER_EXPIRE", "NORMAL_TEMPORARY_FAILURE", "SERVICE_UNAVAILABLE", "INVALID_NUMBER_FORMAT",
    "INCOMPATIBLE_DESTINATION", "NUMBER_CHANGED", "SUBSCRIBER_ABSENT",
]);

/** Acefone sends some keys with stray whitespace (e.g. "customer_no_with_prefix "). */
function trimKeys(body) {
    const out = {};
    for (const [k, v] of Object.entries(body && typeof body === "object" ? body : {})) {
        out[String(k).trim()] = v;
    }
    return out;
}

function isAcefonePayload(input) {
    if (!input || typeof input !== "object" || Array.isArray(input)) return false;
    const body = trimKeys(input);
    if (body.call && typeof body.call === "object") return false; // new-provider nested shape
    if (typeof body.event === "string") return false; // legacy/new provider shape
    const hasNumbers = body.call_to_number != null || body.caller_id_number != null;
    const hasId = body.uuid != null || body.call_id != null;
    const hasAcefoneMarker =
        body.customer_no_with_prefix != null ||
        body.customer_number_with_prefix != null ||
        body.billing_circle != null ||
        body.ref_id != null ||
        body.call_flow != null;
    return hasNumbers && hasId && hasAcefoneMarker;
}

function parseCustomIdentifier(v) {
    if (v && typeof v === "object" && !Array.isArray(v)) return v;
    const t = String(v ?? "").trim();
    return t.startsWith("{") ? t : null;
}

/** The dial API sends our call_unique_id UUID without dashes; restore the dashed form we store. */
function canonicalCallUniqueId(v) {
    const s = pickNonEmpty(v);
    if (s && /^[0-9a-f]{32}$/i.test(s)) {
        const h = s.toLowerCase();
        return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
    }
    return s;
}

function toSeconds(v) {
    if (v == null || v === "") return null;
    const n = Number(v);
    return Number.isFinite(n) && n >= 0 ? Math.round(n) : null;
}

function isAnsweredStatus(s) {
    const v = String(s || "").trim().toLowerCase();
    return v === "answered" || v === "answered by customer" || v === "answered by agent";
}

function inferEvent(body) {
    const override = pickNonEmpty(body.ondial_event, body.ondialEvent);
    if (override && VALID_EVENTS.has(override)) return override;

    const hasEnd = pickNonEmpty(body.end_stamp, body.end_time, body.hangup_cause, body.hangup_cause_key) != null;
    if (hasEnd) return "call_hangup";
    if (pickNonEmpty(body.disposition) != null) return "call_hangup"; // dialer disposition = call over
    if (pickNonEmpty(body.answered_agent_number, body.answer_agent_number) != null || isAnsweredStatus(body.call_status)) {
        return "call_answered";
    }
    if (pickNonEmpty(body.agent_number) != null) return "call_ringing"; // Dialed on Agent / Dialer connected
    return "call_initiated"; // Call received on Server / Department / IVR / DTMF
}

function normalizeAcefonePayload(body) {
    const raw = trimKeys(body);
    // Only read OUR ids from custom-parameter carriers. Acefone's own `campaign_id` / `campaign_name`
    // belong to its dialer campaign and must never be mistaken for an Ondial campaign id.
    const cp = extractCustomParameters(
        {
            custom_parameters: raw.custom_parameters ?? raw.customParameters,
            customParameters: raw.customParameters,
            call_unique_id: raw.call_unique_id,
            contact_id: raw.contact_id,
            lead_id: raw.lead_id,
        },
        // custom_identifier may carry the same JSON when set on the dial request
        { custom_parameters: parseCustomIdentifier(raw.custom_identifier) }
    );
    const providerCallId = pickNonEmpty(raw.call_id, raw.uuid);
    const callUniqueId = canonicalCallUniqueId(pickNonEmpty(cp.call_unique_id, raw.ref_id));
    const event = inferEvent(raw);

    const dirRaw = String(raw.direction || "").trim().toLowerCase();
    const direction = dirRaw === "inbound" ? "inbound" : dirRaw ? "outbound" : null; // click_to_call → outbound
    const customer = pickNonEmpty(
        raw.customer_no_with_prefix,
        raw.customer_number_with_prefix,
        raw.customer_number
    );
    // Inbound: DID is the dialed number (used for inbound billing); outbound: customer is the callee.
    const to = direction === "inbound" ? pickNonEmpty(raw.call_to_number) : pickNonEmpty(customer, raw.call_to_number);
    const from = direction === "inbound" ? pickNonEmpty(customer, raw.caller_id_number) : pickNonEmpty(raw.caller_id_number);

    const billsec = toSeconds(raw.billsec);
    const answered =
        isAnsweredStatus(raw.call_status) ||
        pickNonEmpty(raw.answer_stamp) != null ||
        (billsec != null && billsec > 0) ||
        event === "call_answered";

    // Human handoff: an agent (not the AI leg) picked up after the customer was already connected.
    // Acefone has no transfer event, so we derive it from the hangup's agent/bridge fields, or from an
    // explicit ondial_event=call_transfer on the "Call answered by Agent" webhook.
    const agentNumber = pickNonEmpty(raw.answered_agent_number, raw.answer_agent_number);
    const bridged = toSeconds(raw.bridged_duration);
    const transferred =
        event === "call_transfer" ||
        (event === "call_hangup" && direction !== "inbound" && agentNumber != null && bridged != null && bridged > 0);
    const transferTarget = transferred ? agentNumber : null;

    const cause = String(pickNonEmpty(raw.hangup_cause, raw.hangup_cause_key) || "").trim().toUpperCase();
    let callStatus = answered ? "ANSWER" : "NO ANSWER";
    if (!answered) {
        if (BUSY_CAUSES.has(cause)) callStatus = "BUSY";
        else if (FAILED_CAUSES.has(cause)) callStatus = "FAILED";
        else if (NO_ANSWER_CAUSES.has(cause)) callStatus = "NO ANSWER";
    }

    return {
        event,
        call_id: callUniqueId || providerCallId,
        to,
        from,
        // Talk time (billsec) is what we bill; `duration` includes ring time.
        duration: billsec,
        contact_id: cp.contact_id,
        campaign_id: cp.campaign_id,
        call_unique_id: callUniqueId,
        lead_id: pickNonEmpty(cp.lead_id, callUniqueId),
        provider_call_id: providerCallId,
        callStatus,
        answered,
        transferTarget,
        transferAt: transferred ? pickNonEmpty(raw.answer_stamp, raw.start_stamp) : null,
        ...(transferred && agentNumber
            ? {
                  legs: [
                      { role: "agent", to: agentNumber, answeredAt: pickNonEmpty(raw.answer_stamp), durationSec: bridged },
                  ],
              }
            : {}),
        direction,
        recordingUrl: pickNonEmpty(raw.recording_url, raw.aws_call_recording_identifier) || null,
        telephony_provider: "acefone",
        _raw: raw,
    };
}

function maskPhone(v) {
    const s = String(v ?? "").replace(/\s+/g, "");
    return s.length > 4 ? `${"*".repeat(Math.min(s.length - 4, 8))}${s.slice(-4)}` : s || null;
}

/**
 * One-line, grep-able summary for logs. Search logs for "[ACEFONE]" (stages: RECEIVED, REJECTED,
 * QUEUED, NORMALIZED, NO_IDENTITY). Phones are masked; raw payload is only logged on REJECTED.
 */
function summarizeAcefone(input, normalized = null) {
    const r = trimKeys(input);
    const out = {
        acefone_call_id: pickNonEmpty(r.call_id) || null,
        acefone_uuid: pickNonEmpty(r.uuid) || null,
        ref_id: pickNonEmpty(r.ref_id) || null,
        direction: pickNonEmpty(r.direction) || null,
        call_status: pickNonEmpty(r.call_status) || null,
        hangup_cause: pickNonEmpty(r.hangup_cause, r.hangup_cause_key) || null,
        reason_key: pickNonEmpty(r.reason_key) || null,
        custom_identifier: r.custom_identifier
            ? (typeof r.custom_identifier === "string"
                  ? r.custom_identifier
                  : JSON.stringify(r.custom_identifier)
              ).slice(0, 160) || null
            : null,
        billsec: pickNonEmpty(r.billsec) || null,
        to: maskPhone(pickNonEmpty(r.customer_no_with_prefix, r.customer_number_with_prefix, r.customer_number, r.call_to_number)),
        did: maskPhone(r.caller_id_number),
        has_recording: Boolean(pickNonEmpty(r.recording_url)),
    };
    if (normalized) {
        out.event = normalized.event;
        out.call_unique_id = normalized.call_unique_id || null;
        out.identity_from = normalized.call_unique_id
            ? pickNonEmpty(r.ref_id) && normalized.call_unique_id === String(r.ref_id).trim()
                ? "ref_id"
                : "custom_parameters"
            : "none";
        out.contact_id = normalized.contact_id || null;
        out.campaign_id = normalized.campaign_id || null;
        out.billable_sec = normalized.duration;
        out.callStatus = normalized.callStatus;
        out.transferTarget = maskPhone(normalized.transferTarget);
    }
    return out;
}

/**
 * Acefone echoes `ref_id`, which the dial API may set to ITS call id rather than our call_unique_id.
 * The worker registers a Redis alias (provider call id -> our call_unique_id + contact + campaign);
 * when `mapping` is that alias, rewrite the normalized identity to our canonical ids.
 */
function applyMappingIdentity(normalized, mapping) {
    const canonical = pickNonEmpty(mapping && mapping.call_unique_id);
    if (!normalized || !canonical) return { normalized, applied: false };
    normalized.provider_call_id = normalized.provider_call_id || normalized.call_unique_id || normalized.call_id;
    normalized.call_unique_id = canonical;
    normalized.call_id = canonical;
    normalized.lead_id = canonical;
    normalized.contact_id = normalized.contact_id || pickNonEmpty(mapping.contact_id);
    normalized.campaign_id = normalized.campaign_id || pickNonEmpty(mapping.campaign_id);
    return { normalized, applied: true };
}

/**
 * Acefone never delivers an "answered" webhook for our API-placed calls, so the AI calling service
 * (which sees the customer pick up on its media stream) posts the answer time to
 * POST /api/v1/webhooks/acefone/answered. We turn that small body into the same `call.answered`
 * event shape VoiceLink sends, so the normal handler marks the call as running (status 2).
 *
 * Minimum body: { "call_unique_id": "<from the websocket start event custom_identifier>" }.
 * Optional: answered_at (ISO, defaults to now), to, from, provider_call_id (callSid), ref_id (Acefone's
 * reference), direction (outbound|inbound, default outbound), contact_id, campaign_id. contact_id / campaign_id are normally NOT sent: they are resolved on our side from the
 * worker's Redis mapping keyed by call_unique_id (same mapping the hangup uses).
 */
function buildAnsweredEventPayload(input, now = new Date()) {
    const b = trimKeys(input);
    const callUniqueId = canonicalCallUniqueId(pickNonEmpty(b.call_unique_id, b.callUniqueId));
    const contactId = pickNonEmpty(b.contact_id, b.contactId);
    if (!callUniqueId) return { ok: false, error: "call_unique_id_required" };

    const rawAt = pickNonEmpty(b.answered_at, b.answeredAt);
    let answeredAt = now.toISOString();
    if (rawAt) {
        const d = new Date(rawAt);
        if (Number.isNaN(d.getTime())) return { ok: false, error: "answered_at_invalid" };
        answeredAt = d.toISOString();
    }

    const campaignId = pickNonEmpty(b.campaign_id, b.campaignId);
    const dir = String(pickNonEmpty(b.direction) || "outbound").toLowerCase();
    if (dir !== "outbound" && dir !== "inbound") return { ok: false, error: "direction_invalid" };
    const refId = pickNonEmpty(b.ref_id, b.refId);
    return {
        ok: true,
        payload: {
            event: "call.answered",
            call: {
                id: pickNonEmpty(b.provider_call_id, b.providerCallId) || callUniqueId,
                direction: dir,
                ...(refId ? { ref_id: refId } : {}),
                from: pickNonEmpty(b.from, b.from_number) || null,
                to: pickNonEmpty(b.to, b.to_number) || null,
                status: "answered",
                callStatus: "ANSWER",
                answeredAt,
                customParameters: {
                    call_unique_id: callUniqueId,
                    ...(contactId ? { contact_id: contactId } : {}),
                    ...(campaignId ? { campaign_id: campaignId } : {}),
                },
            },
        },
    };
}

/**
 * Conversation / transcript posts from the AI service for Acefone calls carry the call id in whatever
 * form the AI side holds: our dashless custom_identifier id, our dashed id, or Acefone's ref_id.
 * CallLogs are keyed by the DASHED call_unique_id, so resolve to that before the CallLog lookup:
 *   1. dashless 32-hex  -> dashed UUID
 *   2. worker alias (ref_id / provider call id) -> its call_unique_id
 * Ids that need no change (VoiceLink, already-dashed) are returned untouched.
 *
 * @param {string} key  normalized call id from the request
 * @param {(id: string) => Promise<object|null>} lookupMapping
 * @returns {Promise<{ callKey: string, changed: boolean, via: string|null }>}
 */
async function resolveConversationCallKey(key, lookupMapping) {
    const original = pickNonEmpty(key);
    if (!original) return { callKey: original, changed: false, via: null };
    let callKey = canonicalCallUniqueId(original);
    let via = callKey !== original ? "dashes_restored" : null;
    try {
        const alias = lookupMapping ? await lookupMapping(callKey) : null;
        const canonical = pickNonEmpty(alias && alias.call_unique_id);
        if (canonical && canonical !== callKey) {
            callKey = canonical;
            via = "ref_id_alias";
        }
    } catch (_err) {
        /* lookup failure must never block storing the conversation */
    }
    return { callKey, changed: callKey !== original, via };
}

/**
 * Last-resort identity: the Ondial test-call route (and some dial paths) register the worker mapping under
 * the dial API's call id, which is Acefone's ref_id, rather than under our call_unique_id. When there is no
 * mapping for call_unique_id, try those other ids. Only contact/campaign are copied; our call_unique_id stays.
 *
 * @param {object} normalized  normalized Acefone event (mutated)
 * @param {string[]} candidateIds  ids to try in order (ref_id, provider call id, ...)
 * @param {(id: string) => Promise<object|null>} lookupMapping
 * @returns {Promise<{ applied: boolean, via: string|null }>}
 */
async function applyRefIdMappingFallback(normalized, candidateIds, lookupMapping) {
    if (!normalized || normalized.contact_id) return { applied: false, via: null };
    for (const id of candidateIds || []) {
        const key = pickNonEmpty(id);
        if (!key) continue;
        let mapping = null;
        try {
            mapping = await lookupMapping(key);
        } catch (_err) {
            continue;
        }
        if (mapping && pickNonEmpty(mapping.contact_id)) {
            normalized.contact_id = pickNonEmpty(mapping.contact_id);
            normalized.campaign_id = normalized.campaign_id || pickNonEmpty(mapping.campaign_id);
            return { applied: true, via: key };
        }
    }
    return { applied: false, via: null };
}

module.exports = {
    applyRefIdMappingFallback,
    resolveConversationCallKey,
    buildAnsweredEventPayload,
    canonicalCallUniqueId,
    isAcefonePayload,
    normalizeAcefonePayload,
    inferEvent,
    summarizeAcefone,
    applyMappingIdentity,
    trimKeys,
};
