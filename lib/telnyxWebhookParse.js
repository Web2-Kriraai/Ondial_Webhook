/**
 * Telnyx Voice API webhook parsing + signature verification.
 * Aligns with https://developers.telnyx.com/development/api-fundamentals/webhooks/receiving-webhooks
 *
 * Supports both Voice shapes Telnyx documents:
 *  A) { data: { event_type, id, occurred_at, payload }, meta }
 *  B) { name, metadata: { event: { event_type, id, occurred_at, payload } }, call_leg_id, ... }
 */
const crypto = require("crypto");
const logger = require("../logger");

const STATUS_RANK = {
  ringing: 1,
  "in-progress": 2,
  busy: 3,
  "no-answer": 3,
  canceled: 3,
  cancelled: 3,
  failed: 3,
  completed: 3,
};

/** Events that drive contact / billing status (vs informational CallLog-only). */
const STATUS_EVENTS = new Set([
  "call.initiated",
  "call.answered",
  "call.bridged",
  "call.hangup",
  "call.rejected",
]);

const INFO_EVENT_PREFIXES = [
  "call.speak.",
  "call.playback.",
  "call.recording.",
  "call.transcription.",
  "call.conversation.",
  "call.dtmf.",
  "call.gather.",
  "call.cost",
  "call.ai.",
  "call.machine.",
  "streaming.",
  "conference.",
  "message.",
];

function getTelnyxPublicKey() {
  return String(process.env.TELNYX_PUBLIC_KEY || "").trim();
}

function isTelnyxSignatureRequired() {
  const flag = String(process.env.TELNYX_WEBHOOK_VERIFY || "").trim().toLowerCase();
  if (flag === "0" || flag === "false" || flag === "off") return false;
  return Boolean(getTelnyxPublicKey());
}

/**
 * Build SPKI DER KeyObject from Telnyx Mission Control public key (base64 raw ed25519).
 */
function createTelnyxPublicKeyObject(publicKeyBase64) {
  const raw = Buffer.from(String(publicKeyBase64 || "").trim(), "base64");
  if (raw.length === 32) {
    const spkiPrefix = Buffer.from("302a300506032b6570032100", "hex");
    return crypto.createPublicKey({
      key: Buffer.concat([spkiPrefix, raw]),
      format: "der",
      type: "spki",
    });
  }
  if (String(publicKeyBase64).includes("BEGIN PUBLIC KEY")) {
    return crypto.createPublicKey(publicKeyBase64);
  }
  return crypto.createPublicKey({
    key: raw,
    format: "der",
    type: "spki",
  });
}

/**
 * Verify Telnyx ed25519 webhook signature.
 * Headers: telnyx-signature-ed25519, telnyx-timestamp
 * Signed message: `${timestamp}|${rawBody}`
 *
 * @returns {{ ok: boolean, reason?: string, skipped?: boolean }}
 */
function verifyTelnyxWebhookSignature(req) {
  const publicKey = getTelnyxPublicKey();
  if (!publicKey) {
    return { ok: true, skipped: true, reason: "no_public_key" };
  }

  const signatureHeader =
    req.headers["telnyx-signature-ed25519"] ||
    req.headers["Telnyx-Signature-Ed25519"];
  const timestampHeader =
    req.headers["telnyx-timestamp"] || req.headers["Telnyx-Timestamp"];

  if (!signatureHeader || !timestampHeader) {
    return { ok: false, reason: "missing_signature_headers" };
  }

  const ts = String(timestampHeader).trim();
  const tsNum = Number(ts);
  if (!Number.isFinite(tsNum)) {
    return { ok: false, reason: "invalid_timestamp" };
  }

  const maxSkewSec = Math.max(
    60,
    Number(process.env.TELNYX_WEBHOOK_MAX_SKEW_SEC || 5 * 60) || 300
  );
  const nowSec = Math.floor(Date.now() / 1000);
  if (Math.abs(nowSec - tsNum) > maxSkewSec) {
    return { ok: false, reason: "timestamp_skew" };
  }

  const rawBody = req.rawBody
    ? Buffer.isBuffer(req.rawBody)
      ? req.rawBody.toString("utf8")
      : String(req.rawBody)
    : JSON.stringify(req.body || {});

  try {
    const keyObject = createTelnyxPublicKeyObject(publicKey);
    const message = Buffer.from(`${ts}|${rawBody}`, "utf8");
    const signature = Buffer.from(String(signatureHeader).trim(), "base64");
    const ok = crypto.verify(null, message, keyObject, signature);
    return ok ? { ok: true } : { ok: false, reason: "signature_mismatch" };
  } catch (err) {
    logger.warn("[Telnyx] Signature verify error", { error: err.message });
    return { ok: false, reason: "verify_error" };
  }
}

/**
 * Normalize Telnyx webhook body into a stable event object.
 */
function parseTelnyxWebhookBody(body) {
  const root = body && typeof body === "object" ? body : {};

  let eventNode = null;
  if (root.data && typeof root.data === "object" && (root.data.event_type || root.data.payload)) {
    eventNode = root.data;
  }

  if (
    !eventNode &&
    root.metadata &&
    typeof root.metadata === "object" &&
    root.metadata.event &&
    typeof root.metadata.event === "object"
  ) {
    eventNode = root.metadata.event;
  }

  if (!eventNode && (root.event_type || root.payload)) {
    eventNode = root;
  }

  eventNode = eventNode && typeof eventNode === "object" ? eventNode : {};
  const payload =
    eventNode.payload && typeof eventNode.payload === "object"
      ? eventNode.payload
      : {};

  const eventType = String(
    eventNode.event_type ||
      eventNode.eventType ||
      root.name ||
      root.event_type ||
      ""
  ).trim();

  const eventId = String(
    eventNode.id || root.id || payload.event_id || ""
  ).trim();

  const occurredAtRaw =
    eventNode.occurred_at ||
    root.event_timestamp ||
    root.occurred_at ||
    payload.end_time ||
    payload.start_time ||
    null;

  const occurredAtIso = (() => {
    if (!occurredAtRaw) return new Date().toISOString();
    const d = new Date(occurredAtRaw);
    return Number.isNaN(d.getTime()) ? new Date().toISOString() : d.toISOString();
  })();

  const callControlId = String(
    payload.call_control_id ||
      payload.callControlId ||
      eventNode.call_control_id ||
      root.call_control_id ||
      ""
  ).trim();

  const callLegId = String(
    payload.call_leg_id || root.call_leg_id || ""
  ).trim();
  const callSessionId = String(
    payload.call_session_id || root.call_session_id || ""
  ).trim();
  const connectionId = String(
    payload.connection_id || payload.connectionId || ""
  ).trim();
  const direction = String(payload.direction || "").trim().toLowerCase();
  const state = String(payload.state || "").trim().toLowerCase();
  const from = String(payload.from || "").trim();
  const to = String(payload.to || "").trim();
  const hangupCause = String(
    payload.hangup_cause || payload.hangupCause || ""
  ).trim();
  const hangupSource = String(
    payload.hangup_source || payload.hangupSource || ""
  ).trim();
  const clientState = payload.client_state || payload.clientState || null;

  const deliveryAttempt =
    Number(root?.meta?.attempt || root?.metadata?.attempt || 0) || null;

  return {
    eventType,
    eventId,
    occurredAtIso,
    payload,
    callControlId,
    callLegId,
    callSessionId,
    connectionId,
    direction,
    state,
    from,
    to,
    hangupCause,
    hangupSource,
    clientState,
    deliveryAttempt,
    rawEventNode: eventNode,
  };
}

function isInformationalTelnyxEvent(eventType) {
  const s = String(eventType || "").toLowerCase();
  if (!s) return true;
  if (STATUS_EVENTS.has(s)) return false;
  return INFO_EVENT_PREFIXES.some((p) => s === p || s.startsWith(p));
}

/**
 * Full CallLog + contact/billing work only for lifecycle events (and cost/recording).
 * streaming.* / speak / dtmf etc. are ack-only to cut DB load and retries noise.
 */
function shouldProcessTelnyxEventFully(eventType) {
  const s = String(eventType || "").trim().toLowerCase();
  if (!s) return false;
  if (STATUS_EVENTS.has(s)) return true;
  if (s === "call.cost" || s.startsWith("call.recording")) return true;
  return false;
}

/**
 * Map Telnyx event (+ hangup_cause) → Twilio-like status used by contact sync / SSE.
 */
function mapTelnyxEventToCallStatus(eventType, { hangupCause } = {}) {
  const s = String(eventType || "").trim().toLowerCase();
  const cause = String(hangupCause || "").trim().toLowerCase();

  if (s === "call.initiated") return "ringing";
  if (s === "call.answered" || s === "call.bridged") return "in-progress";

  if (s === "call.hangup") {
    if (cause === "user_busy" || cause === "call_rejected" || cause === "busy") {
      return "busy";
    }
    if (cause === "no_answer" || cause === "timeout" || cause === "unallocated_number") {
      return "no-answer";
    }
    if (cause === "originator_cancel" || cause === "cancelled" || cause === "canceled") {
      return "canceled";
    }
    if (cause === "normal_clearing" || cause === "normal_unspecified" || !cause) {
      return "completed";
    }
    if (cause.includes("fail") || cause === "network_out_of_order") {
      return "failed";
    }
    return "completed";
  }

  if (s === "call.rejected") return "failed";
  return null;
}

function statusRank(status) {
  const key = String(status || "").toLowerCase();
  return STATUS_RANK[key] || 0;
}

/**
 * Prefer higher-rank / terminal statuses when events arrive out of order.
 */
function preferTelnyxStatus(currentStatus, nextStatus) {
  const cur = String(currentStatus || "").toLowerCase() || null;
  const next = String(nextStatus || "").toLowerCase() || null;
  if (!next) return cur;
  if (!cur) return next;
  if (statusRank(next) >= statusRank(cur)) return next;
  return cur;
}

function durationSecFromTelnyxPayload(payload, answeredAtIso) {
  const end = payload?.end_time || payload?.endTime;
  const answer =
    payload?.answer_time ||
    payload?.answered_at ||
    payload?.answerTime ||
    answeredAtIso ||
    null;
  if (answer && end) {
    const a = new Date(answer).getTime();
    const b = new Date(end).getTime();
    if (Number.isFinite(a) && Number.isFinite(b) && b >= a) {
      return Math.max(0, Math.floor((b - a) / 1000));
    }
  }
  const explicit = Number(
    payload?.billable_duration_secs ??
      payload?.duration_secs ??
      payload?.CallDuration ??
      payload?.duration
  );
  if (Number.isFinite(explicit) && explicit >= 0) return Math.floor(explicit);
  const start = payload?.start_time || payload?.startTime;
  if (start && end) {
    const a = new Date(start).getTime();
    const b = new Date(end).getTime();
    if (Number.isFinite(a) && Number.isFinite(b) && b >= a) {
      return Math.max(0, Math.floor((b - a) / 1000));
    }
  }
  return null;
}

/** Best-effort public recording URL from call.recording.* payloads. */
function extractTelnyxRecordingUrl(payload = {}) {
  const candidates = [
    payload.recording_urls?.mp3,
    payload.recording_urls?.wav,
    payload.public_recording_urls?.mp3,
    payload.public_recording_urls?.wav,
    payload.recording_url,
    payload.recordingUrl,
    payload.download_url,
    payload.downloadUrl,
    payload.url,
  ];
  for (const c of candidates) {
    const s = String(c || '').trim();
    if (s.startsWith('http')) return s;
  }
  return null;
}

module.exports = {
  parseTelnyxWebhookBody,
  verifyTelnyxWebhookSignature,
  isTelnyxSignatureRequired,
  mapTelnyxEventToCallStatus,
  preferTelnyxStatus,
  statusRank,
  isInformationalTelnyxEvent,
  shouldProcessTelnyxEventFully,
  durationSecFromTelnyxPayload,
  extractTelnyxRecordingUrl,
  STATUS_EVENTS,
};
