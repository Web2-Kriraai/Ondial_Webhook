/**
 * Sync contactprocessings from Telnyx Call Control events (parity with Twilio path).
 */
const {
  mapTwilioCallStatusToReceiveStatus,
  syncTwilioContactFromCall,
} = require("./twilioContactSync");
const { pickNonEmpty } = require("./customParameters");
const { mapTelnyxEventToCallStatus } = require("./telnyxWebhookParse");

/** @deprecated use mapTelnyxEventToCallStatus — kept for callers that pass raw status strings */
function mapTelnyxEventToStatus(eventTypeOrStatus) {
  const s = String(eventTypeOrStatus || "").trim().toLowerCase();
  const fromEvent = mapTelnyxEventToCallStatus(s);
  if (fromEvent) return fromEvent;
  if (s === "completed" || s === "in-progress" || s === "ringing") return s;
  if (
    s === "busy" ||
    s === "no-answer" ||
    s === "failed" ||
    s === "canceled" ||
    s === "cancelled"
  ) {
    return s === "cancelled" ? "canceled" : s;
  }
  return null;
}

function resolveTelnyxContactId({ telnyxMapping, body, storedDoc }) {
  return pickNonEmpty(telnyxMapping?.contact_id, body?.contact_id, storedDoc?.contact_id);
}

async function syncTelnyxContactFromCall({
  contactIdRaw,
  eventType,
  status,
  hangupCause,
  callControlId,
  source = "telnyx_webhook",
}) {
  const mapped =
    mapTelnyxEventToCallStatus(eventType, { hangupCause }) ||
    mapTelnyxEventToStatus(status) ||
    null;
  if (!mapped) {
    return { outcome: "skip_unmapped_status" };
  }
  return syncTwilioContactFromCall({
    contactIdRaw,
    twilioStatus: mapped,
    callSid: callControlId,
    source,
  });
}

module.exports = {
  mapTelnyxEventToStatus,
  mapTwilioCallStatusToReceiveStatus,
  resolveTelnyxContactId,
  syncTelnyxContactFromCall,
};
