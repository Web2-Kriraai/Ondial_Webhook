/**
 * Resolve telephony provider from an ingress body.
 * Used by common /hangup and /conversation routes.
 */
function resolveTelephonyProvider(body = {}) {
  const explicit = String(body.provider || body.telephony_provider || "")
    .trim()
    .toLowerCase();
  if (explicit === "telnyx" || explicit === "twilio") return explicit;

  if (
    body.call_control_id ||
    body.telnyx_call_control_id ||
    body.callControlId
  ) {
    return "telnyx";
  }
  if (body.CallSid || body.call_sid || body.twilio_call_sid) {
    return "twilio";
  }
  return null;
}

module.exports = {
  resolveTelephonyProvider,
};
