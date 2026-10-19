/**
 * Resolve telephony provider from an ingress body.
 * Used by common /hangup and /conversation routes.
 * Canonical field is `provider`; `telephony_provider` is legacy-only.
 */
function resolveTelephonyProvider(body = {}) {
  const explicit = String(body.provider || body.telephony_provider || "")
    .trim()
    .toLowerCase();
  if (explicit === "telnyx" || explicit === "twilio") return explicit;
  // India carriers (voicelink, acefone) all share the pool flow.
  if (explicit === "pool" || explicit === "india" || explicit === "voicelink" || explicit === "acefone") {
    return "pool";
  }

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
