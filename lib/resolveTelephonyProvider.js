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
  if (explicit === "frejun") return "frejun";
  if (explicit === "pool" || explicit === "india") return "pool";

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
  const frejunId = String(
    body.frejun_call_id || body.call_id || body.data?.call_id || ""
  ).trim();
  if (frejunId.startsWith("cs_")) {
    return "frejun";
  }
  return null;
}

module.exports = {
  resolveTelephonyProvider,
};
