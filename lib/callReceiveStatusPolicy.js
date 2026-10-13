/**
 * callReceiveStatus monotonic contract:
 * - 3 (completed) must never be downgraded to 0/1/2 by late / duplicate webhooks.
 * - Follow-up / retry engines are responsible for resetting callReceiveStatus to 0
 *   before the next attempt; only then webhooks may write 1/2/3 again.
 */

function shouldBlockDowngradeFromFinal(currentCallReceiveStatus, newStatus) {
  return Number(currentCallReceiveStatus) === 3 && Number(newStatus) !== 3;
}

function buildCallReceiveStatusUpdateFilter({ oid, newStatus }) {
  const shouldBlockDowngrade = Number(newStatus) !== 3;
  return shouldBlockDowngrade
    ? { _id: oid, callReceiveStatus: { $ne: 3 } }
    : { _id: oid };
}

function toMs(value) {
  if (value == null || value === "") return null;
  const ms = value instanceof Date ? value.getTime() : typeof value === "number" ? value : Date.parse(value);
  return Number.isFinite(ms) ? ms : null;
}

/**
 * Is a carrier status / conversation event for a call that is no longer the contact's
 * current attempt? Late events must not touch status / scheduledAt / nextRetryAt /
 * callReceiveStatus / callAttempts (the CallLog itself is still updated by the caller).
 *
 * Signals (each needs a known call start; unknown → not stale, i.e. legacy behaviour):
 * - contact re-armed after this call started (lastFollowUpAt > callStartedAt)
 * - contact is a pending follow-up scheduled after this call started
 * - a newer carrier call was registered for this contact after this call started
 *
 * @returns {{ stale: boolean, reason?: string }}
 */
function isStaleCallEventForContact({ contact, callSid, callStartedAt, latestCall } = {}) {
  const startMs = toMs(callStartedAt);
  if (!contact || startMs == null) return { stale: false };

  const lastFollowUpMs = toMs(contact.lastFollowUpAt);
  if (lastFollowUpMs != null && lastFollowUpMs > startMs) {
    return { stale: true, reason: "contact_rearmed_after_call" };
  }

  const scheduledMs = toMs(contact.scheduledAt);
  if (
    contact.isFollowUp === true &&
    String(contact.status || "") === "pending" &&
    scheduledMs != null &&
    scheduledMs > startMs
  ) {
    return { stale: true, reason: "pending_follow_up_after_call" };
  }

  const sid = callSid != null ? String(callSid).trim() : "";
  const latestSid = latestCall?.carrier_call_id ? String(latestCall.carrier_call_id).trim() : "";
  const latestMs = toMs(latestCall?.registeredAt);
  if (sid && latestSid && latestSid !== sid && latestMs != null && latestMs > startMs) {
    return { stale: true, reason: "superseded_by_newer_call" };
  }

  return { stale: false };
}

module.exports = {
  shouldBlockDowngradeFromFinal,
  buildCallReceiveStatusUpdateFilter,
  isStaleCallEventForContact,
};

