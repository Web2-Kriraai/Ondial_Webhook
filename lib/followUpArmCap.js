/**
 * Per-contact follow-up call cap (shared convention with Calling_system1 / analysis repos).
 *
 * - cap = campaign.maxRetryAttempts: absent / null / NaN → 3 (schema default); else
 *   Math.max(1, floor(n)) — a requested callback is always honoured at least once.
 * - contact.followUpArmCount: absent → 0; $inc by 1 on every follow-up call arm, guarded in the
 *   update filter so concurrent arms cannot exceed the cap.
 * - When capped: push statusHistory { reason: 'follow-up-cap-reached', ... } and do not arm.
 */

const DEFAULT_MAX_FOLLOW_UP_CALLS_PER_CONTACT = 3;

function resolveMaxFollowUpCallsPerContact(campaign) {
  const raw = campaign?.maxRetryAttempts;
  if (raw == null || raw === "") return DEFAULT_MAX_FOLLOW_UP_CALLS_PER_CONTACT;
  const n = Number(raw);
  if (!Number.isFinite(n)) return DEFAULT_MAX_FOLLOW_UP_CALLS_PER_CONTACT;
  return Math.max(1, Math.floor(n));
}

function getFollowUpArmCount(contact) {
  const n = Number(contact?.followUpArmCount);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : 0;
}

function isFollowUpCapReached(contact, campaign) {
  return getFollowUpArmCount(contact) >= resolveMaxFollowUpCallsPerContact(campaign);
}

/**
 * Filter fragment: matches only while followUpArmCount (absent = 0) is below `cap`.
 * Not an explicit null — $inc on a null field throws.
 */
function followUpArmCapFilter(cap) {
  return { $or: [{ followUpArmCount: { $exists: false } }, { followUpArmCount: { $lt: cap } }] };
}

function buildFollowUpCapReachedHistory({ contact, campaign, analysisId = null, source }) {
  const status = String(contact?.status || "");
  return {
    fromStatus: status,
    toStatus: status,
    reason: "follow-up-cap-reached",
    analysisId,
    cap: resolveMaxFollowUpCallsPerContact(campaign),
    count: getFollowUpArmCount(contact),
    source,
    timestamp: new Date(),
  };
}

/**
 * Plan B (mirrors Calling_system1 shared-lib): a missed follow-up call gets its own retries.
 * One cycle = one armed callback, identified by lastFollowUpAt|scheduledAt (every arm path
 * writes one of them). Attempts are counted per cycle instead of lifetime callAttempts.
 */
function followUpCycleKey(contact) {
  const ms = (v) => {
    if (v == null || v === "") return "";
    const t = new Date(v).getTime();
    return Number.isFinite(t) ? String(t) : "";
  };
  const key = `${ms(contact?.lastFollowUpAt)}|${ms(contact?.scheduledAt)}`;
  return key === "|" ? "" : key;
}

/**
 * @param {object} contact
 * @param {number} maxRetries total attempts allowed (per cycle for follow-ups)
 * @param {number} lifetimeAttemptNumber this dial's number in callAttempts (non-follow-up rule)
 * @returns {{ isRetryable: boolean, followUp: null | { cycleKey: string, attemptNumber: number } }}
 */
function resolveUnansweredRetryDecision(contact, maxRetries, lifetimeAttemptNumber) {
  const max = Number.isFinite(Number(maxRetries)) ? Math.max(1, Number(maxRetries)) : 3;
  if (contact?.isFollowUp === true) {
    const cycleKey = followUpCycleKey(contact);
    // No cycle marker: cannot tell cycles apart — fall back to the lifetime rule.
    if (cycleKey) {
      const sameCycle = contact?.followUpAttemptCycle === cycleKey;
      const used = sameCycle ? Number(contact?.followUpAttemptCount) : 0;
      const attemptNumber = (Number.isFinite(used) && used > 0 ? Math.floor(used) : 0) + 1;
      return { isRetryable: attemptNumber < max, followUp: { cycleKey, attemptNumber } };
    }
  }
  return { isRetryable: Number(lifetimeAttemptNumber) < max, followUp: null };
}

/** $set fields recording a follow-up attempt. */
function followUpAttemptSetFields(followUp) {
  if (!followUp) return {};
  return {
    followUpAttemptCycle: followUp.cycleKey,
    followUpAttemptCount: followUp.attemptNumber,
  };
}

module.exports = {
  DEFAULT_MAX_FOLLOW_UP_CALLS_PER_CONTACT,
  resolveMaxFollowUpCallsPerContact,
  getFollowUpArmCount,
  isFollowUpCapReached,
  followUpArmCapFilter,
  buildFollowUpCapReachedHistory,
  followUpCycleKey,
  resolveUnansweredRetryDecision,
  followUpAttemptSetFields,
};
