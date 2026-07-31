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

module.exports = {
  shouldBlockDowngradeFromFinal,
  buildCallReceiveStatusUpdateFilter,
};

