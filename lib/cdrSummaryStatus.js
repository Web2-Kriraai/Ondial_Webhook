/**
 * Maps telephony CDR summary fields → contactprocessings.callReceiveStatus
 * (must stay aligned with Calling_system1 worker retry / completed branches).
 *
 * @param {{ Duration?: string|number, CallStatus?: string }} body
 * @returns {0|1|3}
 */
function mapCdrSummaryToReceiveStatus(body) {
  const Duration = body?.Duration;
  const CallStatus = body?.CallStatus;

  const dur = parseInt(String(Duration ?? ''), 10) || 0;
  const norm = String(CallStatus || '')
    .trim()
    .toUpperCase()
    .replace(/\s+/g, ' ');

  const terminalNotAnswered =
    norm === 'BUSY' ||
    norm === 'NO ANSWER' ||
    norm === 'NOANSWER' ||
    norm === 'FAILED' ||
    norm === 'CANCEL' ||
    norm === 'CANCELLED' ||
    norm === 'CONGESTION';
  const terminalAnswered = norm === 'ANSWER' || norm === 'ANSWERED';

  if (terminalNotAnswered) return 1;
  if (terminalAnswered) return 3;
  if (dur > 0) return 3;
  return 0;
}

module.exports = { mapCdrSummaryToReceiveStatus };
