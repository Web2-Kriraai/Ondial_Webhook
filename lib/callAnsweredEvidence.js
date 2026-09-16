/**
 * Pure helpers: decide whether a CallLog / event slice proves the leg was answered.
 * Used by hangup/CDR paths and unit tests (no Mongo).
 */

function eventType(e) {
  return String(e?.event_type || e?.type || '').toLowerCase();
}

/**
 * Telephony / pool events that mean the A-leg reached answered / talking.
 * @param {Array} events sliced to this call leg when outbound
 */
function eventsShowAnsweredStage(events) {
  if (!Array.isArray(events) || !events.length) return false;
  return events.some((e) => {
    const ty = eventType(e);
    return (
      ty === 'call_answered' ||
      ty === 'call_transfer' ||
      ty === 'call.transfer' ||
      ty.includes('pool_conversation')
    );
  });
}

/**
 * Conversation turns on CallLog (India/pool or nested provider shapes).
 */
function callLogShowsConversationTurns(doc) {
  if (!doc || typeof doc !== 'object') return false;
  const counts = [
    Array.isArray(doc.conversation?.turns) ? doc.conversation.turns.length : 0,
    Array.isArray(doc.pool?.conversation?.turns) ? doc.pool.conversation.turns.length : 0,
    Array.isArray(doc.twilio?.conversation?.turns) ? doc.twilio.conversation.turns.length : 0,
    Array.isArray(doc.telnyx?.conversation?.turns) ? doc.telnyx.conversation.turns.length : 0,
  ];
  return Math.max(...counts) > 0;
}

/**
 * Combined evidence for hangup / CDR "did we answer?"
 */
function callLogShowsAnsweredEvidence(doc, { eventsSlice = null } = {}) {
  const events = Array.isArray(eventsSlice)
    ? eventsSlice
    : Array.isArray(doc?.call_data?.events)
      ? doc.call_data.events
      : [];
  if (eventsShowAnsweredStage(events)) return true;
  if (callLogShowsConversationTurns(doc)) return true;
  return false;
}

module.exports = {
  eventsShowAnsweredStage,
  callLogShowsConversationTurns,
  callLogShowsAnsweredEvidence,
};
