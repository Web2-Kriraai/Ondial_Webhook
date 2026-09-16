const assert = require('node:assert/strict');

const {
  shouldBlockDowngradeFromFinal,
  buildCallReceiveStatusUpdateFilter,
} = require('../lib/callReceiveStatusPolicy');
const {
  eventsShowAnsweredStage,
  callLogShowsConversationTurns,
  callLogShowsAnsweredEvidence,
} = require('../lib/callAnsweredEvidence');

function test(name, fn) {
  try {
    fn();
    console.log(`OK  ${name}`);
  } catch (e) {
    console.error(`FAIL ${name}`);
    throw e;
  }
}

test('pool conversation CRS=2 filter allows write when not final', () => {
  const oid = 'dummy_object_id';
  assert.deepEqual(buildCallReceiveStatusUpdateFilter({ oid, newStatus: 2 }), {
    _id: oid,
    callReceiveStatus: { $ne: 3 },
  });
});

test('pool conversation CRS=2 blocked when current is already 3', () => {
  assert.equal(shouldBlockDowngradeFromFinal(3, 2), true);
});

test('eventsShowAnsweredStage true for pool_conversation_upserted', () => {
  assert.equal(
    eventsShowAnsweredStage([{ event_type: 'pool_conversation_upserted', data: { call_id: 'x' } }]),
    true
  );
});

test('eventsShowAnsweredStage true for call_answered / call_transfer', () => {
  assert.equal(eventsShowAnsweredStage([{ event_type: 'call_answered' }]), true);
  assert.equal(eventsShowAnsweredStage([{ type: 'call_transfer' }]), true);
});

test('eventsShowAnsweredStage false for dial-only events', () => {
  assert.equal(eventsShowAnsweredStage([{ event_type: 'call_initiated' }]), false);
  assert.equal(eventsShowAnsweredStage([]), false);
});

test('callLogShowsConversationTurns detects conversation / pool turns', () => {
  assert.equal(
    callLogShowsConversationTurns({
      conversation: { turns: [{ role: 'agent', text: 'hi' }] },
    }),
    true
  );
  assert.equal(
    callLogShowsConversationTurns({
      pool: { conversation: { turns: [{ role: 'user', text: 'yes' }] } },
    }),
    true
  );
  assert.equal(callLogShowsConversationTurns({ conversation: { turns: [] } }), false);
  assert.equal(callLogShowsConversationTurns({}), false);
});

test('callLogShowsAnsweredEvidence true when only pool turns (no answered event)', () => {
  const doc = {
    call_data: { events: [{ event_type: 'call_initiated' }] },
    pool: { conversation: { turns: [{ role: 'agent', text: 'hello' }] } },
  };
  assert.equal(callLogShowsAnsweredEvidence(doc), true);
});

test('callLogShowsAnsweredEvidence true when only pool_conversation_upserted event', () => {
  const doc = {
    call_data: {
      events: [{ event_type: 'pool_conversation_upserted', data: { turn_count: 2 } }],
    },
  };
  assert.equal(
    callLogShowsAnsweredEvidence(doc, {
      eventsSlice: doc.call_data.events,
    }),
    true
  );
});

test('callLogShowsAnsweredEvidence false with neither turns nor answer events', () => {
  const doc = {
    call_data: { events: [{ event_type: 'call_initiated' }] },
    conversation: { turns: [] },
  };
  assert.equal(callLogShowsAnsweredEvidence(doc), false);
});

console.log('All callAnsweredEvidence / CRS pool tests passed.');
