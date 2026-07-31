const assert = require('node:assert/strict');

const {
  shouldBlockDowngradeFromFinal,
  buildCallReceiveStatusUpdateFilter,
} = require('../lib/callReceiveStatusPolicy');

function test(name, fn) {
  try {
    fn();
    console.log(`OK  ${name}`);
  } catch (e) {
    console.error(`FAIL ${name}`);
    throw e;
  }
}

test('shouldBlockDowngradeFromFinal blocks 3 -> 0/1/2', () => {
  assert.equal(shouldBlockDowngradeFromFinal(3, 0), true);
  assert.equal(shouldBlockDowngradeFromFinal(3, 1), true);
  assert.equal(shouldBlockDowngradeFromFinal(3, 2), true);
});

test('shouldBlockDowngradeFromFinal allows 3 -> 3', () => {
  assert.equal(shouldBlockDowngradeFromFinal(3, 3), false);
});

test('shouldBlockDowngradeFromFinal allows non-3 current', () => {
  assert.equal(shouldBlockDowngradeFromFinal(1, 3), false);
  assert.equal(shouldBlockDowngradeFromFinal(2, 1), false);
});

test('buildCallReceiveStatusUpdateFilter blocks downgrade by requiring callReceiveStatus != 3', () => {
  const oid = 'dummy_object_id';
  assert.deepEqual(buildCallReceiveStatusUpdateFilter({ oid, newStatus: 1 }), {
    _id: oid,
    callReceiveStatus: { $ne: 3 },
  });
});

test('buildCallReceiveStatusUpdateFilter allows upgrade writes when newStatus === 3', () => {
  const oid = 'dummy_object_id';
  assert.deepEqual(buildCallReceiveStatusUpdateFilter({ oid, newStatus: 3 }), { _id: oid });
});

