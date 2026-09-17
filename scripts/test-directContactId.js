const assert = require('node:assert/strict');

const {
  isDirectPhoneContactId,
  isDirectSessionContactId,
  classifyContactIdForCrs,
} = require('../lib/directContactId');
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

test('ObjectId → objectId strategy, no phone fallback', () => {
  const c = classifyContactIdForCrs('6aa90d9d41754e6d8cba8f1f');
  assert.equal(c.kind, 'objectId');
  assert.equal(c.requiresPhoneFallback, false);
  assert.equal(c.phoneDigits, null);
});

test('direct_<10digits> → direct_phone with digits', () => {
  assert.equal(isDirectPhoneContactId('direct_9408645627'), true);
  assert.equal(isDirectSessionContactId('direct_9408645627'), false);
  const c = classifyContactIdForCrs('direct_9408645627');
  assert.equal(c.kind, 'direct_phone');
  assert.equal(c.phoneDigits, '9408645627');
  assert.equal(c.requiresPhoneFallback, false);
});

test('direct_<hex session> → requires phone fallback', () => {
  const sid = 'direct_d5ddda8ea79341648c9c732601a3234c';
  assert.equal(isDirectPhoneContactId(sid), false);
  assert.equal(isDirectSessionContactId(sid), true);
  const c = classifyContactIdForCrs(sid);
  assert.equal(c.kind, 'direct_session');
  assert.equal(c.requiresPhoneFallback, true);
  assert.equal(c.phoneDigits, null);
});

test('empty / other → other', () => {
  assert.equal(classifyContactIdForCrs('').kind, 'other');
  assert.equal(classifyContactIdForCrs('not-an-id').kind, 'other');
  assert.equal(classifyContactIdForCrs('direct_').kind, 'other');
});

test('CRS=2 pool promote filter allows write when not final', () => {
  const oid = 'dummy_object_id';
  assert.deepEqual(buildCallReceiveStatusUpdateFilter({ oid, newStatus: 2 }), {
    _id: oid,
    callReceiveStatus: { $ne: 3 },
  });
});

test('CRS=2 blocked when current is already 3', () => {
  assert.equal(shouldBlockDowngradeFromFinal(3, 2), true);
});

console.log('All directContactId / CRS promote policy tests passed.');
