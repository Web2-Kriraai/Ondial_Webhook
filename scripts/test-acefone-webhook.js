const assert = require("node:assert");
const { isAcefonePayload, normalizeAcefonePayload } = require("../lib/acefoneWebhook");

const hangup = {
    uuid: "u-1", call_id: "ace-100", call_to_number: "919800000000", caller_id_number: "918000000001",
    customer_no_with_prefix: "+919800000000", direction: "outbound", call_status: "Answered",
    start_stamp: "2026-10-19 10:00:00", answer_stamp: "2026-10-19 10:00:05", end_stamp: "2026-10-19 10:01:05",
    hangup_cause: "NORMAL_CLEARING", billsec: "60", duration: "65", recording_url: "https://r/x.mp3",
    ref_id: "cuid-1", billing_circle: "x",
};
assert(isAcefonePayload(hangup));
let n = normalizeAcefonePayload(hangup);
assert.strictEqual(n.event, "call_hangup");
assert.strictEqual(n.call_id, "cuid-1");
assert.strictEqual(n.provider_call_id, "ace-100");
assert.strictEqual(n.duration, 60);
assert.strictEqual(n.callStatus, "ANSWER");
assert.strictEqual(n.to, "+919800000000");
assert.strictEqual(n.direction, "outbound");

const missed = { ...hangup, call_status: "Missed", answer_stamp: "", billsec: "0" };
n = normalizeAcefonePayload(missed);
assert.strictEqual(n.event, "call_hangup");
assert.strictEqual(n.answered, false);
assert.strictEqual(n.callStatus, "NO ANSWER");

const answered = { uuid: "u", call_id: "a", call_to_number: "1", caller_id_number: "2", answered_agent_number: "3", call_status: "Answered", ref_id: "c", billing_circle: "x", direction: "inbound" };
n = normalizeAcefonePayload(answered);
assert.strictEqual(n.event, "call_answered");
assert.strictEqual(n.to, "1"); // inbound: DID

const recv = { uuid: "u", call_id: "a", call_to_number: "1", caller_id_number: "2", start_stamp: "x", billing_circle: "x" };
assert.strictEqual(normalizeAcefonePayload(recv).event, "call_initiated");

const viaCp = normalizeAcefonePayload({ ...hangup, ref_id: undefined, custom_parameters: '{"call_unique_id":"cu2","contact_id":"c9","campaign_id":"k1"}' });
assert.strictEqual(viaCp.call_unique_id, "cu2");
assert.strictEqual(viaCp.contact_id, "c9");

assert(!isAcefonePayload({ event: "call.completed", call: {} }));
assert(!isAcefonePayload({ event: "call_hangup", call_id: "x", to: "1" }));
console.log("acefone webhook tests passed");

// transfer: customer answered, then agent bridged
n = normalizeAcefonePayload({ ...hangup, answered_agent_number: "919811111111", bridged_duration: "30" });
assert.strictEqual(n.transferTarget, "919811111111");
assert.strictEqual(n.legs.length, 1);
n = normalizeAcefonePayload({ ...answered, direction: "outbound", ondial_event: "call_transfer" });
assert.strictEqual(n.event, "call_transfer");
// busy / failed
n = normalizeAcefonePayload({ ...hangup, call_status: "Missed", answer_stamp: "", billsec: "0", hangup_cause: "USER_BUSY" });
assert.strictEqual(n.callStatus, "BUSY");
n = normalizeAcefonePayload({ ...hangup, call_status: "Missed", answer_stamp: "", billsec: "0", hangup_cause: "UNALLOCATED_NUMBER" });
assert.strictEqual(n.callStatus, "FAILED");
console.log("acefone transfer/cause tests passed");

const { summarizeAcefone } = require("../lib/acefoneWebhook");
const sm = summarizeAcefone(hangup, normalizeAcefonePayload(hangup));
assert.strictEqual(sm.identity_from, "ref_id");
assert.strictEqual(sm.event, "call_hangup");
assert(!JSON.stringify(sm).includes("9800000000")); // phone masked
assert.strictEqual(summarizeAcefone({ ...hangup, ref_id: undefined }, normalizeAcefonePayload({ ...hangup, ref_id: undefined })).identity_from, "none");
console.log("acefone log summary tests passed");
