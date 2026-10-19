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

// ---- real payload shape seen on dev (trailing-space key, Acefone's own campaign_id, ref_id = dial API call id) ----
const { applyMappingIdentity } = require("../lib/acefoneWebhook");
const real = {
    uuid: "6ad5c2a7854f3", call_to_number: "916353125194", caller_id_number: "918065936453",
    start_stamp: "2026-10-19 12:41:35", answer_stamp: "", end_stamp: "2026-10-19 12:41:41",
    billsec: "0", duration: "6", direction: "clicktocall", call_status: "missed",
    answered_agent_number: "", call_id: "SRVINF-BGN-SRV116-T5-1792393895.49308",
    billing_circle: "India", "customer_no_with_prefix ": "+916353125194",
    campaign_name: "AcefoneDialer", campaign_id: "777", hangup_cause_key: "NO_ANSWER",
    ref_id: "01a15300-5e80-7383-be69-b42ed217532e", custom_identifier: "",
};
assert(isAcefonePayload(real));
let r = normalizeAcefonePayload(real);
assert.strictEqual(r.event, "call_hangup");
assert.strictEqual(r.campaign_id, null, "Acefone's own campaign_id must not become ours");
assert.strictEqual(r.to, "+916353125194");
assert.strictEqual(r.callStatus, "NO ANSWER");
// worker alias maps the dial API id -> our call_unique_id/contact/campaign
const res = applyMappingIdentity(r, { call_unique_id: "ef2fa3f5-59e8-4ef0-a813-e86ea45da035", contact_id: "c1", campaign_id: "k1" });
assert(res.applied);
assert.strictEqual(r.call_unique_id, "ef2fa3f5-59e8-4ef0-a813-e86ea45da035");
assert.strictEqual(r.call_id, "ef2fa3f5-59e8-4ef0-a813-e86ea45da035");
assert.strictEqual(r.contact_id, "c1");
assert.strictEqual(r.campaign_id, "k1");
assert.strictEqual(r.provider_call_id, "SRVINF-BGN-SRV116-T5-1792393895.49308"); // Acefone call id kept
assert(!applyMappingIdentity(normalizeAcefonePayload(real), null).applied);
// JSON in custom_identifier carries our ids
r = normalizeAcefonePayload({ ...real, ref_id: undefined, custom_identifier: '{"call_unique_id":"cu9","contact_id":"c9","campaign_id":"k9"}' });
assert.strictEqual(r.call_unique_id, "cu9");
assert.strictEqual(r.campaign_id, "k9");
console.log("acefone real-payload + alias tests passed");

// ---- real Acefone log payload: custom_identifier is an OBJECT, billing_circle an object, hex id without dashes ----
const real2 = {
    uuid: "6ad5c9c23cc7c", call_to_number: "916353125194", caller_id_number: "8065936453",
    start_stamp: "2026-10-19 13:11:54", answer_stamp: "", end_stamp: "2026-10-19 13:11:57", billsec: "0",
    direction: "clicktocall", duration: "0", answered_agent_number: "", call_flow: [], recording_url: "",
    call_status: "missed", call_id: "SRVINF-BGN-SRV116-T5-1792395714.56376",
    billing_circle: { operator: "Reliance JIO", circle: "Gujarat" },
    "customer_no_with_prefix ": "+916353125194", campaign_name: "", campaign_id: "",
    hangup_cause_key: "INCOMPATIBLE_DESTINATION", hangup_cause_code: "88",
    ref_id: "01a1531c-1ecf-7271-9d13-3c2716d47a88",
    custom_identifier: { call_unique_id: "f235fad8991b4294a3496c814449a00e", contact_id: "6ad5c286d9242a96f3dea380" },
};
assert(isAcefonePayload(real2));
const n2 = normalizeAcefonePayload(real2);
assert.strictEqual(n2.call_unique_id, "f235fad8-991b-4294-a349-6c814449a00e");
assert.strictEqual(n2.call_id, "f235fad8-991b-4294-a349-6c814449a00e");
assert.strictEqual(n2.contact_id, "6ad5c286d9242a96f3dea380");
assert.strictEqual(n2.campaign_id, null);
assert.strictEqual(n2.event, "call_hangup");
assert.strictEqual(n2.callStatus, "FAILED");
const sm2 = summarizeAcefone(real2, n2);
assert.strictEqual(sm2.identity_from, "custom_parameters");
assert(!JSON.stringify(sm2).includes("[object Object]"));
console.log("acefone custom_identifier object tests passed");

// ---- AI-side "answered" API body -> call.answered event ----
const { buildAnsweredEventPayload } = require("../lib/acefoneWebhook");
let a = buildAnsweredEventPayload({
    call_unique_id: "827c021fefd348db9de36ea68fd0f7c4", contact_id: "c1", campaign_id: "k1",
    answered_at: "2026-10-19T13:39:02+05:30", to: "+919574661175",
});
assert(a.ok);
assert.strictEqual(a.payload.event, "call.answered");
assert.strictEqual(a.payload.call.customParameters.call_unique_id, "827c021f-efd3-48db-9de3-6ea68fd0f7c4");
assert.strictEqual(a.payload.call.customParameters.campaign_id, "k1");
assert.strictEqual(a.payload.call.answeredAt, "2026-10-19T08:09:02.000Z");
assert.strictEqual(a.payload.call.direction, "outbound");
assert(a.payload.call.id && a.payload.call.callStatus === "ANSWER");
// shape is exactly what the handler's nested-provider path expects
assert(typeof a.payload.event === "string" && a.payload.event.includes(".") && typeof a.payload.call === "object");
assert.strictEqual(buildAnsweredEventPayload({ contact_id: "c1" }).error, "call_unique_id_required");
// minimal body from the AI websocket start event: only call_unique_id (+ to / callSid)
const min = buildAnsweredEventPayload({
    call_unique_id: "9b564a3a304244478dc61eb44268c412", to: "+919574661175", provider_call_id: "SRVINF-BGN-SRV113-T9-1792401515.79528",
});
assert(min.ok);
assert.strictEqual(min.payload.call.customParameters.call_unique_id, "9b564a3a-3042-4447-8dc6-1eb44268c412");
assert(!("contact_id" in min.payload.call.customParameters));
assert.strictEqual(min.payload.call.to, "+919574661175");
assert.strictEqual(min.payload.call.id, "SRVINF-BGN-SRV113-T9-1792401515.79528");
assert(buildAnsweredEventPayload({ call_unique_id: "u" }).ok);
assert.strictEqual(buildAnsweredEventPayload({ call_unique_id: "u", contact_id: "c", answered_at: "nope" }).error, "answered_at_invalid");
assert(!("campaign_id" in buildAnsweredEventPayload({ call_unique_id: "u", contact_id: "c" }).payload.call.customParameters));
console.log("acefone answered-api tests passed");

// ---- ref_id + direction on the answered API ----
const full = buildAnsweredEventPayload({
    call_unique_id: "9b564a3a304244478dc61eb44268c412", to: "+919574661175",
    provider_call_id: "SRVINF-BGN-SRV113-T9-1792401515.79528",
    ref_id: "01a15374-a3b8-72bf-9c48-a35ab2684089", direction: "outbound",
});
assert(full.ok);
assert.strictEqual(full.payload.call.ref_id, "01a15374-a3b8-72bf-9c48-a35ab2684089");
assert.strictEqual(full.payload.call.direction, "outbound");
assert.strictEqual(buildAnsweredEventPayload({ call_unique_id: "u" }).payload.call.direction, "outbound"); // default
assert(!("ref_id" in buildAnsweredEventPayload({ call_unique_id: "u" }).payload.call));
assert.strictEqual(buildAnsweredEventPayload({ call_unique_id: "u", direction: "INBOUND" }).payload.call.direction, "inbound");
assert.strictEqual(buildAnsweredEventPayload({ call_unique_id: "u", direction: "sideways" }).error, "direction_invalid");
console.log("acefone answered-api ref_id/direction tests passed");
