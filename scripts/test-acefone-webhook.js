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

// ---- conversation call-id resolution (async) ----
(async () => {
    const { resolveConversationCallKey } = require("../lib/acefoneWebhook");
    const aliases = { "01a15374-a3b8-72bf-9c48-a35ab2684089": { call_unique_id: "9b564a3a-3042-4447-8dc6-1eb44268c412" } };
    const lookup = async (id) => aliases[id] || null;
    let r = await resolveConversationCallKey("9b564a3a304244478dc61eb44268c412", lookup);
    assert.deepStrictEqual(r, { callKey: "9b564a3a-3042-4447-8dc6-1eb44268c412", changed: true, via: "dashes_restored" });
    r = await resolveConversationCallKey("01a15374-a3b8-72bf-9c48-a35ab2684089", lookup);
    assert.strictEqual(r.callKey, "9b564a3a-3042-4447-8dc6-1eb44268c412");
    assert.strictEqual(r.via, "ref_id_alias");
    r = await resolveConversationCallKey("9b564a3a-3042-4447-8dc6-1eb44268c412", lookup); // already ours
    assert.strictEqual(r.changed, false);
    r = await resolveConversationCallKey("some-voicelink-id", lookup); // untouched
    assert.deepStrictEqual(r, { callKey: "some-voicelink-id", changed: false, via: null });
    r = await resolveConversationCallKey("9b564a3a304244478dc61eb44268c412", async () => { throw new Error("redis down"); });
    assert.strictEqual(r.callKey, "9b564a3a-3042-4447-8dc6-1eb44268c412"); // lookup failure never blocks
    console.log("acefone conversation call-id tests passed");
})();

(async () => {
    const { applyRefIdMappingFallback } = require("../lib/acefoneWebhook");
    const store = { "01a1539a-09f8-7032-a67c-254dca0c82cb": { contact_id: "c77", campaign_id: "k77" } };
    const lookup = async (id) => store[id] || null;
    let n = { call_unique_id: "e675cb59-96e3-44d4-ba94-b634d450afe1", contact_id: null, campaign_id: null };
    let r = await applyRefIdMappingFallback(n, ["01a1539a-09f8-7032-a67c-254dca0c82cb", "SRVINF-x"], lookup);
    assert.strictEqual(r.applied, true);
    assert.strictEqual(n.contact_id, "c77");
    assert.strictEqual(n.campaign_id, "k77");
    assert.strictEqual(n.call_unique_id, "e675cb59-96e3-44d4-ba94-b634d450afe1"); // our id unchanged
    n = { contact_id: "already" };
    assert.strictEqual((await applyRefIdMappingFallback(n, ["01a1539a-09f8-7032-a67c-254dca0c82cb"], lookup)).applied, false);
    n = { contact_id: null };
    assert.strictEqual((await applyRefIdMappingFallback(n, ["nope", ""], lookup)).applied, false);
    assert.strictEqual((await applyRefIdMappingFallback({ contact_id: null }, ["x"], async () => { throw new Error("redis"); })).applied, false);
    console.log("acefone ref_id mapping fallback tests passed");
})();

// ---- duplicate hangup delivery shares one event key ----
{
    const { acefoneEventKey } = require("../lib/acefoneWebhook");
    const first = { ...real2, call_id: "SRVINF-BGN-SRV116-T5-1792404355.92119" };
    const second = { ...first, recording_name: "rec-123.mp3" }; // 2nd copy adds recording_name
    assert.strictEqual(acefoneEventKey(first), acefoneEventKey(second));
    assert.strictEqual(acefoneEventKey(first), "acefone:SRVINF-BGN-SRV116-T5-1792404355.92119:call_hangup");
    // a different event for the same call must NOT collide
    const ans = acefoneEventKey({ uuid: "u", call_id: "SRVINF-BGN-SRV116-T5-1792404355.92119", call_to_number: "1", caller_id_number: "2", billing_circle: "x", call_status: "answered by customer" });
    assert.notStrictEqual(ans, acefoneEventKey(first));
    assert.strictEqual(acefoneEventKey({ foo: 1 }), null);
    console.log("acefone duplicate-hangup event key tests passed");
}

(async () => {
    const { resolveCampaignFromContact } = require("../lib/acefoneWebhook");
    const fakeDb = (doc) => () => ({ collection: (n) => { assert.strictEqual(n, "contactprocessings"); return { findOne: async () => doc }; } });
    assert.strictEqual(await resolveCampaignFromContact(fakeDb({ campaignId: "6ad5f02963029591472bb6fc" }), "6ad5f0e463029591472bb88e"), "6ad5f02963029591472bb6fc");
    assert.strictEqual(await resolveCampaignFromContact(fakeDb(null), "6ad5f0e463029591472bb88e"), null);
    assert.strictEqual(await resolveCampaignFromContact(fakeDb({ campaignId: "x" }), "not-an-objectid"), null);
    assert.strictEqual(await resolveCampaignFromContact(() => { throw new Error("db down"); }, "6ad5f0e463029591472bb88e"), null);
    console.log("acefone contact->campaign resolution tests passed");
})();

// ---- AI-side transfer API body -> call.transfer event ----
{
    const { buildTransferEventPayload } = require("../lib/acefoneWebhook");
    const min = buildTransferEventPayload({ call_unique_id: "9b564a3a304244478dc61eb44268c412", transfer_target: "+919313212890" });
    assert(min.ok);
    assert.strictEqual(min.payload.event, "call.transfer");
    assert.strictEqual(min.payload.call.transferTarget, "+919313212890");
    assert.strictEqual(min.payload.call.customParameters.call_unique_id, "9b564a3a-3042-4447-8dc6-1eb44268c412");
    assert.strictEqual(min.payload.call.direction, "outbound");
    assert(typeof min.payload.call.transferAt === "string" && !Number.isNaN(new Date(min.payload.call.transferAt).getTime()));
    assert(!("contact_id" in min.payload.call.customParameters));
    assert(!("transferReason" in min.payload.call));
    const full = buildTransferEventPayload({
        call_unique_id: "9b564a3a-3042-4447-8dc6-1eb44268c412", transfer_target: "919313212890",
        transfer_at: "2026-10-22T14:05:09+05:30", reason: "user_requests_human",
        to: "+919574661175", ref_id: "01a15374-a3b8-72bf-9c48-a35ab2684089", provider_call_id: "SRVINF-1", direction: "outbound",
        contact_id: "c1", campaign_id: "k1",
    });
    assert(full.ok);
    assert.strictEqual(full.payload.call.transferAt, "2026-10-22T08:35:09.000Z");
    assert.strictEqual(full.payload.call.transferReason, "user_requests_human");
    assert.strictEqual(full.payload.call.ref_id, "01a15374-a3b8-72bf-9c48-a35ab2684089");
    assert.strictEqual(full.payload.call.customParameters.contact_id, "c1");
    assert.strictEqual(buildTransferEventPayload({ transfer_target: "+919313212890" }).error, "call_unique_id_required");
    assert.strictEqual(buildTransferEventPayload({ call_unique_id: "u" }).error, "transfer_target_required");
    assert.strictEqual(buildTransferEventPayload({ call_unique_id: "u", transfer_target: "12" }).error, "transfer_target_invalid");
    assert.strictEqual(buildTransferEventPayload({ call_unique_id: "u", transfer_target: "1234567890123456" }).error, "transfer_target_invalid");
    assert.strictEqual(buildTransferEventPayload({ call_unique_id: "u", transfer_target: "+919313212890", transfer_at: "nope" }).error, "transfer_at_invalid");
    assert.strictEqual(buildTransferEventPayload({ call_unique_id: "u", transfer_target: "+919313212890", direction: "sideways" }).error, "direction_invalid");
    // shape the handler's nested-provider path expects
    assert(min.payload.event.includes(".") && typeof min.payload.call === "object");
    console.log("acefone transfer-api tests passed");
}

// ---- transfer API accepts the AI service's own payload format ----
{
    const { buildTransferEventPayload } = require("../lib/acefoneWebhook");
    const aiPayload = {
        event: "call_transfer", status: "accepted", call_unique_id: "d31e02370bd7453ea0d3404c7bbc7e07",
        ref_id: "01a15374-a3b8-72bf-9c48-a35ab2684089", provider_call_id: "SRVINF-BGN-SRV113-T9-1792401245.78631",
        direction: "outbound", to: "+919574661175", target: "919313212890",
        provider_message: "Transfer succeeded", http_status: 200, occurred_at: "2026-10-22T10:32:52Z",
    };
    const ok = buildTransferEventPayload(aiPayload);
    assert(ok.ok && !ok.ignored);
    assert.strictEqual(ok.payload.event, "call.transfer");
    assert.strictEqual(ok.payload.call.transferTarget, "919313212890");
    assert.strictEqual(ok.payload.call.transferAt, "2026-10-22T10:32:52.000Z");
    assert.strictEqual(ok.payload.call.customParameters.call_unique_id, "d31e0237-0bd7-453e-a0d3-404c7bbc7e07");
    assert.strictEqual(ok.payload.call.id, "SRVINF-BGN-SRV113-T9-1792401245.78631");
    assert.strictEqual(ok.payload.call.to, "+919574661175");
    assert.strictEqual(ok.payload.call.transferProviderMessage, "Transfer succeeded");
    // no status at all behaves as accepted
    const { status, ...noStatus } = aiPayload;
    assert(buildTransferEventPayload(noStatus).ok);
    // failed / skipped are acknowledged but NEVER produce a transfer event
    for (const st of ["failed", "skipped"]) {
        const r = buildTransferEventPayload({ ...aiPayload, status: st, reason: "acefone_rejected", provider_message: "Rejected", http_status: 400 });
        assert(r.ok && r.ignored === true, st);
        assert.strictEqual(r.status, st);
        assert.strictEqual(r.reason, "acefone_rejected");
        assert(!("payload" in r));
    }
    // a failed event needs no target, but still needs the call id
    assert(buildTransferEventPayload({ call_unique_id: "u", status: "failed" }).ignored);
    assert.strictEqual(buildTransferEventPayload({ status: "failed" }).error, "call_unique_id_required");
    assert.strictEqual(buildTransferEventPayload({ call_unique_id: "u", status: "maybe", target: "919313212890" }).error, "status_invalid");
    assert.strictEqual(buildTransferEventPayload({ call_unique_id: "u", status: "ACCEPTED", target: "919313212890" }).ok, true);
    assert.strictEqual(buildTransferEventPayload({ call_unique_id: "u", status: "accepted" }).error, "transfer_target_required");
    console.log("acefone transfer-api AI-format tests passed");
}

// ---- talk time recovered from answer/end stamps when Acefone reports billsec 0 ----
{
    const dropped = {
        uuid: "u", call_id: "SRVINF-1", call_to_number: "916352617754", caller_id_number: "8065990635",
        billing_circle: "x", call_status: "answered", direction: "clicktocall",
        start_stamp: "2026-10-22 16:22:40", answer_stamp: "2026-10-22 16:22:58", end_stamp: "2026-10-22 16:23:43",
        billsec: "0", hangup_cause_key: "NETWORK_OUT_OF_ORDER", reason_key: "Calls dropped",
        ref_id: "r", custom_identifier: { call_unique_id: "cbb09ee7f1b64e18af5a68ec25dcade8", contact_id: "c1" },
    };
    let d = normalizeAcefonePayload(dropped);
    assert.strictEqual(d.duration, 45);
    assert.strictEqual(d.durationSource, "answer_end_stamps");
    assert.strictEqual(d.callStatus, "ANSWER");
    // the zone does not matter: both stamps share it
    d = normalizeAcefonePayload({ ...dropped, answer_stamp: "2026-10-22T10:52:58Z", end_stamp: "2026-10-22T10:53:43Z" });
    assert.strictEqual(d.duration, 45);
    d = normalizeAcefonePayload({ ...dropped, answer_stamp: "1792666378", end_stamp: "1792666423" }); // epoch seconds
    assert.strictEqual(d.duration, 45);
    // a real billsec always wins
    d = normalizeAcefonePayload({ ...dropped, billsec: "161" });
    assert.strictEqual(d.duration, 161);
    assert.strictEqual(d.durationSource, "billsec");
    // never answered (no answer_stamp): stays 0, never invents talk time
    d = normalizeAcefonePayload({ ...dropped, answer_stamp: "", call_status: "missed" });
    assert.strictEqual(d.duration, 0);
    assert.strictEqual(d.durationSource, "billsec");
    // end before answer, or no end stamp: ignored
    assert.strictEqual(normalizeAcefonePayload({ ...dropped, end_stamp: "2026-10-22 16:22:00" }).duration, 0);
    assert.strictEqual(normalizeAcefonePayload({ ...dropped, end_stamp: "" }).duration, 0);
    // sub-second answered call still bills one second
    assert.strictEqual(normalizeAcefonePayload({ ...dropped, answer_stamp: "2026-10-22T10:52:58.000Z", end_stamp: "2026-10-22T10:52:58.400Z" }).duration, 1);
    console.log("acefone talk-time recovery tests passed");
}

// Contact recovered from the call id the worker stamped on it (Redis mapping missing)
(async () => {
    const { resolveContactByActiveCall } = require("../lib/acefoneWebhook");
    const calls = [];
    const dbWith = (doc) => () => ({
        collection: (name) => ({
            findOne: async (filter, opts) => {
                calls.push({ name, filter, opts });
                return doc;
            },
        }),
    });
    const hit = await resolveContactByActiveCall(dbWith({ _id: "6adb0a5e1d3e268e32e1d9c3", campaignId: "6adb09afe01f06600dc4f9a3" }), "be1c5949-ebe3-4746-838e-26fa0bb7e16b");
    assert.deepStrictEqual(hit, { contact_id: "6adb0a5e1d3e268e32e1d9c3", campaign_id: "6adb09afe01f06600dc4f9a3" });
    assert.strictEqual(calls[0].name, "contactprocessings");
    assert.deepStrictEqual(calls[0].filter, { activeCallUniqueId: "be1c5949-ebe3-4746-838e-26fa0bb7e16b" });
    assert.deepStrictEqual(await resolveContactByActiveCall(dbWith({ _id: "c1" }), "x"), { contact_id: "c1", campaign_id: null });
    assert.strictEqual(await resolveContactByActiveCall(dbWith(null), "x"), null);
    assert.strictEqual(await resolveContactByActiveCall(dbWith({ _id: "c1" }), ""), null);
    assert.strictEqual(await resolveContactByActiveCall(dbWith({ _id: "c1" }), undefined), null);
    assert.strictEqual(await resolveContactByActiveCall(() => { throw new Error("db down"); }, "x"), null);
    console.log("acefone active-call contact recovery tests passed");
})().catch((e) => { console.error(e); process.exit(1); });
