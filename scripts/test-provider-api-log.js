/**
 * Unit checks for provider API hit logging helpers.
 * Run: node scripts/test-provider-api-log.js
 */
const assert = require("assert");
const {
  summarizeProvidedFields,
  extractIdentities,
  logProviderApiHit,
} = require("../lib/providerApiLog");

{
  const provided = summarizeProvidedFields({
    CallSid: "CAxxx",
    CallStatus: "completed",
    CallDuration: 42,
    Timestamp: "2020-01-01T00:00:00Z",
    campaign_id: "c1",
  });
  assert.strictEqual(provided.CallSid, true);
  assert.strictEqual(provided.CallStatus, true);
  assert.strictEqual(provided.call_control_id, false);
  assert.strictEqual(provided.campaign_id, true);
  assert.ok(provided.body_keys.includes("CallSid"));
}

{
  const provided = summarizeProvidedFields({
    data: {
      event_type: "call.hangup",
      id: "evt-1",
      payload: {
        call_control_id: "v3:abc",
        from: "+1",
        to: "+91",
        hangup_cause: "normal_clearing",
        billed_duration_secs: 60,
      },
    },
  });
  assert.strictEqual(provided.call_control_id, true);
  assert.strictEqual(provided.event_type, true);
  assert.strictEqual(provided.hangup_cause, true);
  assert.strictEqual(provided.billed_duration, true);
  assert.strictEqual(provided.CallSid, false);
}

{
  const ids = extractIdentities({
    data: {
      event_type: "call.answered",
      payload: { call_control_id: "v3:xyz", to: "+91635" },
    },
  });
  assert.strictEqual(ids.call_control_id, "v3:xyz");
  assert.strictEqual(ids.event_type, "call.answered");
  assert.strictEqual(ids.to, "+91635");
}

{
  const req = { method: "POST", originalUrl: "/twilio/call-status", ip: "127.0.0.1" };
  const logs = [];
  const fakeLogger = { info: (msg, data) => logs.push({ msg, data }) };
  const env = logProviderApiHit(req, {
    provider: "telnyx",
    api: "webhooks",
    expectedRoute: "/telnyx/webhooks",
    action: "forwarded",
    body: { data: { event_type: "call.initiated", payload: { call_control_id: "v3:1" } } },
    logger: fakeLogger,
  });
  assert.strictEqual(env.misrouted, true);
  assert.strictEqual(env.hit_route, "/twilio/call-status");
  assert.strictEqual(env.expected_route, "/telnyx/webhooks");
  assert.strictEqual(env.fields_provided.call_control_id, true);
  assert.ok(logs.length >= 1);
}

{
  const env = logProviderApiHit(
    { method: "POST", originalUrl: "/telnyx/conversation", ip: "127.0.0.1" },
    {
      provider: "telnyx",
      api: "conversation",
      expectedRoute: "/telnyx/conversation",
      action: "received",
      body: { call_control_id: "v3:1", is_test_call: true, turns: [{ role: "ai", text: "hi" }] },
      logger: { info() {} },
    }
  );
  assert.strictEqual(env.call_kind, "TEST");
  assert.strictEqual(env.is_test_call, true);
}

{
  const env = logProviderApiHit(
    { method: "POST", originalUrl: "/twilio/call-status", ip: "127.0.0.1" },
    {
      provider: "twilio",
      api: "call-status",
      expectedRoute: "/twilio/call-status",
      action: "received",
      body: { CallSid: "CAxx", CallStatus: "completed", Timestamp: "1" },
      logger: { info() {} },
      extra: { is_test_call: false, test_signal: "mapping" },
    }
  );
  assert.strictEqual(env.call_kind, "NORMAL");
  assert.strictEqual(env.is_test_call, false);
}

{
  const { resolveCallKind } = require("../lib/providerApiLog");
  assert.strictEqual(resolveCallKind({}, {}).call_kind, "UNKNOWN");
  assert.strictEqual(
    resolveCallKind({ custom_parameters: { is_test_call: true } }, {}).call_kind,
    "TEST"
  );
}

console.log("test-provider-api-log: ok");
