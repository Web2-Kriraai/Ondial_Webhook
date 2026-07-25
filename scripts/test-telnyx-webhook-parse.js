/**
 * Quick unit checks for Telnyx webhook parse (no Redis/DB).
 * Run: node scripts/test-telnyx-webhook-parse.js
 */
const assert = require("assert");
const {
  parseTelnyxWebhookBody,
  mapTelnyxEventToCallStatus,
  preferTelnyxStatus,
  durationSecFromTelnyxPayload,
  isInformationalTelnyxEvent,
  extractTelnyxRecordingUrl,
} = require("../lib/telnyxWebhookParse");

// Shape A: { data: { event_type, payload } }
{
  const parsed = parseTelnyxWebhookBody({
    data: {
      record_type: "event",
      event_type: "call.initiated",
      id: "evt-1",
      occurred_at: "2018-02-02T22:25:27.521992Z",
      payload: {
        call_control_id: "cc-1",
        direction: "incoming",
        from: "+12025550133",
        to: "+12025550131",
      },
    },
    meta: { attempt: 1 },
  });
  assert.strictEqual(parsed.eventType, "call.initiated");
  assert.strictEqual(parsed.eventId, "evt-1");
  assert.strictEqual(parsed.callControlId, "cc-1");
  assert.strictEqual(parsed.direction, "incoming");
  assert.strictEqual(parsed.deliveryAttempt, 1);
}

// Shape B: metadata.event (Voice API top-level)
{
  const parsed = parseTelnyxWebhookBody({
    call_leg_id: "leg-1",
    call_session_id: "sess-1",
    name: "call.answered",
    type: "webhook",
    metadata: {
      attempt: 2,
      event: {
        event_type: "call.answered",
        id: "evt-2",
        occurred_at: "2019-11-10T22:26:27.521992Z",
        payload: {
          call_control_id: "v2:abc",
          start_time: "2019-11-10T22:26:26.521992Z",
        },
      },
    },
  });
  assert.strictEqual(parsed.eventType, "call.answered");
  assert.strictEqual(parsed.eventId, "evt-2");
  assert.strictEqual(parsed.callControlId, "v2:abc");
  assert.strictEqual(parsed.callLegId, "leg-1");
  assert.strictEqual(parsed.deliveryAttempt, 2);
}

assert.strictEqual(mapTelnyxEventToCallStatus("call.initiated"), "ringing");
assert.strictEqual(mapTelnyxEventToCallStatus("call.answered"), "in-progress");
assert.strictEqual(mapTelnyxEventToCallStatus("call.bridged"), "in-progress");
assert.strictEqual(mapTelnyxEventToCallStatus("call.hangup", { hangupCause: "normal_clearing" }), "completed");
assert.strictEqual(mapTelnyxEventToCallStatus("call.hangup", { hangupCause: "user_busy" }), "busy");
assert.strictEqual(mapTelnyxEventToCallStatus("call.hangup", { hangupCause: "no_answer" }), "no-answer");

assert.strictEqual(preferTelnyxStatus("ringing", "in-progress"), "in-progress");
assert.strictEqual(preferTelnyxStatus("completed", "ringing"), "completed");
assert.strictEqual(preferTelnyxStatus("in-progress", "completed"), "completed");

assert.strictEqual(isInformationalTelnyxEvent("call.speak.ended"), true);
assert.strictEqual(isInformationalTelnyxEvent("call.hangup"), false);

{
  const dur = durationSecFromTelnyxPayload(
    {
      answer_time: "2020-01-01T00:00:10.000Z",
      end_time: "2020-01-01T00:00:40.000Z",
    },
    null
  );
  assert.strictEqual(dur, 30);
}

assert.strictEqual(extractTelnyxRecordingUrl({
  recording_urls: { mp3: 'https://cdn.example.com/a.mp3' },
}), 'https://cdn.example.com/a.mp3');

console.log("test-telnyx-webhook-parse: ok");
