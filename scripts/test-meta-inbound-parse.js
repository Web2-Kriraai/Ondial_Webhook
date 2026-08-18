/**
 * Smoke: slim WhatsApp AI payload + inbound parse scenarios (no Mongo).
 */
const assert = require("assert");
const { parseInboundMessages, isStopMessage } = require("../whatsapp/inboundParser");
const { buildWhatsappAiReplyPayload } = require("../whatsapp/buildWhatsappAiReplyPayload");
const {
  parseMetaTemplateStatusUpdates,
} = require("../whatsapp/metaTemplateStatusUpdates");
const { parseMetaStatusUpdates } = require("../whatsapp/metaStatusUpdates");

const templatePayload = {
  object: "whatsapp_business_account",
  entry: [
    {
      changes: [
        {
          field: "message_template_status_update",
          value: {
            event: "APPROVED",
            message_template_id: "123456789",
            message_template_name: "test_template",
            message_template_language: "en_US",
            message_template_category: "UTILITY",
          },
        },
      ],
    },
  ],
};

const updates = parseMetaTemplateStatusUpdates(templatePayload);
assert.strictEqual(updates.length, 1);
assert.strictEqual(updates[0].status, "active");

const statuses = parseMetaStatusUpdates({
  entry: [{ changes: [{ value: { statuses: [{ id: "wamid.abc", status: "delivered" }] } }] }],
});
assert.strictEqual(statuses[0].status, "delivered");

function metaMessages(messages) {
  return {
    entry: [{ changes: [{ field: "messages", value: { messages } }] }],
  };
}

const textInbound = parseInboundMessages(
  metaMessages([
    {
      from: "916353125194",
      id: "wamid.xyz",
      timestamp: "1710000000",
      type: "text",
      text: { body: "Hii" },
    },
  ])
);
assert.strictEqual(textInbound.length, 1);
assert.strictEqual(textInbound[0].phone, "916353125194");
assert.strictEqual(textInbound[0].text, "Hii");

const buttonInbound = parseInboundMessages(
  metaMessages([
    {
      from: "916353125194",
      id: "wamid.btn",
      type: "button",
      button: { text: "Yes, interested" },
    },
  ])
);
assert.strictEqual(buttonInbound[0].text, "Yes, interested");

const interactiveInbound = parseInboundMessages(
  metaMessages([
    {
      from: "916353125194",
      id: "wamid.list",
      type: "interactive",
      interactive: { list_reply: { title: "Premium Plan" } },
    },
  ])
);
assert.strictEqual(interactiveInbound[0].text, "Premium Plan");

const imageInbound = parseInboundMessages(
  metaMessages([
    {
      from: "916353125194",
      id: "wamid.img",
      type: "image",
      image: { caption: "Here is the invoice" },
    },
  ])
);
assert.strictEqual(imageInbound[0].text, "Here is the invoice");

const voiceInbound = parseInboundMessages(
  metaMessages([
    {
      from: "916353125194",
      id: "wamid.voice",
      type: "audio",
      audio: { id: "media-1" },
    },
  ])
);
assert.strictEqual(voiceInbound[0].text, "[Customer sent a voice message]");

const reactionSkipped = parseInboundMessages(
  metaMessages([
    {
      from: "916353125194",
      id: "wamid.react",
      type: "reaction",
      reaction: { emoji: "👍" },
    },
  ])
);
assert.strictEqual(reactionSkipped.length, 0);

const aisensyInbound = parseInboundMessages({
  type: "replied",
  phone: "916353125194",
  text: "Call me tomorrow",
  messageId: "ais-1",
});
assert.strictEqual(aisensyInbound[0].text, "Call me tomorrow");

assert.strictEqual(isStopMessage("STOP"), true);
assert.strictEqual(isStopMessage("OPT OUT"), true);

const payload = buildWhatsappAiReplyPayload({
  phone: "916353125194",
  message: "Yes, tell me the plan price",
  session: {
    sessionKey: "wa:916353125194:campaign:c1",
    callId: "CA123",
    analysisId: "a1",
    history: [
      { role: "assistant", text: "Hi Rahul from ShopSphere", timestamp: "2026-08-18T03:10:11.000Z" },
      { role: "user", text: "Yes, tell me the plan price", timestamp: "2026-08-18T03:12:40.000Z" },
    ],
  },
  campaign: { _id: "c1" },
  contact: { _id: "ct1" },
  analysis: { _id: "a1", call_id: "CA123" },
  callLogs: [
    {
      call_id: "CA111",
      createdAt: "2026-08-17T10:02:00.000Z",
      startedAt: "2026-08-17T10:02:00.000Z",
      endedAt: "2026-08-17T10:05:40.000Z",
      conversation: {
        turns: [{ AI: "Hi Rahul, calling from ShopSphere." }, { User: "Busy, call later." }],
      },
    },
    {
      call_id: "CA123",
      createdAt: "2026-08-18T02:58:04.000Z",
      startedAt: "2026-08-18T02:58:04.000Z",
      endedAt: "2026-08-18T03:04:22.000Z",
      conversation: {
        turns: [
          { AI: "Hi Rahul, Premium Plan is Rs 2,499 per month." },
          { User: "Yes, send the price on WhatsApp." },
        ],
      },
    },
  ],
});

assert.deepStrictEqual(Object.keys(payload).sort(), [
  "analysisId",
  "callConversation",
  "callEndedAt",
  "callId",
  "callStartedAt",
  "campaignId",
  "contactId",
  "history",
  "message",
  "phone",
  "sessionId",
  "task",
]);
assert.strictEqual(payload.task, "whatsapp_session_reply");
assert.strictEqual(payload.history.length, 2);
assert.strictEqual(payload.callConversation.length, 2);
assert.strictEqual(payload.callConversation[0].callId, "CA111");
assert.strictEqual(payload.callConversation[1].callId, "CA123");
assert.strictEqual(payload.callConversation[1].turns[0].role, "assistant");
assert.strictEqual(payload.callConversation[1].turns[1].role, "user");
assert.strictEqual(payload.callStartedAt, "2026-08-18T02:58:04.000Z");
assert.strictEqual(payload.analysis, undefined);
assert.strictEqual(payload.dialScriptExcerpt, undefined);

console.log("ok: meta inbound parse + slim AI payload smoke");
