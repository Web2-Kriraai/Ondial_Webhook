const assert = require("node:assert/strict");
const test = require("node:test");
const { resolveCampaignIntlTimeZoneId } = require("../whatsapp/campaignIntlTimeZone");
const { buildWhatsappAiReplyPayload } = require("../whatsapp/buildWhatsappAiReplyPayload");

test("Windows IST campaign label maps to Asia/Kolkata", () => {
  assert.equal(
    resolveCampaignIntlTimeZoneId("(UTC+5:30) Chennai, Kolkata, Mumbai, New Delhi (IST)"),
    "Asia/Kolkata"
  );
});

test("WhatsApp AI payload current_time accepts campaign form timezone labels", () => {
  const wrapped = buildWhatsappAiReplyPayload({
    phone: "916353125194",
    message: "Hi",
    campaign: {
      timezone: "(UTC+5:30) Chennai, Kolkata, Mumbai, New Delhi (IST)",
    },
  });
  assert.equal(wrapped.payload.timezone, "Asia/Kolkata");
  assert.match(wrapped.payload.current_time, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$/);
  assert.equal(wrapped.payload.inbound_message, "Hi");
  assert.match(wrapped.payload.company.name, /\S/);
});

test("Python WhatsApp reply schema uses snake_case calls and non-empty company name", () => {
  const wrapped = buildWhatsappAiReplyPayload({
    phone: "916353125194",
    message: "Hii",
    campaign: {
      knowledgeBaseSummarized: "ShopSphere Online is a large multi-category e-commerce platform.",
      selectedServices: ["sales"],
      campaignServiceSubId: "lead_outreach",
    },
    callLogs: [
      {
        isTestCall: false,
        call_id: "ffd8ddba-0b3f-4caf-bc80-60e03add61a6",
        startedAt: "2026-08-18T09:11:11.209Z",
        conversation: {
          turns: [
            { role: "assistant", text: "Namaste" },
            { role: "user", text: "Haan" },
          ],
        },
      },
    ],
  });
  const payload = wrapped.payload;
  assert.equal(payload.company.name, "ShopSphere Online");
  assert.equal(payload.service_id, undefined);
  assert.equal(payload.call_conversation[0].call_id, "ffd8ddba-0b3f-4caf-bc80-60e03add61a6");
  assert.equal(payload.call_conversation[0].callId, undefined);
  assert.equal(payload.call_conversation[0].callStartedAt, undefined);
  assert.ok(payload.call_conversation[0].turns[0].timestamp);
  assert.ok(payload.call_conversation[0].turns[1].timestamp);
});

test("top-level call_id matches a call_conversation item", () => {
  const wrapped = buildWhatsappAiReplyPayload({
    phone: "916353125194",
    message: "Hello",
    session: { callId: "c80d64fc-dc78-4aee-9ec9-d311d0907b4d" },
    analysis: { call_id: "c80d64fc-dc78-4aee-9ec9-d311d0907b4d" },
    campaign: { companyName: "ShopSphere" },
    callLogs: [
      {
        isTestCall: false,
        call_id: "ffd8ddba-0b3f-4caf-bc80-60e03add61a6",
        conversation: { turns: [{ role: "user", text: "Hi", timestamp: "2026-08-18T09:11:11.209Z" }] },
      },
    ],
  });
  assert.equal(wrapped.payload.call_conversation[0].call_id, "ffd8ddba-0b3f-4caf-bc80-60e03add61a6");
  assert.equal(wrapped.payload.call_id, "ffd8ddba-0b3f-4caf-bc80-60e03add61a6");
});
