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
});
