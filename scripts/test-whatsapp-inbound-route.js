const assert = require("assert");
const {
  pickFollowupContact,
  isFollowupSessionActive,
} = require("../whatsapp/whatsappAiRelay");
const {
  summarizeMetaWhatsappPayload,
  shouldLogMetaWebhook,
  compactMetaWebhookLog,
} = require("../lib/metaWhatsappLogSummary");

const now = Date.parse("2026-08-18T12:00:00.000Z");
const campaignA = "6a842105cc816447b394e99a";
const campaignB = "6a842af8d47c172daa8dfc1e";

const contactA = {
  _id: "contact-a",
  campaignId: campaignA,
  whatsappQueueStatus: "WAITING_REPLY",
  whatsappStatus: "sent",
  lastWhatsappSentAt: "2026-08-18T11:44:10.740Z",
  lastCustomerWhatsappReplyAt: "2026-08-18T11:44:07.000Z",
  conversationWindowOpensUntil: "2026-08-19T11:44:07.000Z",
  updatedAt: "2026-08-18T11:44:12.820Z",
};

const contactB = {
  _id: "contact-b",
  campaignId: campaignB,
  whatsappQueueStatus: "WAITING_REPLY",
  whatsappStatus: "read",
  lastWhatsappSentAt: "2026-08-18T09:56:21.968Z",
  lastCustomerWhatsappReplyAt: "2026-08-18T09:57:16.000Z",
  conversationWindowOpensUntil: "2026-08-19T09:57:16.000Z",
  updatedAt: "2026-08-18T09:57:18.095Z",
};

const picked = pickFollowupContact([contactB, contactA], { campaignId: campaignB }, now);
assert.strictEqual(String(picked.campaignId), campaignA, "last WhatsApp send wins over older campaign");

const afterBSendsAgain = pickFollowupContact(
  [
    contactA,
    { ...contactB, lastWhatsappSentAt: "2026-08-18T11:50:00.000Z" },
  ],
  { campaignId: campaignA },
  now
);
assert.strictEqual(
  String(afterBSendsAgain.campaignId),
  campaignB,
  "newer template/AI send on the other campaign takes the next inbound"
);

assert.strictEqual(
  isFollowupSessionActive(contactA, { closedAt: new Date("2026-08-15T07:50:39.905Z") }, now),
  false,
  "closed session for that campaign does not stay active"
);
assert.equal(isFollowupSessionActive(contactA, { campaignId: campaignA, history: [{ role: "user" }] }, now), true);

const inboundPayload = {
  object: "whatsapp_business_account",
  entry: [
    {
      id: "1767875077538973",
      changes: [
        {
          field: "messages",
          value: {
            metadata: { phone_number_id: "1200331536506095", display_phone_number: "919825191846" },
            messages: [
              { from: "916353125194", type: "text", text: { body: "Okay" }, id: "wamid.long" },
            ],
          },
        },
      ],
    },
  ],
};
const inboundSummary = summarizeMetaWhatsappPayload(inboundPayload);
assert.equal(shouldLogMetaWebhook(inboundSummary), true);
const inboundLog = compactMetaWebhookLog(inboundSummary, { jobId: "job-1" });
assert.equal(inboundLog.from, "916353125194");
assert.equal(inboundLog.text, "Okay");
assert.equal(inboundLog.jobId, "job-1");
assert.equal(inboundLog.wabaId, undefined);
assert.equal(inboundLog.rawBytes, undefined);

const statusOnly = summarizeMetaWhatsappPayload({
  entry: [
    {
      changes: [
        {
          field: "messages",
          value: {
            metadata: { phone_number_id: "1200331536506095" },
            statuses: [{ id: "wamid.x", status: "read", recipient_id: "916353125194" }],
          },
        },
      ],
    },
  ],
});
assert.equal(shouldLogMetaWebhook(statusOnly), false);

console.log("ok: last-touch inbound campaign routing");
