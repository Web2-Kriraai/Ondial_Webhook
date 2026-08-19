const assert = require("assert");
const {
  pickFollowupContact,
  isFollowupSessionActive,
} = require("../whatsapp/whatsappAiRelay");
const {
  isWhatsappConversationWindowOpen,
} = require("../whatsapp/whatsappFollowupService");
const {
  summarizeMetaWhatsappPayload,
  shouldLogMetaWebhook,
  metaWebhookIngressLog,
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
const inboundLog = metaWebhookIngressLog(inboundSummary, { jobId: "job-1" });
assert.equal(inboundLog.message, "[MetaWhatsApp] inbound");
assert.equal(inboundLog.data.from, "916353125194");
assert.equal(inboundLog.data.text, "Okay");
assert.equal(inboundLog.data.jobId, "job-1");
assert.equal(inboundLog.data.failed, undefined);

const failedPayload = {
  entry: [
    {
      changes: [
        {
          field: "messages",
          value: {
            metadata: { phone_number_id: "1200331536506095" },
            statuses: [
              {
                id: "wamid.x",
                status: "failed",
                recipient_id: "919979710905",
                errors: [
                  {
                    code: 131049,
                    title: "This message was not delivered to maintain healthy ecosystem engagement.",
                  },
                ],
              },
            ],
          },
        },
      ],
    },
  ],
};
const failedSummary = summarizeMetaWhatsappPayload(failedPayload);
assert.equal(shouldLogMetaWebhook(failedSummary), true);
const failedLog = metaWebhookIngressLog(failedSummary, { jobId: "job-2" });
assert.equal(failedLog.level, "warn");
assert.equal(failedLog.message, "[MetaWhatsApp] delivery failed");
assert.equal(failedLog.data.from, undefined);
assert.equal(failedLog.data.text, undefined);
assert.equal(failedLog.data.failed[0].recipient, "919979710905");
assert.equal(failedLog.data.failed[0].errorCode, 131049);

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

assert.equal(
  isWhatsappConversationWindowOpen({
    conversationWindowOpensUntil: "2026-08-20T12:00:00.000Z",
  }, Date.parse("2026-08-19T12:00:00.000Z")),
  true,
  "open while conversationWindowOpensUntil is in the future"
);
assert.equal(
  isWhatsappConversationWindowOpen({
    lastCustomerWhatsappReplyAt: "2026-08-19T11:00:00.000Z",
  }, Date.parse("2026-08-19T12:00:00.000Z")),
  true,
  "open when customer replied within 24h"
);
assert.equal(
  isWhatsappConversationWindowOpen({
    lastCustomerWhatsappReplyAt: "2026-08-17T11:00:00.000Z",
  }, Date.parse("2026-08-19T12:00:00.000Z")),
  false,
  "closed when last customer reply is older than 24h"
);

console.log("ok: last-touch inbound campaign routing");
