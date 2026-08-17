/**
 * Smoke test: Meta template status parse + lifecycle mapping (no Mongo).
 */
const assert = require("assert");
const {
  parseMetaTemplateStatusUpdates,
} = require("../whatsapp/metaTemplateStatusUpdates");
const { parseMetaStatusUpdates } = require("../whatsapp/metaStatusUpdates");
const { parseInboundMessages, isStopMessage } = require("../whatsapp/inboundParser");

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
assert.strictEqual(updates[0].metaTemplateId, "123456789");
assert.strictEqual(updates[0].templateName, "test_template");

const statusPayload = {
  entry: [
    {
      changes: [
        {
          value: {
            statuses: [{ id: "wamid.abc", status: "delivered" }],
          },
        },
      ],
    },
  ],
};
const statuses = parseMetaStatusUpdates(statusPayload);
assert.strictEqual(statuses.length, 1);
assert.strictEqual(statuses[0].status, "delivered");

const inboundPayload = {
  entry: [
    {
      changes: [
        {
          field: "messages",
          value: {
            messages: [
              {
                from: "916353125194",
                id: "wamid.xyz",
                timestamp: "1710000000",
                type: "text",
                text: { body: "Hii" },
              },
            ],
          },
        },
      ],
    },
  ],
};
const inbound = parseInboundMessages(inboundPayload);
assert.strictEqual(inbound.length, 1);
assert.strictEqual(inbound[0].phone, "916353125194");
assert.strictEqual(inbound[0].text, "Hii");
assert.strictEqual(isStopMessage("STOP"), true);

console.log("ok: meta inbound parse smoke");
