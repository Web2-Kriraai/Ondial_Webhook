const test = require("node:test");
const assert = require("node:assert/strict");
const {
  inboundFollowupUsesAi,
  disabledInboundFollowupAnalysisFeatures,
  buildInboundFollowupAnalysisFeatures,
  resolveInboundCallerEmail,
} = require("../lib/inbound/inboundFollowupAnalysisFeatures");

test("caller email: structured fields only, nested lead, invalid ignored", () => {
  assert.equal(resolveInboundCallerEmail({ callerEmail: "asha@example.com" }), "asha@example.com");
  assert.equal(resolveInboundCallerEmail({ lead: { email: "lead@example.in" } }), "lead@example.in");
  assert.equal(resolveInboundCallerEmail({ email: "not-an-email" }), "");
  assert.equal(resolveInboundCallerEmail(null), "");
  assert.equal(resolveInboundCallerEmail({}), "");
});

const config = {
  followup: true,
  followupSelection: "ai",
  followupChannels: ["whatsapp", "email"],
  followupWhatsappTemplateIds: ["w2", "w1"],
  followupEmailTemplateIds: ["e1"],
};
const whatsappTemplates = [
  { _id: "w1", name: "Order update", templateName: "order_update", bodyText: "Hi {{1}}, your {{2}} is ready.", sampleValues: ["Rahul", "order"] },
  { _id: "w2", name: "Brochure", templateName: "brochure", bodyText: "Hello, here is our brochure." },
  { _id: "w9", name: "Not selected", bodyText: "secret" },
];
const emailTemplates = [
  { _id: "e1", name: "Pricing", subject: "Pricing from {{company_name}}", body: "Hi {{contact_name}}, {{email_content}}" },
  { _id: "e9", name: "Not selected", subject: "s", body: "b" },
];
const DISABLED = {
  features_enabled: {
    is_followup_enabled: false,
    email_followup: { status: false },
    whatsapp_followup: { status: false },
    callback_scheduling: { status: false },
  },
};
const build = (over = {}) =>
  buildInboundFollowupAnalysisFeatures({
    config: { ...config, ...(over.config || {}) },
    emailTemplates,
    whatsappTemplates,
    caller: { name: "Yash Patel", phone: "+919913493053", ...(over.caller || {}) },
    companyName: "Acme",
    agentName: "Bhargav",
  });

test("AI mode only when follow-up is on and the choice is not 'all'", () => {
  assert.equal(inboundFollowupUsesAi(config), true);
  assert.equal(inboundFollowupUsesAi({ followup: true }), true);
  assert.equal(inboundFollowupUsesAi({ ...config, followupSelection: "all" }), false);
  assert.equal(inboundFollowupUsesAi({ ...config, followup: false }), false);
});

test("disabled shape: every feature off, no user_contact_info", () => {
  assert.deepEqual(disabledInboundFollowupAnalysisFeatures(), DISABLED);
  assert.equal("user_contact_info" in disabledInboundFollowupAnalysisFeatures(), false);
  assert.deepEqual(buildInboundFollowupAnalysisFeatures(), DISABLED);
});

test("follow-up off gives the disabled shape", () => {
  const r = build({ config: { followup: false } });
  assert.deepEqual(r, DISABLED);
  assert.equal("user_contact_info" in r, false);
});

test("selection 'all' gives the disabled shape", () => {
  const r = build({ config: { followupSelection: "all" } });
  assert.deepEqual(r, DISABLED);
  assert.equal("user_contact_info" in r, false);
});

test("offers only the selected templates, in the owner's order, in the outbound shape", () => {
  const r = build();
  assert.equal(r.features_enabled.is_followup_enabled, true);
  const wa = r.features_enabled.whatsapp_followup.templates;
  assert.deepEqual(wa.map((t) => t.id), ["w2", "w1"]);
  assert.deepEqual(Object.keys(wa[1]).sort(), ["campaign_name", "description", "id", "sample_values", "title"]);
  assert.equal(wa[1].description, "Hi {{1}}, your {{2}} is ready.");
  assert.equal(wa[1].campaign_name, "order_update");
  assert.deepEqual(r.features_enabled.email_followup.templates.map((t) => t.id), ["e1"]);
  assert.equal(JSON.stringify(r).includes("Not selected"), false, "unselected templates never reach the AI");
  assert.deepEqual(r.features_enabled.callback_scheduling, { status: false });
});

test("email placeholders the call already answers are filled; the rest is left for the AI", () => {
  const email = build().features_enabled.email_followup.templates[0];
  assert.equal(email.subject, 'Pricing from {{company_name:"Acme"}}');
  assert.equal(email.body, 'Hi {{contact_name:"Yash Patel"}}, {{email_content}}');
});

test("caller info becomes user_contact_info", () => {
  assert.deepEqual(build().user_contact_info, {
    contact_name: "Yash Patel",
    contact_mobile: "+919913493053",
    contact_email: "",
  });
});

test("a channel with no usable template is switched off; none at all gives the disabled shape", () => {
  assert.deepEqual(build({ config: { followupChannels: ["whatsapp"] } }).features_enabled.email_followup, { status: false });
  assert.deepEqual(build({ config: { followupChannels: ["email"], followupEmailTemplateIds: ["gone"] } }), DISABLED);
  assert.deepEqual(build({ config: { followupChannels: [] } }), DISABLED);
  assert.deepEqual(build({ config: { followupChannels: ["email", "whatsapp"], followupEmailTemplateIds: [], followupWhatsappTemplateIds: ["gone"] } }), DISABLED);
});

test("email only", () => {
  const r = build({ config: { followupChannels: ["email"] } });
  assert.equal(r.features_enabled.is_followup_enabled, true);
  assert.equal(r.features_enabled.email_followup.status, true);
  assert.deepEqual(r.features_enabled.email_followup.templates.map((t) => t.id), ["e1"]);
  assert.deepEqual(r.features_enabled.whatsapp_followup, { status: false });
  assert.deepEqual(r.features_enabled.callback_scheduling, { status: false });
  assert.ok(r.user_contact_info);
});

test("whatsapp only", () => {
  const r = build({ config: { followupChannels: ["whatsapp"] } });
  assert.equal(r.features_enabled.is_followup_enabled, true);
  assert.deepEqual(r.features_enabled.email_followup, { status: false });
  assert.equal(r.features_enabled.whatsapp_followup.status, true);
  assert.deepEqual(r.features_enabled.whatsapp_followup.templates.map((t) => t.id), ["w2", "w1"]);
  assert.deepEqual(r.features_enabled.callback_scheduling, { status: false });
  assert.ok(r.user_contact_info);
});

test("both channels (enabled shape unchanged)", () => {
  const r = build();
  assert.deepEqual(Object.keys(r).sort(), ["features_enabled", "user_contact_info"]);
  assert.deepEqual(Object.keys(r.features_enabled), [
    "is_followup_enabled",
    "email_followup",
    "whatsapp_followup",
    "callback_scheduling",
  ]);
  assert.equal(r.features_enabled.is_followup_enabled, true);
  assert.equal(r.features_enabled.email_followup.status, true);
  assert.equal(r.features_enabled.whatsapp_followup.status, true);
  assert.deepEqual(r.features_enabled.callback_scheduling, { status: false });
});
