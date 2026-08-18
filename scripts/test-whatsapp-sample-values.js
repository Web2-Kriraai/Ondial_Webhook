const assert = require("node:assert/strict");
const test = require("node:test");
const {
  buildWhatsappSampleValues,
  extractSampleValuesFromMetaComponents,
  mapWhatsappTemplateDocForAnalysis,
  resolveWhatsappTemplateDescription,
  resolveWhatsappTemplateSampleArray,
} = require("../lib/analysis_services/whatsappSampleValues");

test("buildWhatsappSampleValues uses platform preset for Meta followup_call_summary_en", () => {
  const out = buildWhatsappSampleValues(
    {
      templateName: "followup_call_summary_en",
      bodyText: "Hi {{1}}, thank you for speaking about {{2}}.",
      variableCount: 2,
      sampleValues: [],
    },
    { contactName: "Kaushik", companyName: "ShopSphere" }
  );
  assert.equal(out["{{1}}"], "Kaushik");
  assert.equal(out["{{2}}"], "your insurance plan");
});

test("buildWhatsappSampleValues fills from contact context when no preset", () => {
  const out = buildWhatsappSampleValues(
    {
      templateName: "custom_unknown_template",
      bodyText: "Hi {{1}}, from {{2}} — {{3}}",
      variableCount: 3,
    },
    { contactName: "Kaushik Sharma", companyName: "ShopSphere", agentName: "Prachi" }
  );
  assert.equal(out["{{1}}"], "Kaushik");
  assert.equal(out["{{2}}"], "ShopSphere");
  assert.equal(out["{{3}}"], "Prachi");
});

test("buildWhatsappSampleValues ignores placeholder-only stored values", () => {
  const out = buildWhatsappSampleValues(
    {
      templateName: "shopsphere_followup_en",
      bodyText: "Hi {{1}}, about {{2}}",
      variableCount: 2,
      sampleValues: ["—", "—"],
    },
    { contactName: "Kaushik" }
  );
  assert.equal(out["{{1}}"], "Kaushik");
  assert.equal(out["{{2}}"], "your recent order");
});

test("extractSampleValuesFromMetaComponents reads Meta example.body_text", () => {
  const samples = extractSampleValuesFromMetaComponents([
    {
      type: "BODY",
      text: "Hi {{1}}, order {{2}}",
      example: { body_text: [["Kaushik", "ORD-99"]] },
    },
  ]);
  assert.deepEqual(samples, ["Kaushik", "ORD-99"]);
});

test("resolveWhatsappTemplateSampleArray prefers non-empty DB sampleValues", () => {
  const arr = resolveWhatsappTemplateSampleArray({
    templateName: "followup_call_summary_en",
    bodyText: "Hi {{1}}, {{2}}",
    variableCount: 2,
    sampleValues: ["Kaushik", "Premium Plan pricing"],
  });
  assert.deepEqual(arr, ["Kaushik", "Premium Plan pricing"]);
});

test("resolveWhatsappTemplateDescription prefers body over Meta category labels", () => {
  assert.equal(
    resolveWhatsappTemplateDescription({
      description: "Meta MARKETING",
      bodyText: "Hi {{1}}, thanks for speaking about {{2}}.",
    }),
    "Hi {{1}}, thanks for speaking about {{2}}."
  );
  assert.equal(
    resolveWhatsappTemplateDescription({
      description: "Meta UTILITY",
      bodyText: "Your order {{1}} is confirmed.",
    }),
    "Your order {{1}} is confirmed."
  );
  assert.equal(
    resolveWhatsappTemplateDescription({
      description: "Meta Cloud API",
      bodyText: "Card offer for {{1}}",
    }),
    "Card offer for {{1}}"
  );
  assert.equal(
    resolveWhatsappTemplateDescription({
      description: "Optional - Product Details",
      bodyText: "",
    }),
    "Optional - Product Details"
  );
});

test("mapWhatsappTemplateDocForAnalysis does not send Meta MARKETING as description", () => {
  const mapped = mapWhatsappTemplateDocForAnalysis({
    _id: "6a8281aa6df1394812ba5145",
    templateName: "followup_call_summary_en",
    description: "Meta UTILITY",
    bodyText: "Hi {{1}}, thank you for speaking about {{2}}.",
    variableCount: 2,
    sampleValues: ["—", "—"],
  });
  assert.equal(mapped.description, "Hi {{1}}, thank you for speaking about {{2}}.");
  const samples = buildWhatsappSampleValues(mapped, { contactName: "Kaushik" });
  assert.equal(samples["{{1}}"], "Kaushik");
  assert.notEqual(samples["{{2}}"], "—");
});
