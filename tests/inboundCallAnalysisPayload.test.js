/**
 * Smoke test for Ondial_Webhook inbound analysis payload builder (CJS).
 * Run from Ondial_Webhook: node --test tests/inboundCallAnalysisPayload.test.js
 */
const { describe, it, after } = require("node:test");
const assert = require("node:assert/strict");
const {
  mapInboundCallFeatures,
  buildInboundConversationText,
  buildInboundCallAnalysisPayload,
  shouldSkipInboundAnalysis,
} = require("../lib/inbound/buildInboundCallAnalysisPayload");
const {
  inboundAnalysisEnabled,
  buildInboundAnalysisUrl,
} = require("../lib/inbound/inboundAnalysisEnv");

describe("webhook inbound analysis payload", () => {
  it("maps healthcare features", () => {
    assert.deepEqual(
      mapInboundCallFeatures("healthcare_reception", {
        enableAppointments: true,
        enableLabReportInquiry: true,
      }),
      ["appointment_booking", "lab_report"]
    );
  });

  it("builds conversation text", () => {
    const text = buildInboundConversationText({
      conversation: {
        turns: [
          { role: "agent", text: "Hello" },
          { role: "user", text: "Hi" },
        ],
      },
    });
    assert.equal(text, "Agent: Hello, User: Hi");
  });

  it("skips empty transcript", () => {
    const r = shouldSkipInboundAnalysis({
      conversation: { conversation: { turns: [] } },
    });
    assert.equal(r.skip, true);
  });

  it("builds payload", () => {
    const result = buildInboundCallAnalysisPayload({
      config: {
        _id: "674a1b2c3d4e5f6789012341",
        name: "Aarav",
        category: "healthcare_reception",
        primaryLanguage: "en-IN",
        categoryConfig: {
          healthcare_reception: { enableAppointments: true },
        },
      },
      company: { name: "Apollo" },
      conversation: {
        call_id: "c1",
        conversation: { turns: [{ role: "user", text: "hi" }] },
      },
    });
    assert.equal(result.ok, true);
    assert.equal(result.payload.categoryConfig.call_feature[0], "appointment_booking");
  });
});

describe("inbound analysis env flags", () => {
  const prev = {
    ENABLED: process.env.INBOUND_ANALYSIS_ENABLED,
    PATH: process.env.INBOUND_ANALYSIS_PATH,
    INBOUND_URL: process.env.INBOUND_ANALYSIS_URL,
    URL: process.env.ANALYSIS_API_URL,
  };

  after(() => {
    for (const [k, v] of Object.entries({
      INBOUND_ANALYSIS_ENABLED: prev.ENABLED,
      INBOUND_ANALYSIS_PATH: prev.PATH,
      INBOUND_ANALYSIS_URL: prev.INBOUND_URL,
      ANALYSIS_API_URL: prev.URL,
    })) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  });

  it("respects INBOUND_ANALYSIS_ENABLED=0 kill switch", () => {
    process.env.INBOUND_ANALYSIS_ENABLED = "0";
    assert.equal(inboundAnalysisEnabled(), false);
  });

  it("uses INBOUND_ANALYSIS_URL full URL when set", () => {
    process.env.INBOUND_ANALYSIS_ENABLED = "1";
    process.env.INBOUND_ANALYSIS_URL =
      "https://foreignscript.ondial.ai/v1/analysis/inbound-call";
    delete process.env.INBOUND_ANALYSIS_PATH;
    assert.equal(
      buildInboundAnalysisUrl(),
      "https://foreignscript.ondial.ai/v1/analysis/inbound-call"
    );
  });

  it("builds URL from ANALYSIS_API_URL + relative INBOUND_ANALYSIS_PATH", () => {
    process.env.INBOUND_ANALYSIS_ENABLED = "1";
    delete process.env.INBOUND_ANALYSIS_URL;
    process.env.ANALYSIS_API_URL = "https://foreignscript.ondial.ai";
    process.env.INBOUND_ANALYSIS_PATH = "/v1/analysis/inbound-call";
    assert.equal(
      buildInboundAnalysisUrl(),
      "https://foreignscript.ondial.ai/v1/analysis/inbound-call"
    );
  });
});
