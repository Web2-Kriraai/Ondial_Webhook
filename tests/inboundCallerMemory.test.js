const test = require("node:test");
const assert = require("node:assert/strict");
const {
  CALLER_MEMORY_MAX_CHARS,
  analysisMemoryFields,
  callerMemoryFilter,
  callerMemoryUpdate,
  callerNumberKey,
  nextOverallSummary,
  previousOverallFor,
  readOverallSummary,
  seedOverallFromConversations,
} = require("../lib/inbound/inboundCallerMemory");

const config = { _id: "cfgV3", lineageId: "lin1", userId: "owner1", companyId: "co1", category: "customer_support" };

test("memory key: owner, lineage, business, last 10 digits of the caller", () => {
  assert.deepEqual(callerMemoryFilter(config, "+918200773061"), {
    userId: "owner1",
    lineageKey: "lin1",
    businessKey: "company:co1",
    callerKey: "8200773061",
  });
  assert.equal(callerNumberKey("918200773061"), "8200773061");
  assert.equal(callerMemoryFilter(config, "withheld"), null);
});

test("first-time caller: no analysis fields", () => {
  assert.deepEqual(analysisMemoryFields(null), {});
  assert.equal(previousOverallFor(null, "c1"), null);
});

test("returning caller: previous_overall_summary + previous_calls_count", () => {
  const prev = previousOverallFor({ overallSummary: "Booked a visit for 1 Nov.", callsCount: 2, lastCallId: "c5" }, "c6");
  assert.deepEqual(analysisMemoryFields(prev), {
    previous_overall_summary: "Booked a visit for 1 Nov.",
    previous_calls_count: 2,
  });
});

test("retried analysis of the same call reuses the pre-call snapshot", () => {
  const prev = previousOverallFor(
    { overallSummary: "merged", callsCount: 3, lastCallId: "c6", previousOverallSummary: "old", previousCallsCount: 2 },
    "c6"
  );
  assert.equal(prev.summary, "old");
  assert.equal(callerMemoryUpdate({ previous: prev, next: { summary: "merged", source: "analysis" }, callId: "c6" }).$set.callsCount, 3);
});

test("analysis overall_summary wins; fallback appends a dated line", () => {
  assert.equal(readOverallSummary({ overall_summary: " merged " }), "merged");
  assert.deepEqual(
    nextOverallSummary({ previous: { summary: "x", callsCount: 1 }, analysisData: { overall_summary: "merged", conversationSummary: "y" } }),
    { summary: "merged", source: "analysis" }
  );
  const first = nextOverallSummary({ analysisData: { conversationSummary: "Asked about fees." }, callStartedAt: "2026-10-27T08:36:02Z" });
  assert.equal(first.summary, "27 Oct 2026, 02:06 pm IST: Asked about fees.");
  const second = nextOverallSummary({
    previous: { summary: first.summary, callsCount: 1 },
    analysisData: { conversationSummary: "Booked a visit." },
    callStartedAt: "2026-10-30T07:12:21Z",
  });
  assert.equal(second.summary.split("\n").length, 2);
  assert.equal(nextOverallSummary({ analysisData: {} }), null);
});

test("fallback stays within the cap, newest lines kept", () => {
  let prev = null;
  for (let i = 1; i <= 5; i += 1) {
    const next = nextOverallSummary({ previous: prev, analysisData: { conversationSummary: `${i} ${"x".repeat(900)}` }, callStartedAt: `2026-10-0${i}T05:00:00Z` });
    prev = { summary: next.summary, callsCount: i };
  }
  assert.ok(prev.summary.length <= CALLER_MEMORY_MAX_CHARS);
  assert.match(prev.summary, /: 5 x/);
});

test("seed: last 3 analysed calls, oldest first", () => {
  const seed = seedOverallFromConversations([
    { startedAt: new Date("2026-10-20T06:00:00Z"), conversationSummary: "A" },
    { startedAt: new Date("2026-10-25T06:00:00Z"), conversationSummary: "B" },
    { startedAt: new Date("2026-10-28T06:00:00Z"), conversationSummary: "C" },
    { startedAt: new Date("2026-10-29T06:00:00Z"), conversationSummary: "D" },
  ]);
  assert.equal(seed.callsCount, 3);
  assert.deepEqual(seed.summary.split("\n").map((l) => l.split(": ")[1]), ["B", "C", "D"]);
});
