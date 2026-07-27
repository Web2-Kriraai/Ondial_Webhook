/**
 * Unit checks for call_id-first carrier resolve helpers.
 * Run: node scripts/test-call-id-first.js
 */
process.env.REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";
process.env.MONGODB_URI = process.env.MONGODB_URI || "mongodb://127.0.0.1:27017/ondial_test";

const assert = require("assert");
const {
    pickDialerCallId,
    hasCarrierId,
} = require("../lib/resolveCarrierFromCallId");
const {
    normalizeCallId,
    normalizeTwilioCallSid,
    normalizeTelnyxCallControlId,
} = require("../callMapping");

function check(label, fn) {
    try {
        fn();
        console.log(`  OK  ${label}`);
    } catch (err) {
        console.error(`  FAIL ${label}: ${err.message}`);
        process.exitCode = 1;
    }
}

console.log("--- call_id-first helpers ---\n");

check("pickDialerCallId prefers call_id", () => {
    assert.strictEqual(
        pickDialerCallId({ call_id: "abc", call_unique_id: "xyz" }),
        "abc"
    );
});

check("pickDialerCallId accepts call_unique_id", () => {
    assert.strictEqual(pickDialerCallId({ call_unique_id: "cid_uuid-1" }), "uuid-1");
});

check("hasCarrierId detects Twilio", () => {
    assert.strictEqual(hasCarrierId({ CallSid: "CAabc" }), true);
});

check("hasCarrierId detects Telnyx", () => {
    assert.strictEqual(hasCarrierId({ call_control_id: "v3:xyz" }), true);
});

check("hasCarrierId false for dialer-only body", () => {
    assert.strictEqual(hasCarrierId({ call_id: "uuid-1", turns: [] }), false);
});

check("normalize helpers", () => {
    assert.strictEqual(normalizeCallId("cid_x"), "x");
    assert.strictEqual(normalizeTwilioCallSid(" CA1 "), "CA1");
    assert.strictEqual(normalizeTelnyxCallControlId(" v3:a "), "v3:a");
});

if (process.exitCode) {
    console.error("\nSome checks failed");
    process.exit(1);
}
console.log("\nAll call_id-first helper checks passed");
