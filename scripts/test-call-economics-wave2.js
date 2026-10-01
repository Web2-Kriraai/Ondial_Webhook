/**
 * Wave 2 unit tests — call economics helpers (no Mongo required for core math).
 */
const assert = require("assert");
const {
    ceilToInterval,
    envEnabled,
} = require("../lib/callEconomics");

assert.strictEqual(ceilToInterval(1, 60), 60);
assert.strictEqual(ceilToInterval(60, 60), 60);
assert.strictEqual(ceilToInterval(61, 60), 120);
assert.strictEqual(ceilToInterval(0, 60), 0);

process.env.CALL_ECONOMICS_ENABLED = "0";
assert.strictEqual(envEnabled("CALL_ECONOMICS_ENABLED", false), false);
process.env.CALL_ECONOMICS_ENABLED = "1";
assert.strictEqual(envEnabled("CALL_ECONOMICS_ENABLED", false), true);
delete process.env.CALL_ECONOMICS_ENABLED;

// Pool must never be in default provider list parsing (indirect via module behavior)
const { maybeRecordCallEconomics } = require("../lib/callEconomics");
(async () => {
    process.env.CALL_ECONOMICS_ENABLED = "1";
    process.env.CALL_ECONOMICS_PROVIDERS = "twilio,telnyx";
    const r = await maybeRecordCallEconomics({
        db: {
            collection() {
                throw new Error("should not touch db for pool");
            },
        },
        callId: "x",
        provider: "pool",
        talkDurationSec: 60,
        customerChargeUsd: 1,
    });
    assert.strictEqual(r.skipped, true);
    assert.strictEqual(r.reason, "pool_excluded");

    process.env.CALL_ECONOMICS_ENABLED = "0";
    const r2 = await maybeRecordCallEconomics({
        db: null,
        callId: "y",
        provider: "twilio",
        talkDurationSec: 60,
        customerChargeUsd: 1,
    });
    assert.strictEqual(r2.skipped, true);
    assert.strictEqual(r2.reason, "disabled");

    console.log("test-call-economics-wave2: OK");
})().catch((err) => {
    console.error(err);
    process.exit(1);
});
