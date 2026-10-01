/**
 * Wave 4 — live rate selection unit tests (default basis=did → no apply).
 */
const assert = require("assert");
const {
    resolveForeignLiveRateSelection,
    applyPriceFloor,
} = require("../lib/pricingShadow");

// applyPriceFloor off by default
{
    delete process.env.PRICE_FLOOR_ENABLED;
    const r = applyPriceFloor({ customerChargeUsd: 0.01, estimatedProviderCostUsd: 0.05, callId: "t" });
    assert.strictEqual(r.floored, false);
    assert.strictEqual(r.charge, 0.01);
}

process.env.PRICE_FLOOR_ENABLED = "1";
process.env.PRICE_FLOOR_ACTION = "log";
{
    const r = applyPriceFloor({ customerChargeUsd: 0.01, estimatedProviderCostUsd: 0.05, callId: "t" });
    assert.strictEqual(r.floored, false);
    assert.strictEqual(r.charge, 0.01);
    assert.strictEqual(r.logged, true);
}
process.env.PRICE_FLOOR_ACTION = "bump";
{
    const r = applyPriceFloor({ customerChargeUsd: 0.01, estimatedProviderCostUsd: 0.05, callId: "t" });
    assert.strictEqual(r.floored, true);
    assert.strictEqual(r.charge, 0.05);
}
delete process.env.PRICE_FLOOR_ENABLED;
delete process.env.PRICE_FLOOR_ACTION;

(async () => {
    process.env.PRICING_BASIS = "did";
    const r = await resolveForeignLiveRateSelection({
        db: { collection: () => ({ findOne: async () => null }) },
        user: {},
        campaign: { numberPolicySnapshot: { provider: "twilio" } },
        callLogDoc: { to_number: "+971501234567", from_number: "+14155550100" },
        liveDidRate: 0.08,
        liveDidIso: "US",
        provider: "twilio",
    });
    assert.strictEqual(r.applied, false);
    assert.strictEqual(r.basis, "did");

    // pool never applies
    const rPool = await resolveForeignLiveRateSelection({
        db: {},
        user: {},
        campaign: { numberPolicySnapshot: { provider: "pool" } },
        callLogDoc: {},
        liveDidRate: 0.05,
        provider: "pool",
    });
    assert.strictEqual(rPool.applied, false);
    assert.strictEqual(rPool.reason, "not_foreign");

    console.log("test-pricing-basis-wave4: OK");
})().catch((err) => {
    console.error(err);
    process.exit(1);
});
