/**
 * Country-wise pricing (Layer 2) — E2E-style resolver checks for Ondial_Webhook.
 *
 * Offline (no Mongo/Redis). Mirrors Ondial's tests/country-pricing-resolver.test.mjs
 * for the CommonJS copy of the resolver, plus the outbound/inbound destination-phone
 * resolution used by app/api/webhooks/call-completed and the worker/inbound paths.
 *
 * Run: node scripts/test-country-pricing.js
 */
const assert = require("assert");
const {
    DEFAULT_COUNTRY_PRICING,
    PACKAGE_IDS,
    VOICE_TIERS,
    normalizeCountryIso,
    normalizeCountryPricingConfig,
    resolvePlanRate,
    resolveResourceCosts,
} = require("../lib/countryPricing");
const {
    countryIsoFromPhone,
    extractDestinationPhone,
    extractCallerPhone,
    resolveCountryBillingPhone,
    resolveDefaultCountryIso,
} = require("../lib/countryFromPhone");

let passed = 0;
function check(name, fn) {
    try {
        fn();
        passed++;
        console.log(`  ok - ${name}`);
    } catch (err) {
        console.error(`  FAIL - ${name}`);
        console.error(`         ${err.message}`);
        process.exitCode = 1;
    }
}

console.log("Country pricing (CommonJS) — resolver shape");
check("DEFAULT_COUNTRY_PRICING seeds IN/US/AU with enabled:true", () => {
    assert.strictEqual(DEFAULT_COUNTRY_PRICING.enabled, true);
    assert.deepStrictEqual(Object.keys(DEFAULT_COUNTRY_PRICING.countries).sort(), ["AU", "IN", "US"]);
});
check("PACKAGE_IDS / VOICE_TIERS match Ondial + Super-Admin", () => {
    assert.deepStrictEqual(PACKAGE_IDS, ["starter", "professional", "enterprise", "premium"]);
    assert.deepStrictEqual(VOICE_TIERS, ["standard", "premium", "elite"]);
});

console.log("\nCountry pricing (CommonJS) — resolvePlanRate");
check("IN/US/AU starter-standard match the user's worked examples", () => {
    assert.strictEqual(resolvePlanRate({ countryIso: "IN", packageId: "starter", voiceTier: "standard", fallbackRatePerMinute: 999 }).ratePerMinute, 0.055);
    assert.strictEqual(resolvePlanRate({ countryIso: "US", packageId: "starter", voiceTier: "standard", fallbackRatePerMinute: 999 }).ratePerMinute, 0.065);
    assert.strictEqual(resolvePlanRate({ countryIso: "AU", packageId: "starter", voiceTier: "standard", fallbackRatePerMinute: 999 }).ratePerMinute, 0.045);
});
check("unlisted country (GB) falls back to US per fallbackOrder", () => {
    const r = resolvePlanRate({ countryIso: "GB", packageId: "starter", voiceTier: "standard", fallbackRatePerMinute: 0.5 });
    assert.strictEqual(r.source, "fallback");
    assert.strictEqual(r.countryIso, "US");
    assert.strictEqual(r.ratePerMinute, 0.065);
});
check("kill switch (enabled:false) always returns the package/global default", () => {
    const disabled = normalizeCountryPricingConfig({ enabled: false });
    const r = resolvePlanRate({ config: disabled, countryIso: "IN", packageId: "starter", voiceTier: "standard", fallbackRatePerMinute: 0.42 });
    assert.strictEqual(r.source, "package");
    assert.strictEqual(r.ratePerMinute, 0.42);
});

console.log("\nCountry pricing (CommonJS) — resolveResourceCosts");
check("IN/US/AU concurrentCallCost + phoneNumberCost from seed matrix", () => {
    assert.strictEqual(resolveResourceCosts({ countryIso: "IN", fallbackPhoneNumberCost: 1, fallbackConcurrentCallCost: 1 }).phoneNumberCost, 4.9);
    assert.strictEqual(resolveResourceCosts({ countryIso: "US", fallbackPhoneNumberCost: 1, fallbackConcurrentCallCost: 1 }).phoneNumberCost, 7);
    assert.strictEqual(resolveResourceCosts({ countryIso: "AU", fallbackPhoneNumberCost: 1, fallbackConcurrentCallCost: 1 }).phoneNumberCost, 6);
});

console.log("\ncountryFromPhone — destination resolution for webhook billing (split model)");
check("countryIsoFromPhone resolves +91/+1/+61 correctly", () => {
    assert.strictEqual(countryIsoFromPhone("+919876543210"), "IN");
    assert.strictEqual(countryIsoFromPhone("+14155552671"), "US");
    assert.strictEqual(countryIsoFromPhone("+61255501234"), "AU");
});
check("extractDestinationPhone picks the callee ('to') field for outbound billing", () => {
    assert.strictEqual(extractDestinationPhone({ to_number: "+14155552671", from_number: "+919876543210" }), "+14155552671");
});
check("extractCallerPhone picks the caller ('from') field for inbound billing", () => {
    assert.strictEqual(extractCallerPhone({ to_number: "+919876543210", from_number: "+14155552671" }), "+14155552671");
});
check("resolveCountryBillingPhone: outbound uses callee, inbound uses caller", () => {
    const doc = { to_number: "+14155552671", from_number: "+919876543210" };
    assert.strictEqual(resolveCountryBillingPhone(doc, { inbound: false }), "+14155552671");
    assert.strictEqual(resolveCountryBillingPhone(doc, { inbound: true }), "+919876543210");
});
check("resolveDefaultCountryIso falls back to campaign.companyCountryIso, then infers from DID for inbound", () => {
    const campaign = { companyCountryIso: "US" };
    assert.strictEqual(resolveDefaultCountryIso({}, campaign, { inbound: false }), "US");
    const inboundDoc = { to_number: "+919876543210" };
    assert.strictEqual(resolveDefaultCountryIso(inboundDoc, campaign, { inbound: true }), "IN");
});

console.log("\nFull E2E scenario — IN account owning an IN + a US number, billing destination country");
check("outbound call FROM IN number TO US contact bills at US rate (destination wins over account/number)", () => {
    const callLog = { to_number: "+14155552671", from_number: "+919876543210" };
    const destinationPhone = resolveCountryBillingPhone(callLog, { inbound: false });
    const destinationIso = countryIsoFromPhone(destinationPhone, "IN");
    const rate = resolvePlanRate({ countryIso: destinationIso, packageId: "starter", voiceTier: "standard", fallbackRatePerMinute: 999 });
    assert.strictEqual(destinationIso, "US");
    assert.strictEqual(rate.ratePerMinute, 0.065);
});
check("outbound call FROM US number TO IN contact bills at IN rate (destination wins)", () => {
    const callLog = { to_number: "+919876543210", from_number: "+14155552671" };
    const destinationPhone = resolveCountryBillingPhone(callLog, { inbound: false });
    const destinationIso = countryIsoFromPhone(destinationPhone, "US");
    const rate = resolvePlanRate({ countryIso: destinationIso, packageId: "starter", voiceTier: "standard", fallbackRatePerMinute: 999 });
    assert.strictEqual(destinationIso, "IN");
    assert.strictEqual(rate.ratePerMinute, 0.055);
});
check("inbound call TO the US DID FROM an IN caller bills at IN rate (caller is the destination for inbound)", () => {
    const callLog = { to_number: "+14155552671", from_number: "+919876543210" };
    const destinationPhone = resolveCountryBillingPhone(callLog, { inbound: true });
    const destinationIso = countryIsoFromPhone(destinationPhone, "US");
    const rate = resolvePlanRate({ countryIso: destinationIso, packageId: "starter", voiceTier: "standard", fallbackRatePerMinute: 999 });
    assert.strictEqual(destinationIso, "IN");
    assert.strictEqual(rate.ratePerMinute, 0.055);
});

console.log(`\n${passed} check(s) passed.`);
if (process.exitCode) {
    console.error("\nSome checks FAILED.");
} else {
    console.log("All checks passed.");
}
