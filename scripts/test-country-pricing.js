/**
 * Country-wise pricing (Layer 2) — E2E-style resolver checks for Ondial_Webhook.
 *
 * Offline (no Mongo/Redis). Mirrors Ondial's tests/country-pricing-resolver.test.mjs
 * for the CommonJS copy of the resolver, plus number-based billing-phone resolution
 * (the OWNED number placing/receiving the call, not the other party) used by
 * app/api/webhooks/call-completed and lib/campaignCreditDeduction.js.
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

console.log("\ncountryFromPhone — number-based billing-phone resolution (OWNED number, not the other party)");
check("countryIsoFromPhone resolves +91/+1/+61 correctly", () => {
    assert.strictEqual(countryIsoFromPhone("+919876543210"), "IN");
    assert.strictEqual(countryIsoFromPhone("+14155552671"), "US");
    assert.strictEqual(countryIsoFromPhone("+61255501234"), "AU");
});
check("extractDestinationPhone picks the 'to' field — used as the DID for inbound, or the fallback line for outbound", () => {
    assert.strictEqual(extractDestinationPhone({ to_number: "+14155552671", from_number: "+919876543210" }), "+14155552671");
});
check("resolveCountryBillingPhone: outbound prefers campaign.selectedPhoneNumber (falls back to 'to'); inbound uses the DID ('to')", () => {
    const doc = { to_number: "+14155552671", from_number: "+919876543210" };
    // Outbound: campaign's own line wins over anything on the call log doc.
    assert.strictEqual(
        resolveCountryBillingPhone(doc, { inbound: false, campaign: { selectedPhoneNumber: "+919000000001" } }),
        "+919000000001"
    );
    // Outbound with no campaign line supplied: falls back to whatever's on the doc.
    assert.strictEqual(resolveCountryBillingPhone(doc, { inbound: false }), "+14155552671");
    // Inbound: always the DID (to_number), never the caller.
    assert.strictEqual(resolveCountryBillingPhone(doc, { inbound: true }), "+14155552671");
});
check("resolveDefaultCountryIso falls back to campaign.companyCountryIso, then infers from DID for inbound", () => {
    const campaign = { companyCountryIso: "US" };
    assert.strictEqual(resolveDefaultCountryIso({}, campaign, { inbound: false }), "US");
    const inboundDoc = { to_number: "+919876543210" };
    assert.strictEqual(resolveDefaultCountryIso(inboundDoc, campaign, { inbound: true }), "IN");
});

console.log("\nFull E2E scenario — IN account, campaign uses a +1 Twilio number to call a +91 contact (number wins, not destination)");
check("outbound call placed FROM the +1 campaign number TO a +91 contact bills at the US rate (number wins)", () => {
    const callLog = { to_number: "+919876543210", from_number: "+14155552671" };
    const campaign = { selectedPhoneNumber: "+14155552671" };
    const billingPhone = resolveCountryBillingPhone(callLog, { inbound: false, campaign });
    const billingIso = countryIsoFromPhone(billingPhone, "IN");
    const rate = resolvePlanRate({ countryIso: billingIso, packageId: "starter", voiceTier: "standard", fallbackRatePerMinute: 999 });
    assert.strictEqual(billingIso, "US");
    assert.strictEqual(rate.ratePerMinute, 0.065);
});
check("outbound call placed FROM the +91 campaign number TO a +1 contact bills at the IN rate (number wins)", () => {
    const callLog = { to_number: "+14155552671", from_number: "+919876543210" };
    const campaign = { selectedPhoneNumber: "+919876543210" };
    const billingPhone = resolveCountryBillingPhone(callLog, { inbound: false, campaign });
    const billingIso = countryIsoFromPhone(billingPhone, "US");
    const rate = resolvePlanRate({ countryIso: billingIso, packageId: "starter", voiceTier: "standard", fallbackRatePerMinute: 999 });
    assert.strictEqual(billingIso, "IN");
    assert.strictEqual(rate.ratePerMinute, 0.055);
});
check("inbound call TO the +1 US DID FROM an IN caller bills at the US rate (DID wins, not the caller)", () => {
    const callLog = { to_number: "+14155552671", from_number: "+919876543210" };
    const billingPhone = resolveCountryBillingPhone(callLog, { inbound: true });
    const billingIso = countryIsoFromPhone(billingPhone, "IN");
    const rate = resolvePlanRate({ countryIso: billingIso, packageId: "starter", voiceTier: "standard", fallbackRatePerMinute: 999 });
    assert.strictEqual(billingIso, "US");
    assert.strictEqual(rate.ratePerMinute, 0.065);
});

console.log(`\n${passed} check(s) passed.`);
if (process.exitCode) {
    console.error("\nSome checks FAILED.");
} else {
    console.log("All checks passed.");
}
