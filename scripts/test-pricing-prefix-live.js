/**
 * Prefix-live destination deduct — unit tests (mock Mongo, no network).
 */
const assert = require("assert");
const {
    resolveForeignLiveRateSelection,
    sellFromCost,
    normalizeDestinationRateMode,
} = require("../lib/pricingShadow");
const { buildPrefixCandidates } = require("../lib/callEconomics");
const {
    destinationDigitsForPrefixLookup,
    isoToDialCode,
} = require("../lib/countryFromPhone");

assert.strictEqual(sellFromCost(0.22, 40), 0.308);
assert.strictEqual(sellFromCost(0.1651, 40), 0.23114);
assert.strictEqual(sellFromCost(null, 40), null);
assert.strictEqual(sellFromCost(-1, 40), null);

assert.strictEqual(normalizeDestinationRateMode("prefix"), "prefix");
assert.strictEqual(normalizeDestinationRateMode("country"), "country");
assert.strictEqual(normalizeDestinationRateMode("weird"), "country");

assert.strictEqual(isoToDialCode("IN"), "91");
assert.strictEqual(isoToDialCode("US"), "1");
assert.strictEqual(isoToDialCode("CA"), "1");
assert.strictEqual(isoToDialCode("AE"), "971");
assert.strictEqual(destinationDigitsForPrefixLookup("6353125194", "IN"), "916353125194");
assert.strictEqual(destinationDigitsForPrefixLookup("+916353125194", "IN"), "916353125194");
assert.strictEqual(destinationDigitsForPrefixLookup("0916353125194", "IN"), "916353125194");
assert.strictEqual(destinationDigitsForPrefixLookup("7473357058", "US"), "17473357058");
assert.strictEqual(destinationDigitsForPrefixLookup("+17473357058", "US"), "17473357058");
assert.strictEqual(destinationDigitsForPrefixLookup("501234567", "AE"), "971501234567");

{
    const c = buildPrefixCandidates("+971501234567");
    assert.ok(c.includes("971501234567"));
    assert.ok(c.includes("971"));
    assert.ok(c.includes("9"));
    assert.strictEqual(c[0].length >= c[c.length - 1].length, true);
}

function makeDb({ cards = [], countryPricing = null, commission = null } = {}) {
    return {
        collection(name) {
            if (name === "provider_rate_cards") {
                return {
                    find(filter) {
                        let rows = cards.filter((c) => {
                            if (filter.provider && c.provider !== filter.provider) return false;
                            if (filter.countryIso && c.countryIso !== filter.countryIso) return false;
                            if (filter.destinationPrefix?.$in) {
                                const pref = String(c.destinationPrefix || "").replace(/\D/g, "");
                                if (!filter.destinationPrefix.$in.includes(pref)) return false;
                            }
                            if (filter.effectiveFrom?.$lte) {
                                const from = new Date(c.effectiveFrom || 0);
                                if (from > filter.effectiveFrom.$lte) return false;
                            }
                            return true;
                        });
                        const api = {
                            project() {
                                return api;
                            },
                            sort(spec) {
                                if (spec?.rateUsdPerMin === -1) {
                                    rows = [...rows].sort(
                                        (a, b) => Number(b.rateUsdPerMin) - Number(a.rateUsdPerMin)
                                    );
                                }
                                return api;
                            },
                            limit(n) {
                                rows = rows.slice(0, n);
                                return api;
                            },
                            async toArray() {
                                return rows;
                            },
                            async next() {
                                return rows[0] || null;
                            },
                        };
                        return api;
                    },
                };
            }
            if (name === "systemsettings") {
                return {
                    async findOne({ key }) {
                        if (key === "countryPricingV1") {
                            return countryPricing != null ? { value: countryPricing } : null;
                        }
                        if (key === "providerCommissionV1") {
                            return commission != null
                                ? { value: commission }
                                : {
                                      value: {
                                          enabled: true,
                                          defaultPercent: 40,
                                          byProvider: { telnyx: 40, twilio: 40 },
                                          byCountry: {},
                                      },
                                  };
                        }
                        if (key === "pricingBasisV1") return null;
                        return null;
                    },
                };
            }
            return { findOne: async () => null };
        },
    };
}

const AE_CARDS = [
    {
        provider: "telnyx",
        countryIso: "AE",
        destinationPrefix: "97150",
        rateUsdPerMin: 0.1651,
        effectiveFrom: new Date("2020-01-01"),
    },
    {
        provider: "telnyx",
        countryIso: "AE",
        destinationPrefix: "971",
        rateUsdPerMin: 0.22,
        effectiveFrom: new Date("2020-01-01"),
    },
];

(async () => {
    process.env.PRICING_BASIS = "destination";
    process.env.PRICING_BASIS_PROVIDERS = "twilio,telnyx";
    process.env.PRICING_DESTINATION_RATE = "prefix";
    process.env.PRICING_MISSING_POLICY = "m1";
    delete process.env.PRICE_FLOOR_ENABLED;

    // 1) Longest prefix hit → cost × 1.40
    {
        const db = makeDb({ cards: AE_CARDS });
        const r = await resolveForeignLiveRateSelection({
            db,
            user: { creditPlan: { currentTier: "A" } },
            campaign: {
                numberPolicySnapshot: { provider: "telnyx" },
                selectedVoice: { tier: "standard" },
            },
            callLogDoc: {
                to_number: "+971501234567",
                from_number: "+14155550100",
                contact_phone: "+971501234567",
            },
            liveDidRate: 0.055,
            liveDidIso: "US",
            provider: "telnyx",
        });
        assert.strictEqual(r.applied, true);
        assert.strictEqual(r.rateSource, "prefix");
        assert.strictEqual(r.matchedPrefix, "97150");
        assert.strictEqual(r.providerCostUsdPerMin, 0.1651);
        assert.strictEqual(r.selectedRate, 0.23114);
        assert.strictEqual(r.destIso, "AE");
    }

    // 2) Prefix miss → country MAX × 1.40
    {
        const db = makeDb({ cards: AE_CARDS });
        const r = await resolveForeignLiveRateSelection({
            db,
            user: { creditPlan: { currentTier: "A" } },
            campaign: {
                numberPolicySnapshot: { provider: "telnyx" },
                selectedVoice: { tier: "standard" },
            },
            // Andorra-like digits but dest ISO forced via phone that won't match AE prefixes:
            // Use AE country via a number that starts with 971 but unknown longer prefix —
            // actually 971 alone matches destinationPrefix "971". Use country with cards that
            // don't match digits: phone in AE format that doesn't start with any stored prefix.
            callLogDoc: {
                to_number: "+971999999999",
                from_number: "+14155550100",
                contact_phone: "+971999999999",
            },
            liveDidRate: 0.055,
            liveDidIso: "US",
            provider: "telnyx",
        });
        // "971" is a prefix of 971999… so this still matches country prefix card.
        // For true miss, use cards that don't cover the dialled digits at all.
        assert.strictEqual(r.applied, true);
        assert.ok(r.rateSource === "prefix" || r.rateSource === "country_max");
        if (r.rateSource === "prefix") {
            assert.strictEqual(r.matchedPrefix, "971");
            assert.strictEqual(r.selectedRate, 0.308);
        }
    }

    // 3) True miss (no prefix match) → country_max
    {
        const cards = [
            {
                provider: "telnyx",
                countryIso: "AU",
                destinationPrefix: "61400",
                rateUsdPerMin: 0.01,
                effectiveFrom: new Date("2020-01-01"),
            },
            {
                provider: "telnyx",
                countryIso: "AU",
                destinationPrefix: "612999",
                rateUsdPerMin: 0.245,
                effectiveFrom: new Date("2020-01-01"),
            },
        ];
        const db = makeDb({ cards });
        const r = await resolveForeignLiveRateSelection({
            db,
            user: { creditPlan: { currentTier: "A" } },
            campaign: {
                numberPolicySnapshot: { provider: "telnyx" },
                selectedVoice: { tier: "standard" },
            },
            // +61 3… Melbourne landline — no matching prefix in cards
            callLogDoc: {
                to_number: "+61391234567",
                from_number: "+14155550100",
                contact_phone: "+61391234567",
            },
            liveDidRate: 0.055,
            liveDidIso: "US",
            provider: "telnyx",
        });
        assert.strictEqual(r.applied, true);
        assert.strictEqual(r.rateSource, "country_max");
        assert.strictEqual(r.matchedPrefix, null);
        assert.strictEqual(r.providerCostUsdPerMin, 0.245);
        assert.strictEqual(r.selectedRate, 0.343);
        assert.strictEqual(r.destIso, "AU");
    }

    // 4) No cards + M1 + missing matrix → refuse
    {
        const db = makeDb({
            cards: [],
            countryPricing: {
                enabled: true,
                countries: {},
                fallbackOrder: [],
            },
        });
        const r = await resolveForeignLiveRateSelection({
            db,
            user: { creditPlan: { currentTier: "A" } },
            campaign: {
                numberPolicySnapshot: { provider: "telnyx" },
                selectedVoice: { tier: "standard" },
            },
            callLogDoc: {
                to_number: "+971501234567",
                from_number: "+14155550100",
                contact_phone: "+971501234567",
            },
            liveDidRate: 0.055,
            liveDidIso: "US",
            provider: "telnyx",
        });
        assert.strictEqual(r.refuse, true, `expected M1 refuse, got ${JSON.stringify(r)}`);
        assert.strictEqual(r.reason, "missing_country_m1");
    }

    // 5) PRICING_DESTINATION_RATE=country → ISO matrix path (no prefix source)
    {
        process.env.PRICING_DESTINATION_RATE = "country";
        process.env.PRICING_MISSING_POLICY = "m2";
        const db = makeDb({ cards: AE_CARDS });
        const r = await resolveForeignLiveRateSelection({
            db,
            user: { creditPlan: { currentTier: "A" } },
            campaign: {
                numberPolicySnapshot: { provider: "telnyx" },
                selectedVoice: { tier: "standard" },
            },
            callLogDoc: {
                to_number: "+971501234567",
                from_number: "+14155550100",
                contact_phone: "+971501234567",
            },
            liveDidRate: 0.055,
            liveDidIso: "US",
            provider: "telnyx",
        });
        assert.strictEqual(r.applied, true);
        assert.strictEqual(r.rateSource, "country_matrix");
        assert.strictEqual(r.destinationRateMode, "country");
        process.env.PRICING_DESTINATION_RATE = "prefix";
        process.env.PRICING_MISSING_POLICY = "m1";
    }

    // 6) Pool never applies
    {
        const rPool = await resolveForeignLiveRateSelection({
            db: makeDb({ cards: AE_CARDS }),
            user: {},
            campaign: { numberPolicySnapshot: { provider: "pool" } },
            callLogDoc: {},
            liveDidRate: 0.05,
            provider: "pool",
        });
        assert.strictEqual(rPool.applied, false);
        assert.strictEqual(rPool.reason, "not_foreign");
    }

    // 7) Local IN 10-digit (no +91) still longest-prefix matches after dial-code normalize
    {
        const cards = [
            {
                provider: "twilio",
                countryIso: "IN",
                destinationPrefix: "916353",
                rateUsdPerMin: 0.0305,
                effectiveFrom: new Date("2020-01-01"),
            },
            {
                provider: "twilio",
                countryIso: "IN",
                destinationPrefix: "91",
                rateUsdPerMin: 0.0351,
                effectiveFrom: new Date("2020-01-01"),
            },
        ];
        const db = makeDb({
            cards,
            commission: {
                enabled: true,
                defaultPercent: 45,
                byProvider: { twilio: 45, telnyx: 40 },
                byCountry: {},
            },
        });
        const r = await resolveForeignLiveRateSelection({
            db,
            user: { creditPlan: { currentTier: "A" } },
            campaign: {
                numberPolicySnapshot: { provider: "twilio" },
                selectedVoice: { tier: "standard" },
                companyCountryIso: "IN",
            },
            callLogDoc: {
                to: "6353125194",
                contact_phone: "6353125194",
            },
            liveDidRate: 0.055,
            liveDidIso: "US",
            provider: "twilio",
        });
        assert.strictEqual(r.applied, true);
        assert.strictEqual(r.rateSource, "prefix");
        assert.strictEqual(r.matchedPrefix, "916353");
        assert.strictEqual(r.providerCostUsdPerMin, 0.0305);
        assert.strictEqual(r.selectedRate, sellFromCost(0.0305, 45));
        assert.strictEqual(r.destIso, "IN");
    }

    // 8) basis=did → no apply even with prefix mode
    {
        process.env.PRICING_BASIS = "did";
        const r = await resolveForeignLiveRateSelection({
            db: makeDb({ cards: AE_CARDS }),
            user: {},
            campaign: { numberPolicySnapshot: { provider: "telnyx" } },
            callLogDoc: { to_number: "+971501234567", contact_phone: "+971501234567" },
            liveDidRate: 0.055,
            provider: "telnyx",
        });
        assert.strictEqual(r.applied, false);
        assert.strictEqual(r.basis, "did");
        process.env.PRICING_BASIS = "destination";
    }

    console.log("test-pricing-prefix-live: OK");
})().catch((err) => {
    console.error(err);
    process.exit(1);
});
