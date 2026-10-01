/**
 * Wave 1 unit tests: NANP/AE prefixes + callee phone + foreign shadow helpers.
 * Run: node --test scripts/test-pricing-shadow-wave1.js
 */
const { describe, it } = require("node:test");
const assert = require("node:assert/strict");

process.env.NANP_ISLAND_PREFIXES = "1";
process.env.PRICING_SHADOW_MODE = "1";

const {
    countryIsoFromPhone,
    resolveCalleeBillingPhone,
    getDialPrefixTable,
} = require("../lib/countryFromPhone");
const {
    isForeignBillingTargetLocal,
    computeCost,
} = require("../lib/pricingShadow");

describe("NANP / AE prefixes", () => {
    it("maps 971 to AE", () => {
        assert.equal(countryIsoFromPhone("+971501234567"), "AE");
    });
    it("maps 1809/1829/1849 to DO before US", () => {
        assert.equal(countryIsoFromPhone("+18095550123"), "DO");
        assert.equal(countryIsoFromPhone("+18295550123"), "DO");
        assert.equal(countryIsoFromPhone("+18495550123"), "DO");
    });
    it("still maps plain US NANP to US", () => {
        assert.equal(countryIsoFromPhone("+14155552671"), "US");
    });
    it("places island prefixes before 1 in the table", () => {
        const table = getDialPrefixTable();
        const i1809 = table.findIndex((r) => r[0] === "1809");
        const i1 = table.findIndex((r) => r[0] === "1");
        assert.ok(i1809 >= 0 && i1 > i1809);
    });
});

describe("resolveCalleeBillingPhone", () => {
    it("prefers to_number over DID", () => {
        const phone = resolveCalleeBillingPhone(
            { to_number: "+971501234567", selectedPhoneNumber: "+14155550100" },
            { campaign: { selectedPhoneNumber: "+14155550100" } }
        );
        assert.equal(phone, "+971501234567");
    });
    it("reads Twilio To / Telnyx nested to", () => {
        assert.equal(
            resolveCalleeBillingPhone({ To: "+18095550123" }, {}),
            "+18095550123"
        );
        assert.equal(
            resolveCalleeBillingPhone({ payload: { to: "+18295550123" } }, {}),
            "+18295550123"
        );
    });
});

describe("isForeignBillingTargetLocal", () => {
    it("false for pool", () => {
        assert.equal(
            isForeignBillingTargetLocal({
                campaign: { numberPolicySnapshot: { provider: "pool" } },
            }),
            false
        );
    });
    it("true for twilio snapshot", () => {
        assert.equal(
            isForeignBillingTargetLocal({
                campaign: { numberPolicySnapshot: { provider: "twilio" } },
            }),
            true
        );
    });
});

describe("computeCost brackets", () => {
    it("matches webhook floor fractions", () => {
        assert.equal(computeCost(1, 0.08), 0.02);
        assert.equal(computeCost(30, 0.08), 0.04);
        assert.equal(computeCost(60, 0.08), 0.08);
        assert.equal(computeCost(61, 0.08), 0.1);
    });
});
