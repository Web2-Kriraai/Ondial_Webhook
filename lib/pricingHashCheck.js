/**
 * Wave 3 — optional startup hash check (warn only, never crash).
 * Compare live seed SHA256 to docs/PRICING_MODULE_CANONICAL_HASH.txt when
 * PRICING_HASH_CHECK=1.
 */
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const logger = require("../logger");
const { DEFAULT_COUNTRY_PRICING, DEFAULT_FALLBACK_ORDER } = require("./countryPricing");

function envOn(name) {
    const v = String(process.env[name] ?? "").trim().toLowerCase();
    return v === "1" || v === "true" || v === "on" || v === "yes";
}

function computeSeedSha() {
    const payload = JSON.stringify({
        fallbackOrder: DEFAULT_FALLBACK_ORDER || DEFAULT_COUNTRY_PRICING?.fallbackOrder,
        countries: DEFAULT_COUNTRY_PRICING?.countries,
    });
    return crypto.createHash("sha256").update(payload).digest("hex");
}

function maybeWarnPricingHash() {
    if (!envOn("PRICING_HASH_CHECK")) return { skipped: true };
    try {
        const live = computeSeedSha();
        const expectedEnv = String(process.env.PRICING_MODULE_CANONICAL_HASH || "").trim();
        const hashFile = path.join(__dirname, "..", "docs", "PRICING_MODULE_CANONICAL_HASH.txt");
        let expectedFile = "";
        if (fs.existsSync(hashFile)) {
            const text = fs.readFileSync(hashFile, "utf8");
            const m = text.match(/SEED_SHA256=([a-f0-9]+)/i);
            if (m) expectedFile = m[1];
        }
        const expected = expectedEnv || expectedFile;
        if (!expected) {
            logger.warn("[PricingHash] PRICING_HASH_CHECK on but no expected hash configured", {
                liveSeedSha: live,
            });
            return { ok: false, reason: "no_expected" };
        }
        if (live !== expected) {
            logger.warn("[PricingHash] countryPricing seed SHA mismatch (warn only)", {
                liveSeedSha: live,
                expectedSeedSha: expected,
            });
            return { ok: false, live, expected };
        }
        logger.info("[PricingHash] countryPricing seed matches canonical", { liveSeedSha: live });
        return { ok: true, live };
    } catch (err) {
        logger.warn("[PricingHash] check failed (ignored)", { error: err?.message || String(err) });
        return { ok: false, error: String(err?.message || err) };
    }
}

module.exports = { maybeWarnPricingHash, computeSeedSha };
