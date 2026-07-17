/**
 * Country-wise pricing resolver (Layer 2 of the 3-layer pricing model):
 *
 *   1. User.billingOverride   — per-user custom rates (highest priority, resolved by caller)
 *   2. countryPricingV1       — country x plan-package x voice-tier rate matrix (this module)
 *   3. CreditPackage defaults — global fallback when no country cell matches (resolved by caller)
 *
 * Mirrors lib/pricing/countryPricing.js in Ondial / Ondial-Super-Admin and
 * shared-lib/src/countryPricing.js in Calling_system1. Supersedes the older
 * voice-only countryCallingRates.js (kept only for one-time migration).
 */

const COUNTRY_PRICING_SETTING_KEY = "countryPricingV1";
const VOICE_TIERS = ["standard", "premium", "elite"];
const PACKAGE_IDS = ["starter", "professional", "enterprise", "premium"];

const DEFAULT_COUNTRY_PRICING = {
    // Staged-rollout kill switch: set to `false` (via Super-Admin or directly on the
    // countryPricingV1 SystemSetting doc) to bypass Layer 2 entirely and fall straight
    // through to CreditPackage global defaults (Layer 3) for every country.
    enabled: true,
    fallbackOrder: ["US", "IN"],
    countries: {
        IN: {
            packages: {
                starter: { standard: 0.055, premium: 0.065, elite: 0.075 },
                professional: { standard: 0.05, premium: 0.06, elite: 0.07 },
                enterprise: { standard: 0.045, premium: 0.055, elite: 0.065 },
                premium: { standard: 0.04, premium: 0.05, elite: 0.06 },
            },
            concurrentCallCost: 4.9,
            phoneNumberCost: 4.9,
        },
        US: {
            packages: {
                starter: { standard: 0.065, premium: 0.075, elite: 0.085 },
                professional: { standard: 0.06, premium: 0.07, elite: 0.08 },
                enterprise: { standard: 0.055, premium: 0.065, elite: 0.075 },
                premium: { standard: 0.05, premium: 0.06, elite: 0.07 },
            },
            concurrentCallCost: 7,
            phoneNumberCost: 7,
        },
        AU: {
            packages: {
                starter: { standard: 0.045, premium: 0.055, elite: 0.065 },
                professional: { standard: 0.04, premium: 0.05, elite: 0.06 },
                enterprise: { standard: 0.035, premium: 0.045, elite: 0.055 },
                premium: { standard: 0.03, premium: 0.04, elite: 0.05 },
            },
            concurrentCallCost: 6,
            phoneNumberCost: 6,
        },
    },
};

function finiteNonNegative(value) {
    const n = Number(value);
    return Number.isFinite(n) && n >= 0 ? n : null;
}

function normalizeCountryIso(raw) {
    if (raw == null) return null;
    const iso = String(raw).trim().toUpperCase();
    if (!/^[A-Z]{2}$/.test(iso) || iso === "XX") return null;
    return iso;
}

function normalizeCountryEntry(raw) {
    if (!raw || typeof raw !== "object") return null;
    const entry = {};

    const packages = {};
    if (raw.packages && typeof raw.packages === "object") {
        for (const pkgId of PACKAGE_IDS) {
            const pkgRaw = raw.packages[pkgId];
            if (!pkgRaw || typeof pkgRaw !== "object") continue;
            const pkgEntry = {};
            for (const tier of VOICE_TIERS) {
                const rate = finiteNonNegative(pkgRaw[tier]);
                if (rate != null) pkgEntry[tier] = rate;
            }
            if (Object.keys(pkgEntry).length > 0) packages[pkgId] = pkgEntry;
        }
    }
    if (Object.keys(packages).length > 0) entry.packages = packages;

    const concurrentCallCost = finiteNonNegative(raw.concurrentCallCost);
    if (concurrentCallCost != null) entry.concurrentCallCost = concurrentCallCost;

    const phoneNumberCost = finiteNonNegative(raw.phoneNumberCost);
    if (phoneNumberCost != null) entry.phoneNumberCost = phoneNumberCost;

    return Object.keys(entry).length > 0 ? entry : null;
}

function normalizeCountryPricingConfig(raw) {
    const base = {
        enabled: true,
        fallbackOrder: [...DEFAULT_COUNTRY_PRICING.fallbackOrder],
        countries: JSON.parse(JSON.stringify(DEFAULT_COUNTRY_PRICING.countries)),
    };
    if (!raw || typeof raw !== "object") return base;

    const enabled = raw.enabled !== false;

    const fallbackOrder = Array.isArray(raw.fallbackOrder)
        ? raw.fallbackOrder.map(normalizeCountryIso).filter(Boolean)
        : base.fallbackOrder;

    const countries = { ...base.countries };
    if (raw.countries && typeof raw.countries === "object") {
        for (const [key, value] of Object.entries(raw.countries)) {
            const iso = normalizeCountryIso(key);
            if (!iso) continue;
            const normalizedEntry = normalizeCountryEntry(value);
            if (!normalizedEntry) continue;
            countries[iso] = {
                ...(countries[iso] || {}),
                ...normalizedEntry,
                packages: {
                    ...(countries[iso]?.packages || {}),
                    ...(normalizedEntry.packages || {}),
                },
            };
        }
    }

    return {
        enabled,
        fallbackOrder: fallbackOrder.length > 0 ? fallbackOrder : base.fallbackOrder,
        countries,
    };
}

function lookupPackageRate(countries, countryIso, packageId, voiceTier) {
    const iso = normalizeCountryIso(countryIso);
    if (!iso || !countries[iso]) return null;
    const pkg = countries[iso].packages?.[String(packageId || "").toLowerCase()];
    if (!pkg) return null;
    const rate = finiteNonNegative(pkg[String(voiceTier || "").toLowerCase()]);
    return rate != null ? { rate, countryIso: iso } : null;
}

function lookupResourceCosts(countries, countryIso) {
    const iso = normalizeCountryIso(countryIso);
    if (!iso || !countries[iso]) return null;
    const { concurrentCallCost, phoneNumberCost } = countries[iso];
    if (concurrentCallCost == null && phoneNumberCost == null) return null;
    return { concurrentCallCost, phoneNumberCost, countryIso: iso };
}

/**
 * @param {{ config?: object, countryIso?: string, packageId: string, voiceTier: string, fallbackRatePerMinute: number }} args
 */
function resolvePlanRate({ config, countryIso, packageId, voiceTier, fallbackRatePerMinute }) {
    const normalized = normalizeCountryPricingConfig(config);
    const pkgId = String(packageId || "starter").toLowerCase();
    const tier = String(voiceTier || "standard").toLowerCase();

    if (normalized.enabled === false) {
        return {
            ratePerMinute: finiteNonNegative(fallbackRatePerMinute) ?? 0,
            countryIso: normalizeCountryIso(countryIso),
            source: "package",
        };
    }

    const direct = lookupPackageRate(normalized.countries, countryIso, pkgId, tier);
    if (direct) {
        return { ratePerMinute: direct.rate, countryIso: direct.countryIso, source: "country" };
    }

    for (const fb of normalized.fallbackOrder) {
        const hit = lookupPackageRate(normalized.countries, fb, pkgId, tier);
        if (hit) {
            return { ratePerMinute: hit.rate, countryIso: hit.countryIso, source: "fallback" };
        }
    }

    return {
        ratePerMinute: finiteNonNegative(fallbackRatePerMinute) ?? 0,
        countryIso: normalizeCountryIso(countryIso),
        source: "package",
    };
}

/**
 * @param {{ config?: object, countryIso?: string, fallbackPhoneNumberCost: number, fallbackConcurrentCallCost: number }} args
 */
function resolveResourceCosts({ config, countryIso, fallbackPhoneNumberCost, fallbackConcurrentCallCost }) {
    const normalized = normalizeCountryPricingConfig(config);

    if (normalized.enabled === false) {
        return {
            phoneNumberCost: finiteNonNegative(fallbackPhoneNumberCost) ?? 0,
            concurrentCallCost: finiteNonNegative(fallbackConcurrentCallCost) ?? 0,
            countryIso: normalizeCountryIso(countryIso),
            source: "package",
        };
    }

    const direct = lookupResourceCosts(normalized.countries, countryIso);
    if (direct) {
        return {
            phoneNumberCost: direct.phoneNumberCost ?? finiteNonNegative(fallbackPhoneNumberCost) ?? 0,
            concurrentCallCost: direct.concurrentCallCost ?? finiteNonNegative(fallbackConcurrentCallCost) ?? 0,
            countryIso: direct.countryIso,
            source: "country",
        };
    }

    for (const fb of normalized.fallbackOrder) {
        const hit = lookupResourceCosts(normalized.countries, fb);
        if (hit) {
            return {
                phoneNumberCost: hit.phoneNumberCost ?? finiteNonNegative(fallbackPhoneNumberCost) ?? 0,
                concurrentCallCost: hit.concurrentCallCost ?? finiteNonNegative(fallbackConcurrentCallCost) ?? 0,
                countryIso: hit.countryIso,
                source: "fallback",
            };
        }
    }

    return {
        phoneNumberCost: finiteNonNegative(fallbackPhoneNumberCost) ?? 0,
        concurrentCallCost: finiteNonNegative(fallbackConcurrentCallCost) ?? 0,
        countryIso: normalizeCountryIso(countryIso),
        source: "package",
    };
}

let cachedConfig = null;
let cachedAt = 0;
const CACHE_TTL_MS = 60_000;

/**
 * Load + normalize `countryPricingV1` from SystemSetting, with a short in-process cache.
 * @param {import('mongodb').Db} db
 */
async function loadCountryPricingConfig(db, { forceRefresh = false } = {}) {
    const now = Date.now();
    if (!forceRefresh && cachedConfig && now - cachedAt < CACHE_TTL_MS) {
        return cachedConfig;
    }
    try {
        const setting = await db.collection("systemsettings").findOne({ key: COUNTRY_PRICING_SETTING_KEY });
        cachedConfig = normalizeCountryPricingConfig(setting?.value);
    } catch {
        cachedConfig = cachedConfig || normalizeCountryPricingConfig(null);
    }
    cachedAt = now;
    return cachedConfig;
}

function invalidateCountryPricingCache() {
    cachedConfig = null;
    cachedAt = 0;
}

const TIER_TO_PACKAGE_ID = { A: "starter", B: "professional", C: "enterprise", D: "premium" };

module.exports = {
    COUNTRY_PRICING_SETTING_KEY,
    VOICE_TIERS,
    PACKAGE_IDS,
    DEFAULT_COUNTRY_PRICING,
    normalizeCountryIso,
    normalizeCountryPricingConfig,
    resolvePlanRate,
    resolveResourceCosts,
    loadCountryPricingConfig,
    invalidateCountryPricingCache,
    TIER_TO_PACKAGE_ID,
};
