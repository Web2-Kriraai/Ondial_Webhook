/**
 * Country-wise pricing resolver (Layer 2 of the 3-layer pricing model):
 *
 *   1. User.billingOverride   — per-user custom rates (highest priority, resolved by caller)
 *   2. countryPricingV1       — country x plan-package x voice-tier rate matrix (this module)
 *   3. CreditPackage defaults — global fallback when no country cell matches (resolved by caller)
 *
 * Each voice-tier cell may be a legacy number or `{ list, sale }`.
 * Per-country `salePriceEnabled` chooses sale vs list for billing.
 * IN may also store `packagesInr` (display-only ₹/min for Credits catalog).
 *
 * Mirrors lib/pricing/countryPricing.js in Ondial / Ondial-Super-Admin and
 * shared-lib/src/countryPricing.js in Calling_system1. Supersedes the older
 * voice-only countryCallingRates.js (kept only for one-time migration).
 */

const COUNTRY_PRICING_SETTING_KEY = "countryPricingV1";
const VOICE_TIERS = ["standard", "premium", "elite"];
const PACKAGE_IDS = ["starter", "professional", "enterprise", "premium"];

/**
 * Seed defaults for Twilio-facing markets + India.
 * Essential=starter, Growth=professional, Scale=enterprise, Enterprise=premium.
 * Plain numbers normalize to `{ list, sale }` on load.
 */
function usLikePackages() {
    return {
        starter: { standard: 0.065, premium: 0.075, elite: 0.085 },
        professional: { standard: 0.06, premium: 0.07, elite: 0.08 },
        enterprise: { standard: 0.055, premium: 0.065, elite: 0.075 },
        premium: { standard: 0.05, premium: 0.06, elite: 0.07 },
    };
}

function usLikeCountryEntry({ concurrentCallCost = 7, phoneNumberCost = 7 } = {}) {
    return {
        salePriceEnabled: false,
        packages: usLikePackages(),
        concurrentCallCost,
        phoneNumberCost,
    };
}

const DEFAULT_COUNTRY_PRICING = {
    enabled: true,
    fallbackOrder: ['US', 'IN'],
    countries: {
        IN: {
            // Mirrors Credit Packages model pricing (List / Sale / ₹) for India.
            salePriceEnabled: true,
            packages: {
                starter: {
                    standard: { list: 0.055, sale: 0.055 },
                    premium: { list: 0.075, sale: 0.055 },
                    elite: { list: 0.11, sale: 0.11 },
                },
                professional: {
                    standard: { list: 0.05, sale: 0.05 },
                    premium: { list: 0.07, sale: 0.05 },
                    elite: { list: 0.1, sale: 0.1 },
                },
                enterprise: {
                    standard: { list: 0.045, sale: 0.045 },
                    premium: { list: 0.065, sale: 0.045 },
                    elite: { list: 0.095, sale: 0.095 },
                },
                premium: {
                    standard: { list: 0.04, sale: 0.04 },
                    premium: { list: 0.06, sale: 0.04 },
                    elite: { list: 0.09, sale: 0.09 },
                },
            },
            packagesInr: {
                starter: { standard: 5, premium: 5, elite: 10 },
                professional: { standard: 4.5, premium: 4.5, elite: 9.5 },
                enterprise: { standard: 4, premium: 4, elite: 9 },
                premium: { standard: 3.5, premium: 3.5, elite: 8.5 },
            },
            concurrentCallCost: 4.9,
            phoneNumberCost: 4.9,
        },
        US: usLikeCountryEntry({ concurrentCallCost: 7, phoneNumberCost: 7 }),
        // Twilio intl picker markets (starting points — edit in Super-Admin as needed)
        GB: usLikeCountryEntry({ concurrentCallCost: 7, phoneNumberCost: 7 }),
        CA: usLikeCountryEntry({ concurrentCallCost: 7, phoneNumberCost: 7 }),
        AU: {
            salePriceEnabled: false,
            packages: {
                starter: { standard: 0.045, premium: 0.055, elite: 0.065 },
                professional: { standard: 0.04, premium: 0.05, elite: 0.06 },
                enterprise: { standard: 0.035, premium: 0.045, elite: 0.055 },
                premium: { standard: 0.03, premium: 0.04, elite: 0.05 },
            },
            concurrentCallCost: 6,
            phoneNumberCost: 6,
        },
        DE: usLikeCountryEntry({ concurrentCallCost: 7, phoneNumberCost: 7 }),
        FR: usLikeCountryEntry({ concurrentCallCost: 7, phoneNumberCost: 7 }),
        MY: usLikeCountryEntry({ concurrentCallCost: 6, phoneNumberCost: 6 }),
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

function normalizeTierCell(raw) {
    if (raw == null) return null;
    if (typeof raw === "number" || typeof raw === "string") {
        const n = finiteNonNegative(raw);
        if (n == null) return null;
        return { list: n, sale: n };
    }
    if (typeof raw === "object") {
        const list = finiteNonNegative(raw.list ?? raw.checked);
        const sale = finiteNonNegative(raw.sale ?? raw.discounted ?? raw.price);
        if (list == null && sale == null) return null;
        return { list: list ?? sale, sale: sale ?? list };
    }
    return null;
}

function normalizePackagesInr(raw) {
    if (!raw || typeof raw !== "object") return null;
    const packagesInr = {};
    for (const pkgId of PACKAGE_IDS) {
        const pkgRaw = raw[pkgId];
        if (!pkgRaw || typeof pkgRaw !== "object") continue;
        const pkgEntry = {};
        for (const tier of VOICE_TIERS) {
            const rate = finiteNonNegative(pkgRaw[tier]);
            if (rate != null) pkgEntry[tier] = rate;
        }
        if (Object.keys(pkgEntry).length > 0) packagesInr[pkgId] = pkgEntry;
    }
    return Object.keys(packagesInr).length > 0 ? packagesInr : null;
}

function normalizeCountryEntry(raw, countryIso = null) {
    if (!raw || typeof raw !== "object") return null;
    const entry = {};

    if (raw.salePriceEnabled === true) entry.salePriceEnabled = true;
    else if (raw.salePriceEnabled === false) entry.salePriceEnabled = false;

    const packages = {};
    if (raw.packages && typeof raw.packages === "object") {
        for (const pkgId of PACKAGE_IDS) {
            const pkgRaw = raw.packages[pkgId];
            if (!pkgRaw || typeof pkgRaw !== "object") continue;
            const pkgEntry = {};
            for (const tier of VOICE_TIERS) {
                const cell = normalizeTierCell(pkgRaw[tier]);
                if (cell) pkgEntry[tier] = cell;
            }
            if (Object.keys(pkgEntry).length > 0) packages[pkgId] = pkgEntry;
        }
    }
    if (Object.keys(packages).length > 0) entry.packages = packages;

    const concurrentCallCost = finiteNonNegative(raw.concurrentCallCost);
    if (concurrentCallCost != null) entry.concurrentCallCost = concurrentCallCost;

    const phoneNumberCost = finiteNonNegative(raw.phoneNumberCost);
    if (phoneNumberCost != null) entry.phoneNumberCost = phoneNumberCost;

    const iso = normalizeCountryIso(countryIso);
    if (iso === "IN") {
        const packagesInr = normalizePackagesInr(raw.packagesInr);
        if (packagesInr) entry.packagesInr = packagesInr;
    }

    return Object.keys(entry).length > 0 ? entry : null;
}

function normalizeCountryPricingConfig(raw) {
    const baseCountries = {};
    for (const [iso, entry] of Object.entries(DEFAULT_COUNTRY_PRICING.countries)) {
        baseCountries[iso] = normalizeCountryEntry(JSON.parse(JSON.stringify(entry)), iso);
    }

    const base = {
        enabled: true,
        fallbackOrder: [...DEFAULT_COUNTRY_PRICING.fallbackOrder],
        countries: baseCountries,
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
            const normalizedEntry = normalizeCountryEntry(value, iso);
            if (!normalizedEntry) continue;
            const prev = countries[iso] || {};
            countries[iso] = {
                ...prev,
                ...normalizedEntry,
                salePriceEnabled:
                    normalizedEntry.salePriceEnabled !== undefined
                        ? normalizedEntry.salePriceEnabled
                        : prev.salePriceEnabled === true,
                packages: {
                    ...(prev.packages || {}),
                    ...(normalizedEntry.packages || {}),
                },
                ...(iso === "IN"
                    ? {
                          packagesInr: {
                              ...(prev.packagesInr || {}),
                              ...(normalizedEntry.packagesInr || {}),
                          },
                      }
                    : {}),
            };
            if (iso === "IN" && countries[iso].packagesInr && Object.keys(countries[iso].packagesInr).length === 0) {
                delete countries[iso].packagesInr;
            }
            if (iso !== "IN") delete countries[iso].packagesInr;
        }
    }

    return {
        enabled,
        fallbackOrder: fallbackOrder.length > 0 ? fallbackOrder : base.fallbackOrder,
        countries,
    };
}

function effectiveRateFromCell(cell, salePriceEnabled) {
    if (!cell) return null;
    if (salePriceEnabled === true && cell.sale != null) return cell.sale;
    return cell.list ?? cell.sale ?? null;
}

function lookupPackageRate(countries, countryIso, packageId, voiceTier) {
    const iso = normalizeCountryIso(countryIso);
    if (!iso || !countries[iso]) return null;
    const countryEntry = countries[iso];
    const pkg = countryEntry.packages?.[String(packageId || "").toLowerCase()];
    if (!pkg) return null;
    const cell = normalizeTierCell(pkg[String(voiceTier || "").toLowerCase()]);
    if (!cell) return null;
    const salePriceEnabled = countryEntry.salePriceEnabled === true;
    const rate = effectiveRateFromCell(cell, salePriceEnabled);
    if (rate == null) return null;
    return {
        rate,
        list: cell.list,
        sale: cell.sale,
        salePriceEnabled,
        countryIso: iso,
    };
}

function lookupResourceCosts(countries, countryIso) {
    const iso = normalizeCountryIso(countryIso);
    if (!iso || !countries[iso]) return null;
    const { concurrentCallCost, phoneNumberCost } = countries[iso];
    if (concurrentCallCost == null && phoneNumberCost == null) return null;
    return { concurrentCallCost, phoneNumberCost, countryIso: iso };
}

function packageResult(hit, source) {
    return {
        ratePerMinute: hit.rate,
        listRatePerMinute: hit.list ?? hit.rate,
        saleRatePerMinute: hit.sale ?? hit.rate,
        salePriceEnabled: hit.salePriceEnabled === true,
        countryIso: hit.countryIso,
        source,
    };
}

function resolvePlanRate({ config, countryIso, packageId, voiceTier, fallbackRatePerMinute }) {
    const normalized = normalizeCountryPricingConfig(config);
    const pkgId = String(packageId || "starter").toLowerCase();
    const tier = String(voiceTier || "standard").toLowerCase();
    const fb = finiteNonNegative(fallbackRatePerMinute) ?? 0;

    if (normalized.enabled === false) {
        return {
            ratePerMinute: fb,
            listRatePerMinute: fb,
            saleRatePerMinute: fb,
            salePriceEnabled: false,
            countryIso: normalizeCountryIso(countryIso),
            source: "package",
        };
    }

    const direct = lookupPackageRate(normalized.countries, countryIso, pkgId, tier);
    if (direct) return packageResult(direct, "country");

    for (const fbIso of normalized.fallbackOrder) {
        const hit = lookupPackageRate(normalized.countries, fbIso, pkgId, tier);
        if (hit) return packageResult(hit, "fallback");
    }

    return {
        ratePerMinute: fb,
        listRatePerMinute: fb,
        saleRatePerMinute: fb,
        salePriceEnabled: false,
        countryIso: normalizeCountryIso(countryIso),
        source: "package",
    };
}

function resolveInrDisplayRate({ config, countryIso, packageId, voiceTier }) {
    const iso = normalizeCountryIso(countryIso);
    if (iso !== "IN") return null;
    const normalized = normalizeCountryPricingConfig(config);
    const pkg = normalized.countries?.IN?.packagesInr?.[String(packageId || "").toLowerCase()];
    if (!pkg) return null;
    return finiteNonNegative(pkg[String(voiceTier || "").toLowerCase()]);
}

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
    normalizeTierCell,
    normalizeCountryPricingConfig,
    resolvePlanRate,
    resolveInrDisplayRate,
    resolveResourceCosts,
    loadCountryPricingConfig,
    invalidateCountryPricingCache,
    TIER_TO_PACKAGE_ID,
};
