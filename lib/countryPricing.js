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
 * Known telephony providers for optional per-country overrides
 * (resource costs + optional $/min packages).
 */
const RESOURCE_PROVIDERS = ["twilio", "pool", "telnyx"];

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

/**
 * @returns {"twilio"|"pool"|"telnyx"|null}
 */
function normalizeProviderId(raw) {
    if (raw == null) return null;
    const id = String(raw).trim().toLowerCase();
    return RESOURCE_PROVIDERS.includes(id) ? id : null;
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

function normalizePackagesMap(raw) {
    if (!raw || typeof raw !== "object") return null;
    const packages = {};
    for (const pkgId of PACKAGE_IDS) {
        const pkgRaw = raw[pkgId];
        if (!pkgRaw || typeof pkgRaw !== "object") continue;
        const pkgEntry = {};
        for (const tier of VOICE_TIERS) {
            const cell = normalizeTierCell(pkgRaw[tier]);
            if (cell) pkgEntry[tier] = cell;
        }
        if (Object.keys(pkgEntry).length > 0) packages[pkgId] = pkgEntry;
    }
    return Object.keys(packages).length > 0 ? packages : null;
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

/** Normalize one providers.<id> cell: resources + optional packages + salePriceEnabled. */
function normalizeProviderEntry(raw) {
    if (!raw || typeof raw !== "object") return null;
    const entry = {};

    if (raw.salePriceEnabled === true) entry.salePriceEnabled = true;
    else if (raw.salePriceEnabled === false) entry.salePriceEnabled = false;

    const packages = normalizePackagesMap(raw.packages);
    if (packages) entry.packages = packages;

    const concurrentCallCost = finiteNonNegative(raw.concurrentCallCost);
    if (concurrentCallCost != null) entry.concurrentCallCost = concurrentCallCost;

    const phoneNumberCost = finiteNonNegative(raw.phoneNumberCost);
    if (phoneNumberCost != null) entry.phoneNumberCost = phoneNumberCost;

    return Object.keys(entry).length > 0 ? entry : null;
}

function normalizeProviders(raw) {
    if (!raw || typeof raw !== "object") return null;
    const providers = {};
    for (const key of Object.keys(raw)) {
        const id = normalizeProviderId(key);
        if (!id) continue;
        const normalized = normalizeProviderEntry(raw[key]);
        if (normalized) providers[id] = normalized;
    }
    return Object.keys(providers).length > 0 ? providers : null;
}

function mergeProviderEntry(prev, incoming) {
    if (!incoming) return prev || null;
    if (!prev) return { ...incoming };
    const merged = {
        ...prev,
        ...incoming,
        packages: {
            ...(prev.packages || {}),
            ...(incoming.packages || {}),
        },
    };
    if (incoming.salePriceEnabled !== undefined) {
        merged.salePriceEnabled = incoming.salePriceEnabled;
    } else if (prev.salePriceEnabled !== undefined) {
        merged.salePriceEnabled = prev.salePriceEnabled;
    }
    if (merged.packages && Object.keys(merged.packages).length === 0) delete merged.packages;
    return merged;
}

function mergeProviders(prevProviders, incomingProviders) {
    const merged = { ...(prevProviders || {}) };
    if (incomingProviders && typeof incomingProviders === "object") {
        for (const [pid, entry] of Object.entries(incomingProviders)) {
            const next = mergeProviderEntry(merged[pid], entry);
            if (next) merged[pid] = next;
        }
    }
    return Object.keys(merged).length > 0 ? merged : null;
}

function normalizeCountryEntry(raw, countryIso = null) {
    if (!raw || typeof raw !== "object") return null;
    const entry = {};

    if (raw.salePriceEnabled === true) entry.salePriceEnabled = true;
    else if (raw.salePriceEnabled === false) entry.salePriceEnabled = false;

    const packages = normalizePackagesMap(raw.packages);
    if (packages) entry.packages = packages;

    const concurrentCallCost = finiteNonNegative(raw.concurrentCallCost);
    if (concurrentCallCost != null) entry.concurrentCallCost = concurrentCallCost;

    const phoneNumberCost = finiteNonNegative(raw.phoneNumberCost);
    if (phoneNumberCost != null) entry.phoneNumberCost = phoneNumberCost;

    const providers = normalizeProviders(raw.providers);
    if (providers) entry.providers = providers;

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
            const { providers: incomingProviders, ...restEntry } = normalizedEntry;
            countries[iso] = {
                ...prev,
                ...restEntry,
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
            const mergedProviders = mergeProviders(prev.providers, incomingProviders);
            if (mergedProviders) countries[iso].providers = mergedProviders;
            else delete countries[iso].providers;
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

function lookupPackageRate(countries, countryIso, packageId, voiceTier, provider) {
    const iso = normalizeCountryIso(countryIso);
    if (!iso || !countries[iso]) return null;
    const countryEntry = countries[iso];
    const providerId = normalizeProviderId(provider);
    const pkgId = String(packageId || "").toLowerCase();
    const tierKey = String(voiceTier || "").toLowerCase();

    if (providerId && countryEntry.providers?.[providerId]?.packages?.[pkgId]) {
        const cell = normalizeTierCell(countryEntry.providers[providerId].packages[pkgId][tierKey]);
        if (cell) {
            const salePriceEnabled =
                countryEntry.providers[providerId].salePriceEnabled !== undefined
                    ? countryEntry.providers[providerId].salePriceEnabled === true
                    : countryEntry.salePriceEnabled === true;
            const rate = effectiveRateFromCell(cell, salePriceEnabled);
            if (rate != null) {
                return {
                    rate,
                    list: cell.list,
                    sale: cell.sale,
                    salePriceEnabled,
                    countryIso: iso,
                    provider: providerId,
                    sourceHint: "provider",
                };
            }
        }
    }

    const pkg = countryEntry.packages?.[pkgId];
    if (!pkg) return null;
    const cell = normalizeTierCell(pkg[tierKey]);
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
        provider: null,
        sourceHint: "country",
    };
}

/**
 * Resolve resource costs for a country, optionally overlaying a provider cell.
 * Provider fields win per-field; missing provider fields inherit country values.
 */
function lookupResourceCosts(countries, countryIso, provider) {
    const iso = normalizeCountryIso(countryIso);
    if (!iso || !countries[iso]) return null;
    const countryEntry = countries[iso];
    let { concurrentCallCost, phoneNumberCost } = countryEntry;
    const providerId = normalizeProviderId(provider);
    let usedProvider = false;
    if (providerId && countryEntry.providers?.[providerId]) {
        const overlay = countryEntry.providers[providerId];
        if (overlay.phoneNumberCost != null) {
            phoneNumberCost = overlay.phoneNumberCost;
            usedProvider = true;
        }
        if (overlay.concurrentCallCost != null) {
            concurrentCallCost = overlay.concurrentCallCost;
            usedProvider = true;
        }
    }
    if (concurrentCallCost == null && phoneNumberCost == null) return null;
    return {
        concurrentCallCost,
        phoneNumberCost,
        countryIso: iso,
        provider: usedProvider ? providerId : null,
        sourceHint: usedProvider ? "provider" : "country",
    };
}

function packageResult(hit, source) {
    return {
        ratePerMinute: hit.rate,
        listRatePerMinute: hit.list ?? hit.rate,
        saleRatePerMinute: hit.sale ?? hit.rate,
        salePriceEnabled: hit.salePriceEnabled === true,
        countryIso: hit.countryIso,
        source: hit.sourceHint === "provider" ? "provider" : source,
        provider: hit.provider || null,
    };
}

function resolvePlanRate({
    config,
    countryIso,
    provider,
    packageId,
    voiceTier,
    fallbackRatePerMinute,
}) {
    const packageFallback = () => ({
        ratePerMinute: finiteNonNegative(fallbackRatePerMinute) ?? 0,
        listRatePerMinute: finiteNonNegative(fallbackRatePerMinute) ?? 0,
        saleRatePerMinute: finiteNonNegative(fallbackRatePerMinute) ?? 0,
        salePriceEnabled: false,
        countryIso: normalizeCountryIso(countryIso),
        source: "package",
        provider: null,
    });

    try {
        const normalized = normalizeCountryPricingConfig(config);
        const pkgId = String(packageId || "starter").toLowerCase();
        const tier = String(voiceTier || "standard").toLowerCase();
        const providerId = normalizeProviderId(provider);

        if (normalized.enabled === false) {
            return packageFallback();
        }

        const direct = lookupPackageRate(normalized.countries, countryIso, pkgId, tier, providerId);
        if (direct) {
            return packageResult(direct, direct.sourceHint === "provider" ? "provider" : "country");
        }

        for (const fbIso of normalized.fallbackOrder) {
            const hit = lookupPackageRate(normalized.countries, fbIso, pkgId, tier, providerId);
            if (hit) {
                return packageResult(
                    hit,
                    hit.sourceHint === "provider" ? "provider" : "fallback"
                );
            }
        }

        return packageFallback();
    } catch {
        return packageFallback();
    }
}

function resolveInrDisplayRate({ config, countryIso, packageId, voiceTier }) {
    const iso = normalizeCountryIso(countryIso);
    if (iso !== "IN") return null;
    const normalized = normalizeCountryPricingConfig(config);
    const pkg = normalized.countries?.IN?.packagesInr?.[String(packageId || "").toLowerCase()];
    if (!pkg) return null;
    return finiteNonNegative(pkg[String(voiceTier || "").toLowerCase()]);
}

function resolveResourceCosts({ config, countryIso, provider, fallbackPhoneNumberCost, fallbackConcurrentCallCost }) {
    const packageFallback = () => ({
        phoneNumberCost: finiteNonNegative(fallbackPhoneNumberCost) ?? 0,
        concurrentCallCost: finiteNonNegative(fallbackConcurrentCallCost) ?? 0,
        countryIso: normalizeCountryIso(countryIso),
        source: "package",
        provider: null,
    });

    try {
        const normalized = normalizeCountryPricingConfig(config);
        const providerId = normalizeProviderId(provider);

        if (normalized.enabled === false) {
            return packageFallback();
        }

        const direct = lookupResourceCosts(normalized.countries, countryIso, providerId);
        if (direct) {
            return {
                phoneNumberCost: direct.phoneNumberCost ?? finiteNonNegative(fallbackPhoneNumberCost) ?? 0,
                concurrentCallCost: direct.concurrentCallCost ?? finiteNonNegative(fallbackConcurrentCallCost) ?? 0,
                countryIso: direct.countryIso,
                source: direct.sourceHint || "country",
                provider: direct.provider,
            };
        }

        for (const fb of normalized.fallbackOrder) {
            const hit = lookupResourceCosts(normalized.countries, fb, providerId);
            if (hit) {
                return {
                    phoneNumberCost: hit.phoneNumberCost ?? finiteNonNegative(fallbackPhoneNumberCost) ?? 0,
                    concurrentCallCost: hit.concurrentCallCost ?? finiteNonNegative(fallbackConcurrentCallCost) ?? 0,
                    countryIso: hit.countryIso,
                    source: hit.sourceHint === "provider" ? "provider" : "fallback",
                    provider: hit.provider,
                };
            }
        }

        return packageFallback();
    } catch {
        return packageFallback();
    }
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
    RESOURCE_PROVIDERS,
    DEFAULT_COUNTRY_PRICING,
    normalizeCountryIso,
    normalizeProviderId,
    normalizeTierCell,
    normalizeCountryPricingConfig,
    resolvePlanRate,
    resolveInrDisplayRate,
    resolveResourceCosts,
    loadCountryPricingConfig,
    invalidateCountryPricingCache,
    TIER_TO_PACKAGE_ID,
};
