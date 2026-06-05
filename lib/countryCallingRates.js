const COUNTRY_CALLING_RATES_SETTING_KEY = "countryCallingRatesV1";

const DEFAULT_COUNTRY_CALLING_RATES = {
    fallbackOrder: ["US", "IN"],
    countries: {
        IN: { standard: 0.055, premium: 0.045, elite: 0.06 },
        US: { standard: 0.075, premium: 0.065, elite: 0.085 },
    },
};

const VOICE_TIERS = ["standard", "premium", "elite"];

function finiteRate(value) {
    const n = Number(value);
    return Number.isFinite(n) && n >= 0 ? n : null;
}

function normalizeCountryIso(raw) {
    if (raw == null) return null;
    const iso = String(raw).trim().toUpperCase();
    if (!/^[A-Z]{2}$/.test(iso) || iso === "XX") return null;
    return iso;
}

function normalizeCountryCallingRates(raw) {
    const base = {
        fallbackOrder: [...DEFAULT_COUNTRY_CALLING_RATES.fallbackOrder],
        countries: { ...DEFAULT_COUNTRY_CALLING_RATES.countries },
    };
    if (!raw || typeof raw !== "object") return base;

    const fallbackOrder = Array.isArray(raw.fallbackOrder)
        ? raw.fallbackOrder.map(normalizeCountryIso).filter(Boolean)
        : base.fallbackOrder;

    const countries = { ...base.countries };
    if (raw.countries && typeof raw.countries === "object") {
        for (const [key, value] of Object.entries(raw.countries)) {
            const iso = normalizeCountryIso(key);
            if (!iso || !value || typeof value !== "object") continue;
            const entry = {};
            for (const tier of VOICE_TIERS) {
                const rate = finiteRate(value[tier]);
                if (rate != null) entry[tier] = rate;
            }
            if (Object.keys(entry).length > 0) countries[iso] = entry;
        }
    }

    return {
        fallbackOrder: fallbackOrder.length > 0 ? fallbackOrder : base.fallbackOrder,
        countries,
    };
}

function lookupCountryRate(countries, countryIso, voiceTier, fallbackOrder = ["US", "IN"]) {
    let tier = String(voiceTier || "standard").toLowerCase();
    if (tier === "basic") tier = "standard";

    const tryIso = (iso) => {
        const key = normalizeCountryIso(iso);
        if (!key || !countries[key]) return null;
        const rate = finiteRate(countries[key][tier]);
        return rate != null ? { rate, countryIso: key } : null;
    };

    const direct = tryIso(countryIso);
    if (direct) return { ...direct, source: "country" };

    for (const fb of fallbackOrder) {
        const hit = tryIso(fb);
        if (hit) return { ...hit, source: "fallback" };
    }

    return null;
}

function applyCountryRateToPackageRate(packageRatePerMinute, config, destinationCountryIso, voiceTier = "standard") {
    const normalized = normalizeCountryCallingRates(config);
    const lookup = lookupCountryRate(
        normalized.countries,
        destinationCountryIso,
        voiceTier,
        normalized.fallbackOrder
    );

    if (lookup) {
        return {
            ratePerMinute: lookup.rate,
            destinationCountryIso: lookup.countryIso,
            countryRateSource: lookup.source,
            countryRateApplied: true,
        };
    }

    return {
        ratePerMinute: packageRatePerMinute,
        destinationCountryIso: normalizeCountryIso(destinationCountryIso),
        countryRateSource: "package",
        countryRateApplied: false,
    };
}

async function loadCountryCallingRates(db) {
    if (!db?.collection) return DEFAULT_COUNTRY_CALLING_RATES;
    try {
        const doc = await db.collection("systemsettings").findOne({ key: COUNTRY_CALLING_RATES_SETTING_KEY });
        return normalizeCountryCallingRates(doc?.value);
    } catch {
        return DEFAULT_COUNTRY_CALLING_RATES;
    }
}

module.exports = {
    COUNTRY_CALLING_RATES_SETTING_KEY,
    DEFAULT_COUNTRY_CALLING_RATES,
    normalizeCountryCallingRates,
    applyCountryRateToPackageRate,
    loadCountryCallingRates,
};
