const DIAL_PREFIX_TO_ISO = [
    ["971", "AE"],
    ["966", "SA"],
    ["91", "IN"],
    ["86", "CN"],
    ["81", "JP"],
    ["82", "KR"],
    ["61", "AU"],
    ["55", "BR"],
    ["52", "MX"],
    ["49", "DE"],
    ["44", "GB"],
    ["39", "IT"],
    ["34", "ES"],
    ["33", "FR"],
    ["1", "US"],
];

function normalizeCountryIso(raw) {
    if (raw == null) return null;
    const iso = String(raw).trim().toUpperCase();
    return /^[A-Z]{2}$/.test(iso) ? iso : null;
}

function countryFromDialDigits(digits) {
    if (!digits) return null;
    for (const [prefix, iso] of DIAL_PREFIX_TO_ISO) {
        if (digits.startsWith(prefix)) return iso;
    }
    return null;
}

function countryIsoFromPhone(phone, defaultCountryIso = "IN") {
    const raw = phone != null ? String(phone).trim() : "";
    if (!raw) return normalizeCountryIso(defaultCountryIso) || "IN";

    const digits = raw.replace(/\D/g, "");
    if (digits.startsWith("00")) {
        const intl = countryFromDialDigits(digits.slice(2));
        if (intl) return intl;
    }
    if (raw.startsWith("+") || digits.length > 10) {
        const intl = countryFromDialDigits(digits);
        if (intl) return intl;
    }

    const def = normalizeCountryIso(defaultCountryIso) || "IN";
    if (def === "IN" && digits.length === 10) return "IN";
    return def;
}

function extractDestinationPhone(sources = {}) {
    const keys = [
        "to_number",
        "phone_number",
        "contact_phone",
        "calleeMobile",
        "mobileNumber",
        "dialTo",
        "dial_to",
        "to",
        "destination",
    ];
    for (const key of keys) {
        const v = sources[key];
        if (v != null && String(v).trim()) return String(v).trim();
    }
    return null;
}

module.exports = {
    countryIsoFromPhone,
    extractDestinationPhone,
};
