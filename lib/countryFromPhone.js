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

/** Inbound caller phone (who is calling the DID). */
function extractCallerPhone(sources = {}) {
    const keys = [
        "from_number",
        "caller_number",
        "from",
        "phone_from",
        "caller",
        "fromPhone",
        "caller_phone",
        "callerNumber",
        "phone_number_from",
    ];
    for (const key of keys) {
        const v = sources[key];
        if (v != null && String(v).trim()) return String(v).trim();
    }
    return null;
}

/** Caller phone from InboundConversation call_data.events when top-level fields are missing. */
function phoneFromInboundEvents(doc) {
    const events = doc?.call_data?.events;
    if (!Array.isArray(events)) return null;
    for (let i = events.length - 1; i >= 0; i--) {
        const d = events[i]?.data;
        if (!d || typeof d !== "object") continue;
        const from = d.from || d.From_Number || d._raw?.call?.from;
        if (from != null && String(from).trim()) return String(from).trim();
    }
    return null;
}

/**
 * Phone/number used for country-based rate lookup — number-based billing:
 * the OWNED number placing/receiving the call drives the rate, not the other party.
 * Outbound: the campaign's own selected/purchased line (falls back to the dialed
 * destination if unavailable). Inbound: the DID that was called (to_number).
 */
function resolveCountryBillingPhone(callLogDoc, { inbound = false, campaign = null } = {}) {
    const doc = callLogDoc || {};
    if (inbound) {
        return extractDestinationPhone(doc) || null;
    }
    return campaign?.selectedPhoneNumber || extractDestinationPhone(doc) || null;
}

/**
 * Default ISO when billing phone is missing or ambiguous.
 * Inbound: infer from DID (to_number); outbound: campaign default.
 */
function resolveDefaultCountryIso(callLogDoc, campaign, { inbound = false } = {}) {
    const campaignDefault =
        campaign?.companyCountryIso || campaign?.contactImportCountryIsoOverride || "IN";

    if (inbound) {
        const linePhone = extractDestinationPhone(callLogDoc || {});
        if (linePhone) return countryIsoFromPhone(linePhone, campaignDefault);
    }

    return normalizeCountryIso(campaignDefault) || "IN";
}

module.exports = {
    countryIsoFromPhone,
    extractDestinationPhone,
    extractCallerPhone,
    phoneFromInboundEvents,
    resolveCountryBillingPhone,
    resolveDefaultCountryIso,
};
