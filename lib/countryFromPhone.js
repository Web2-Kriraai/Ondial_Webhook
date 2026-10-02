const DIAL_PREFIX_TO_ISO_BASE = [
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

/** Dominican Republic NANP — must appear before ["1","US"] for longest-prefix match. */
const NANP_ISLAND_PREFIXES = [
    ["1809", "DO"],
    ["1829", "DO"],
    ["1849", "DO"],
];

function nanpIslandPrefixesEnabled() {
    const raw = process.env.NANP_ISLAND_PREFIXES;
    if (raw == null || String(raw).trim() === "") return true;
    const v = String(raw).trim().toLowerCase();
    return v !== "0" && v !== "false" && v !== "off" && v !== "no";
}

function getDialPrefixTable() {
    if (!nanpIslandPrefixesEnabled()) return DIAL_PREFIX_TO_ISO_BASE;
    // Insert island prefixes immediately before the NANP country code "1".
    const out = [];
    for (const row of DIAL_PREFIX_TO_ISO_BASE) {
        if (row[0] === "1") out.push(...NANP_ISLAND_PREFIXES);
        out.push(row);
    }
    return out;
}

/** @deprecated use getDialPrefixTable(); kept for tests that read the constant shape */
const DIAL_PREFIX_TO_ISO = DIAL_PREFIX_TO_ISO_BASE;

function normalizeCountryIso(raw) {
    if (raw == null) return null;
    const iso = String(raw).trim().toUpperCase();
    return /^[A-Z]{2}$/.test(iso) ? iso : null;
}

function countryFromDialDigits(digits) {
    if (!digits) return null;
    for (const [prefix, iso] of getDialPrefixTable()) {
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

/** ISO → ITU dialing code (first match from dial table; CA shares US NANP "1"). */
const ISO_TO_DIAL_CODE = (() => {
    const map = Object.create(null);
    for (const [prefix, iso] of DIAL_PREFIX_TO_ISO_BASE) {
        if (!map[iso]) map[iso] = prefix;
    }
    if (!map.CA) map.CA = "1";
    return map;
})();

function isoToDialCode(countryIso) {
    const iso = normalizeCountryIso(countryIso);
    return iso ? ISO_TO_DIAL_CODE[iso] || null : null;
}

/**
 * Digits for longest-prefix rate-card lookup.
 * Local numbers (e.g. IN 10-digit without 91) get the country dial code prepended
 * using the already-resolved destination ISO so cards keyed as 91… / 1… / 971… match.
 */
function destinationDigitsForPrefixLookup(phone, countryIso) {
    let digits = String(phone || "").replace(/\D/g, "");
    if (!digits) return "";
    if (digits.startsWith("00")) digits = digits.slice(2);
    if (!digits) return "";

    const dial = isoToDialCode(countryIso);
    if (!dial) return digits;
    if (digits.startsWith(dial)) return digits;

    // National trunk prefix (0…) — strip before prepending country code.
    let national = digits;
    if (national.startsWith("0")) {
        national = national.replace(/^0+/, "");
        if (!national) return digits;
        if (national.startsWith(dial)) return national;
    }

    return dial + national;
}

function firstNonEmpty(...values) {
    for (const v of values) {
        if (v != null && String(v).trim()) return String(v).trim();
    }
    return null;
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
        // Twilio StatusCallback
        "To",
        "Called",
        "CalledVia",
    ];
    for (const key of keys) {
        const v = sources[key];
        if (v != null && String(v).trim()) return String(v).trim();
    }
    // Nested Telnyx / provider payloads
    const nested = firstNonEmpty(
        sources?.payload?.to,
        sources?.telnyx?.to,
        sources?.twilio?.to,
        sources?._raw?.call?.to,
        sources?._raw?.To,
        sources?._raw?.Called
    );
    return nested;
}

/**
 * Callee (destination) phone for foreign destination-based pricing shadow.
 * Does NOT prefer campaign.selectedPhoneNumber (that is the DID).
 */
function resolveCalleeBillingPhone(callLogDoc, { campaign = null } = {}) {
    const doc = callLogDoc || {};
    return (
        extractDestinationPhone(doc) ||
        extractDestinationPhone(doc.call_data || {}) ||
        extractDestinationPhone(doc.twilio || {}) ||
        extractDestinationPhone(doc.telnyx || {}) ||
        firstNonEmpty(doc?.contact_phone, doc?.phone, campaign?.lastDialedTo) ||
        null
    );
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
    isoToDialCode,
    destinationDigitsForPrefixLookup,
    extractDestinationPhone,
    extractCallerPhone,
    phoneFromInboundEvents,
    resolveCountryBillingPhone,
    resolveCalleeBillingPhone,
    resolveDefaultCountryIso,
    getDialPrefixTable,
    NANP_ISLAND_PREFIXES,
    DIAL_PREFIX_TO_ISO,
};
