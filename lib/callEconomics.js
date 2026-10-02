/**
 * Phase C — observe-only provider cost / margin (twilio/telnyx).
 * Never changes customer charges. Gated by CALL_ECONOMICS_ENABLED=0 default.
 */
const logger = require("../logger");

function envEnabled(name, defaultOn = false) {
    const raw = process.env[name];
    if (raw == null || String(raw).trim() === "") return defaultOn;
    const v = String(raw).trim().toLowerCase();
    return v !== "0" && v !== "false" && v !== "off" && v !== "no";
}

function parseProviders(raw) {
    const s = String(raw || "twilio,telnyx").trim();
    return s
        .split(",")
        .map((x) => x.trim().toLowerCase())
        .filter((p) => p && p !== "pool");
}

function roundSix(n) {
    return parseFloat(Number(n).toFixed(6));
}

function ceilToInterval(seconds, intervalSec) {
    const s = Math.max(0, Number(seconds) || 0);
    const iv = Math.max(1, Number(intervalSec) || 60);
    return Math.ceil(s / iv) * iv;
}

async function loadProviderFees(db) {
    const defaults = {
        telnyx: {
            callControlPerMin: 0.002,
            recordingPerMin: 0.002,
            transferPerInvocation: 0.1,
        },
        twilio: {
            // UNVERIFIED — placeholders only
            recordingPerMin: 0,
            storagePerMin: 0,
        },
    };
    try {
        const row = await db.collection("systemsettings").findOne({ key: "providerFeesV1" });
        if (row?.value && typeof row.value === "object") {
            return { ...defaults, ...row.value };
        }
    } catch {
        /* defaults */
    }
    return defaults;
}

const RATE_CARD_PROJECTION = {
    destinationPrefix: 1,
    countryIso: 1,
    routeType: 1,
    rateUsdPerMin: 1,
    interval1Sec: 1,
    intervalNSec: 1,
    perCallFeeUsd: 1,
    effectiveFrom: 1,
};

/**
 * Build candidate prefixes (longest → shortest) for $in lookup.
 * Caps at 15 digits (E.164).
 */
function buildPrefixCandidates(destinationDigits) {
    const digits = String(destinationDigits || "").replace(/\D/g, "");
    if (!digits) return [];
    const maxLen = Math.min(digits.length, 15);
    const out = [];
    for (let len = maxLen; len >= 1; len--) {
        out.push(digits.slice(0, len));
    }
    return out;
}

/**
 * Longest-prefix match on destination digits against provider_rate_cards.
 * Uses candidate truncations + indexed $in (no full-collection scan).
 */
async function lookupRateCard(db, { provider, destinationDigits, at = new Date() }) {
    const p = String(provider || "").toLowerCase();
    const digits = String(destinationDigits || "").replace(/\D/g, "");
    if (!p || !digits) return null;

    const candidates = buildPrefixCandidates(digits);
    if (!candidates.length) return null;

    const cards = await db
        .collection("provider_rate_cards")
        .find({
            provider: p,
            destinationPrefix: { $in: candidates },
            effectiveFrom: { $lte: at },
        })
        .project(RATE_CARD_PROJECTION)
        .toArray();

    let best = null;
    let bestLen = -1;
    let bestFrom = -1;
    for (const c of cards) {
        const pref = String(c.destinationPrefix || "").replace(/\D/g, "");
        if (!pref || !digits.startsWith(pref)) continue;
        const from = new Date(c.effectiveFrom || 0).getTime();
        if (pref.length > bestLen || (pref.length === bestLen && from > bestFrom)) {
            best = c;
            bestLen = pref.length;
            bestFrom = from;
        }
    }
    return best;
}

/**
 * Highest rateUsdPerMin for provider × country (effective cards only).
 * Returns the card row with the max rate (for prefix / audit), or null.
 */
async function lookupCountryMaxRate(db, { provider, countryIso, at = new Date() }) {
    const p = String(provider || "").toLowerCase();
    const iso = String(countryIso || "").toUpperCase();
    if (!p || !/^[A-Z]{2}$/.test(iso)) return null;

    const card = await db
        .collection("provider_rate_cards")
        .find({
            provider: p,
            countryIso: iso,
            effectiveFrom: { $lte: at },
        })
        .project(RATE_CARD_PROJECTION)
        .sort({ rateUsdPerMin: -1, effectiveFrom: -1 })
        .limit(1)
        .next();

    return card || null;
}

async function upsertCallEconomics(db, doc) {
    const callId = String(doc.callId || "").trim();
    if (!callId) return { skipped: true };
    await db.collection("call_economics").updateOne(
        { callId },
        {
            $set: { ...doc, callId, updatedAt: new Date() },
            $setOnInsert: { createdAt: new Date() },
        },
        { upsert: true }
    );
    return { ok: true };
}

/**
 * Best-effort economics write after customer charge is known.
 */
async function maybeRecordCallEconomics({
    db,
    callId,
    provider,
    destinationCountryIso,
    destinationPhone,
    talkDurationSec,
    providerBilledSeconds,
    customerChargeUsd,
    actualProviderCostUsd = null,
    transferCount = 0,
    recordingMinutes = null,
}) {
    try {
        if (!envEnabled("CALL_ECONOMICS_ENABLED", false)) {
            return { skipped: true, reason: "disabled" };
        }
        const p = String(provider || "").toLowerCase();
        if (p === "pool") return { skipped: true, reason: "pool_excluded" };
        if (!parseProviders(process.env.CALL_ECONOMICS_PROVIDERS).includes(p)) {
            return { skipped: true, reason: "provider_not_in_scope" };
        }

        const digits = String(destinationPhone || "").replace(/\D/g, "");
        const card = await lookupRateCard(db, { provider: p, destinationDigits: digits });
        const fees = await loadProviderFees(db);
        const feeCfg = fees[p] || {};

        let commissionPercent = null;
        let commissionSource = null;
        let targetCustomerChargeUsd = null;
        try {
            const { loadProviderCommission, resolveCommissionPercent } = require("./providerCommission");
            const commissionCfg = await loadProviderCommission(db);
            const resolved = resolveCommissionPercent(commissionCfg, {
                provider: p,
                countryIso: destinationCountryIso,
            });
            commissionPercent = resolved.percent;
            commissionSource = resolved.source;
        } catch {
            /* optional */
        }

        const billedSec =
            providerBilledSeconds != null && Number.isFinite(Number(providerBilledSeconds))
                ? Math.max(0, Math.floor(Number(providerBilledSeconds)))
                : Math.max(0, Math.floor(Number(talkDurationSec) || 0));

        const interval = Number(card?.intervalNSec || card?.interval1Sec || 60) || 60;
        const billedForEstimate = ceilToInterval(billedSec, interval);
        const minutes = billedForEstimate / 60;
        const rate = Number(card?.rateUsdPerMin || 0) || 0;
        const perCall = Number(card?.perCallFeeUsd || 0) || 0;

        let feeTotal = 0;
        const feeBreakdown = {};
        if (p === "telnyx") {
            feeBreakdown.callControl = roundSix(minutes * Number(feeCfg.callControlPerMin || 0));
            const recMin =
                recordingMinutes != null ? Number(recordingMinutes) : minutes;
            feeBreakdown.recording = roundSix(recMin * Number(feeCfg.recordingPerMin || 0));
            feeBreakdown.transfer = roundSix(
                Math.max(0, Number(transferCount) || 0) * Number(feeCfg.transferPerInvocation || 0)
            );
            feeTotal = roundSix(
                feeBreakdown.callControl + feeBreakdown.recording + feeBreakdown.transfer
            );
        } else if (p === "twilio") {
            // UNVERIFIED placeholders
            feeBreakdown.recording = roundSix(
                minutes * Number(feeCfg.recordingPerMin || 0)
            );
            feeBreakdown.storage = roundSix(minutes * Number(feeCfg.storagePerMin || 0));
            feeTotal = roundSix(feeBreakdown.recording + feeBreakdown.storage);
        }

        const estimatedProviderCostUsd = roundSix(rate * minutes + perCall + feeTotal);
        const actual =
            actualProviderCostUsd != null && Number.isFinite(Number(actualProviderCostUsd))
                ? roundSix(Number(actualProviderCostUsd))
                : null;
        const costUsed = actual != null ? actual : estimatedProviderCostUsd;
        const customer = roundSix(Number(customerChargeUsd) || 0);
        const marginUsd = roundSix(customer - costUsed);
        const marginPct = customer > 0 ? roundSix(marginUsd / customer) : null;

        if (commissionPercent != null && Number.isFinite(Number(commissionPercent))) {
            targetCustomerChargeUsd = roundSix(costUsed * (1 + Number(commissionPercent) / 100));
        }

        await upsertCallEconomics(db, {
            callId: String(callId),
            provider: p,
            destinationCountryIso: destinationCountryIso
                ? String(destinationCountryIso).toUpperCase()
                : null,
            rateCardId: card?._id || null,
            providerBilledSeconds: billedSec,
            estimatedProviderCostUsd,
            actualProviderCostUsd: actual,
            customerChargeUsd: customer,
            marginUsd,
            marginPct,
            commissionPercent,
            commissionSource,
            targetCustomerChargeUsd,
            costSource: actual != null ? "actual" : "estimated",
            fees: feeBreakdown,
            feesV2: {
                // Wave 7 / Phase G: extended fee structure only (no customer surcharge).
                callControl: feeBreakdown.callControl ?? 0,
                recording: feeBreakdown.recording ?? 0,
                transfer: feeBreakdown.transfer ?? 0,
                storage: feeBreakdown.storage ?? 0,
                other: feeBreakdown.other ?? 0,
            },
        });
        return { ok: true, estimatedProviderCostUsd, marginUsd };
    } catch (err) {
        logger.warn("[CallEconomics] record failed (ignored)", {
            error: err?.message || String(err),
            callId: callId || null,
        });
        return { ok: false, error: String(err?.message || err) };
    }
}

async function ensureCallEconomicsIndexes(database) {
    const loggerLocal = logger;
    try {
        await database.collection("provider_rate_cards").createIndex(
            { provider: 1, destinationPrefix: 1, routeType: 1, effectiveFrom: 1 },
            { name: "provider_rate_cards_unique", unique: true }
        );
        await database.collection("provider_rate_cards").createIndex(
            { provider: 1, destinationPrefix: 1, effectiveFrom: 1 },
            { name: "provider_rate_cards_prefix_lookup" }
        );
        await database.collection("provider_rate_cards").createIndex(
            { provider: 1, countryIso: 1, rateUsdPerMin: -1, effectiveFrom: -1 },
            { name: "provider_rate_cards_country_max" }
        );
        await database.collection("call_economics").createIndex(
            { callId: 1 },
            { name: "call_economics_callId_unique", unique: true }
        );
        await database.collection("call_economics").createIndex(
            { provider: 1, destinationCountryIso: 1, createdAt: -1 },
            { name: "call_economics_provider_dest" }
        );
        loggerLocal.info("[DB] call_economics / provider_rate_cards indexes ensured");
    } catch (err) {
        loggerLocal.warn("[DB] call_economics indexes partial failure", {
            error: err?.message || String(err),
        });
    }
}

/**
 * When Telnyx call.cost arrives later, patch actual cost + margin if economics row exists.
 */
async function maybeUpdateActualProviderCost({ db, callId, actualProviderCostUsd, providerBilledSeconds }) {
    try {
        if (!envEnabled("CALL_ECONOMICS_ENABLED", false)) {
            return { skipped: true, reason: "disabled" };
        }
        const id = String(callId || "").trim();
        if (!id) return { skipped: true };
        const actual = Number(actualProviderCostUsd);
        if (!Number.isFinite(actual)) return { skipped: true };

        const existing = await db.collection("call_economics").findOne({ callId: id });
        if (!existing) return { skipped: true, reason: "no_row" };

        const customer = roundSix(Number(existing.customerChargeUsd) || 0);
        const marginUsd = roundSix(customer - roundSix(actual));
        const marginPct = customer > 0 ? roundSix(marginUsd / customer) : null;
        const set = {
            actualProviderCostUsd: roundSix(actual),
            costSource: "actual",
            marginUsd,
            marginPct,
            updatedAt: new Date(),
        };
        if (providerBilledSeconds != null && Number.isFinite(Number(providerBilledSeconds))) {
            set.providerBilledSeconds = Math.max(0, Math.floor(Number(providerBilledSeconds)));
        }
        await db.collection("call_economics").updateOne({ callId: id }, { $set: set });
        return { ok: true };
    } catch (err) {
        logger.warn("[CallEconomics] actual cost update failed (ignored)", {
            error: err?.message || String(err),
            callId: callId || null,
        });
        return { ok: false };
    }
}

module.exports = {
    maybeRecordCallEconomics,
    maybeUpdateActualProviderCost,
    lookupRateCard,
    lookupCountryMaxRate,
    buildPrefixCandidates,
    ensureCallEconomicsIndexes,
    ceilToInterval,
    envEnabled,
};
