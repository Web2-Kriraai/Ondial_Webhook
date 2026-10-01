/**
 * Pricing basis + shadow helpers for foreign (twilio/telnyx) calls only.
 * Never alters the live DID charge path; shadow writes are best-effort.
 */
const {
    countryIsoFromPhone,
    resolveCountryBillingPhone,
    resolveCalleeBillingPhone,
    resolveDefaultCountryIso,
} = require("./countryFromPhone");
const { resolvePlanRate, loadCountryPricingConfig, TIER_TO_PACKAGE_ID } = require("./countryPricing");
const { ObjectId } = require("mongodb");
const logger = require("../logger");

const FALLBACK_BRACKETS = [
    { fromSecond: 1, toSecond: 15, percentOfRatePerMinute: 25 },
    { fromSecond: 16, toSecond: 30, percentOfRatePerMinute: 50 },
    { fromSecond: 31, toSecond: 45, percentOfRatePerMinute: 75 },
    { fromSecond: 46, toSecond: 60, percentOfRatePerMinute: 100 },
];

function envFlagTrue(name, defaultTrue = false) {
    const raw = process.env[name];
    if (raw == null || String(raw).trim() === "") return defaultTrue;
    const v = String(raw).trim().toLowerCase();
    return v !== "0" && v !== "false" && v !== "off" && v !== "no";
}

function parseProviderList(raw, fallback = ["twilio", "telnyx"]) {
    const s = String(raw || "").trim();
    if (!s) return fallback;
    return s
        .split(",")
        .map((x) => x.trim().toLowerCase())
        .filter(Boolean)
        .filter((p) => p !== "pool");
}

function normalizeBasis(raw) {
    const v = String(raw || "").trim().toLowerCase();
    if (v === "destination" || v === "max_of_both" || v === "did") return v;
    return "did";
}

/**
 * Resolution: user.billingOverride.pricingBasis → systemsettings.pricingBasisV1 → env → did
 */
async function resolvePricingBasis(db, user) {
    const fromUser = user?.billingOverride?.pricingBasis;
    if (fromUser) return normalizeBasis(fromUser);

    try {
        const row = await db.collection("systemsettings").findOne({ key: "pricingBasisV1" });
        if (row?.value != null) {
            const v = typeof row.value === "string" ? row.value : row.value?.basis;
            if (v) return normalizeBasis(v);
        }
    } catch {
        /* ignore */
    }

    return normalizeBasis(process.env.PRICING_BASIS || "did");
}

function isForeignBillingTargetLocal({ campaign, provider } = {}) {
    if (campaign?.isForeign === true) return true;
    const p = String(provider || campaign?.numberPolicySnapshot?.provider || "")
        .trim()
        .toLowerCase();
    if (p === "twilio" || p === "telnyx") return true;
    const billing = String(campaign?.numberPolicySnapshot?.billingMode || "")
        .trim()
        .toLowerCase();
    return billing === "per_number";
}

function providersAllow(provider) {
    const list = parseProviderList(process.env.PRICING_BASIS_PROVIDERS);
    const p = String(provider || "").trim().toLowerCase();
    if (!p) return false;
    return list.includes(p);
}

function roundSix(n) {
    return parseFloat(Number(n).toFixed(6));
}

function computeCost(durationSec, ratePerMinute, brackets = FALLBACK_BRACKETS) {
    const fullMinutes = Math.floor(durationSec / 60);
    const remainingSeconds = durationSec % 60;
    let partialMinuteFraction = 0;
    if (remainingSeconds > 0) {
        const matched = brackets.find(
            (b) => remainingSeconds >= b.fromSecond && remainingSeconds <= b.toSecond
        );
        const bracket = matched || brackets[brackets.length - 1];
        partialMinuteFraction = (bracket?.percentOfRatePerMinute ?? 100) / 100;
    }
    return roundSix(fullMinutes * ratePerMinute + partialMinuteFraction * ratePerMinute);
}

function packageIdFromUser(user) {
    const tier = String(user?.creditPlan?.currentTier || "A").toUpperCase();
    return TIER_TO_PACKAGE_ID[tier] || "starter";
}

function voiceTierFromCampaign(campaign) {
    return String(campaign?.selectedVoice?.tier || "standard").toLowerCase();
}

/**
 * Best-effort shadow log. Never throws to caller.
 */
async function maybeWritePricingShadowLog(db, payload) {
    try {
        if (!envFlagTrue("PRICING_SHADOW_MODE", true)) return { skipped: true, reason: "shadow_off" };
        if (!payload?.callId) return { skipped: true, reason: "no_call_id" };

        await db.collection("pricing_shadow_log").updateOne(
            { callId: String(payload.callId) },
            {
                $set: {
                    ...payload,
                    callId: String(payload.callId),
                    updatedAt: new Date(),
                },
                $setOnInsert: { createdAt: new Date() },
            },
            { upsert: true }
        );
        return { ok: true };
    } catch (err) {
        logger.warn("[PricingShadow] write failed (ignored)", {
            error: err?.message || String(err),
            callId: payload?.callId || null,
        });
        return { ok: false, error: String(err?.message || err) };
    }
}

/**
 * After live DID charge is computed, also compute dest / max for foreign shadow.
 * Live charge path must already have used DID; this only logs.
 */
async function recordForeignPricingShadow({
    db,
    callLogDoc,
    campaign,
    user,
    durationSec,
    liveCharge,
    liveRate,
    liveCountryIso,
    callId,
    provider,
    brackets,
    inbound = false,
}) {
    try {
        if (!envFlagTrue("PRICING_SHADOW_MODE", true)) return;
        if (!isForeignBillingTargetLocal({ campaign, provider })) return;
        if (!providersAllow(provider || campaign?.numberPolicySnapshot?.provider)) return;

        const dur = Math.max(0, Math.floor(Number(durationSec) || 0));
        const didPhone = resolveCountryBillingPhone(callLogDoc, { inbound, campaign });
        const defaultCountry = resolveDefaultCountryIso(callLogDoc, campaign, { inbound });
        const didIso =
            liveCountryIso ||
            countryIsoFromPhone(didPhone, defaultCountry) ||
            defaultCountry ||
            "IN";

        const calleePhone = resolveCalleeBillingPhone(callLogDoc, { campaign });
        const destIso = countryIsoFromPhone(calleePhone, defaultCountry) || defaultCountry || "IN";

        const missingPolicy = String(process.env.PRICING_MISSING_POLICY || "m2")
            .trim()
            .toLowerCase();

        const countryConfig = await loadCountryPricingConfig(db);
        const packageId = packageIdFromUser(user);
        const voiceTier = voiceTierFromCampaign(campaign);
        const telephonyProvider =
            provider ||
            campaign?.numberPolicySnapshot?.provider ||
            campaign?.selectedPhoneProvider ||
            null;

        const resolve = (iso) =>
            resolvePlanRate({
                config: countryConfig,
                countryIso: iso,
                provider: telephonyProvider,
                packageId,
                voiceTier,
                fallbackRatePerMinute: liveRate,
            });

        const didResolved = resolve(didIso);
        let destResolved = resolve(destIso);

        if (destResolved.source === "fallback" || destResolved.source === "package") {
            logger.warn("[pricing_fallback]", {
                callId,
                destIso,
                source: destResolved.source,
                policy: missingPolicy,
                provider: telephonyProvider,
            });
            if (missingPolicy === "m1" && destResolved.source === "fallback") {
                // LIVE destination would refuse country cell; shadow still records rates.
            }
        }

        const rateDid = Number(didResolved.ratePerMinute);
        const rateDest = Number(destResolved.ratePerMinute);
        const chargeDid =
            liveCharge != null && Number.isFinite(Number(liveCharge))
                ? roundSix(Number(liveCharge))
                : computeCost(dur, rateDid, brackets || FALLBACK_BRACKETS);
        const chargeDest = computeCost(dur, rateDest, brackets || FALLBACK_BRACKETS);
        const chargeMax = computeCost(
            dur,
            Math.max(rateDid, rateDest),
            brackets || FALLBACK_BRACKETS
        );

        const basis = await resolvePricingBasis(db, user);

        await maybeWritePricingShadowLog(db, {
            callId: String(callId),
            provider: String(telephonyProvider || "unknown").toLowerCase(),
            campaignId: campaign?._id || campaign?.id || null,
            contactId: callLogDoc?.contact_id || null,
            didIso: String(didIso).toUpperCase(),
            destIso: String(destIso).toUpperCase(),
            rateDid,
            rateDest,
            chargeDid,
            chargeDest,
            chargeMax,
            durationSec: dur,
            packageId,
            voiceTier,
            salePriceEnabled: Boolean(didResolved.salePriceEnabled),
            pricingBasisResolved: basis,
            calleePhone: calleePhone || null,
            didPhone: didPhone || null,
        });
    } catch (err) {
        logger.warn("[PricingShadow] recordForeignPricingShadow failed (ignored)", {
            error: err?.message || String(err),
            callId: callId || null,
        });
    }
}

module.exports = {
    envFlagTrue,
    parseProviderList,
    resolvePricingBasis,
    isForeignBillingTargetLocal,
    recordForeignPricingShadow,
    maybeWritePricingShadowLog,
    computeCost,
    FALLBACK_BRACKETS,
};
