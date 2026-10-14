/**
 * Campaign outbound credit deduction on call_hangup (CallLogs).
 * Mirrors Calling_system1 worker bracket billing; idempotent via reference.billingKey.
 */
const { ObjectId } = require("mongodb");
const { getDb } = require("../db");
const { objectIdToString } = require("./mongoObjectId");
const logger = require("../logger");
const { shouldSkipCreditDeduction } = require("./shouldSkipCreditDeduction");
const { pauseActiveCampaignsForInsufficientCredits } = require("./pauseCampaignsForInsufficientCredits");
const {
    resolvePlanRate,
    loadCountryPricingConfig,
    TIER_TO_PACKAGE_ID,
} = require("./countryPricing");
const {
    countryIsoFromPhone,
    resolveCountryBillingPhone,
    resolveDefaultCountryIso,
} = require("./countryFromPhone");

const CALLLOGS_COLLECTION = process.env.CALLLOGS_COLLECTION || "CallLogs";
const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";
const INBOUNDCALLLOG_COLLECTION = process.env.INBOUNDCALLLOG_COLLECTION || "InboundConversation";

function callLogFilterForCollection(callId, collectionName) {
    const id = String(callId || "").trim();
    if (collectionName === INBOUNDCALLLOG_COLLECTION) {
        return { call_id: id };
    }
    // Prefer call_id equality (indexed). Callers that need multi-field resolve should
    // use findCallLogsByIdentity; this filter is for updateOne/findOne when id is dialer UUID.
    return { $or: [{ call_id: id }, { call_unique_id: id }, { lead_id: id }] };
}

const FALLBACK_BRACKETS = [
    { fromSecond: 1, toSecond: 15, percentOfRatePerMinute: 25 },
    { fromSecond: 16, toSecond: 30, percentOfRatePerMinute: 50 },
    { fromSecond: 31, toSecond: 45, percentOfRatePerMinute: 75 },
    { fromSecond: 46, toSecond: 60, percentOfRatePerMinute: 100 },
];

/**
 * After a charge, pause the org's campaigns when the wallet drops below this (USD credits).
 * Matches the lowest per-minute plan rate (starter/standard), i.e. < one billable minute left.
 */
const LOW_BALANCE_PAUSE_THRESHOLD = 0.065;

function roundSix(n) {
    return parseFloat(Number(n).toFixed(6));
}

/** First non-empty phone-like string from candidates. */
function firstPhoneToken(...candidates) {
    for (const raw of candidates) {
        const s = String(raw ?? "").trim();
        if (!s || s === "+" || s === "-" || s === "—" || s === "–") continue;
        return s;
    }
    return null;
}

/**
 * Phone shown in credit history for a call.
 * Outbound → dialed party (to_number). Inbound → caller (from_number).
 * Twilio CallLogs often omit from_number; do not rely on it for outbound.
 */
function resolveContactPhoneForCredit(callLogDoc, { inbound = false } = {}) {
    if (!callLogDoc || typeof callLogDoc !== "object") return null;
    const twilio = callLogDoc.twilio && typeof callLogDoc.twilio === "object" ? callLogDoc.twilio : {};
    if (inbound) {
        return firstPhoneToken(
            callLogDoc.caller_number,
            callLogDoc.from_number,
            callLogDoc.phone_number,
            callLogDoc.contact_phone,
            callLogDoc.mobileNumber,
            twilio.From,
            twilio.Caller
        );
    }
    return firstPhoneToken(
        callLogDoc.to_number,
        callLogDoc.phone_number,
        callLogDoc.contact_phone,
        callLogDoc.mobileNumber,
        twilio.To,
        twilio.Called,
        callLogDoc.from_number,
        callLogDoc.caller_number,
        twilio.From,
        twilio.Caller
    );
}

function kbRatePerMinuteFromCampaign(campaign) {
    const v = Number(campaign?.kbRatePerMinute);
    if (!Number.isFinite(v) || v < 0) return 0;
    return roundSix(v);
}

function wordCountFromInboundConfig(cfg) {
    if (!Array.isArray(cfg?.selectedKnowledgebases)) return 0;
    return cfg.selectedKnowledgebases.reduce((sum, kb) => {
        const wc = Number(kb?.wordCount);
        return sum + (Number.isFinite(wc) && wc > 0 ? Math.floor(wc) : 0);
    }, 0);
}

const KB_LEGACY_SLABS = [
    { minWords: 1000, maxWords: 1500, addOnInrPerMinute: 0.5 },
    { minWords: 1501, maxWords: 2000, addOnInrPerMinute: 0.7 },
    { minWords: 2001, maxWords: 2500, addOnInrPerMinute: 0.9 },
    { minWords: 2501, maxWords: 3000, addOnInrPerMinute: 0.95 },
    { minWords: 3001, maxWords: 3500, addOnInrPerMinute: 1 },
    { minWords: 3501, maxWords: 4000, addOnInrPerMinute: 1.1 },
    { minWords: 4001, maxWords: 4500, addOnInrPerMinute: 1.3 },
    { minWords: 4501, maxWords: 5000, addOnInrPerMinute: 1.4 },
    { minWords: 5001, maxWords: null, addOnInrPerMinute: 1.5 },
];

function kbRateFromSlabs(wordCount, slabs) {
    const wc = Math.floor(Number(wordCount) || 0);
    if (wc < 1000) return 0;
    for (const slab of slabs || []) {
        const min = Number(slab?.minWords);
        const maxRaw = slab?.maxWords;
        const max = maxRaw == null ? Infinity : Number(maxRaw);
        const rate = Number(slab?.addOnInrPerMinute);
        if (!Number.isFinite(min) || !Number.isFinite(rate)) continue;
        if (wc >= min && wc <= max) return rate;
    }
    return 0;
}

async function kbRatePerMinuteFromInboundConfig(db, cfg) {
    const explicit = Number(cfg?.kbRatePerMinute);
    if (Number.isFinite(explicit) && explicit > 0) return roundSix(explicit);

    const words = wordCountFromInboundConfig(cfg);
    if (words < 1000) return 0;

    let slabs = KB_LEGACY_SLABS;
    let inrPerUsd = null;
    try {
        const row =
            (await db.collection("systemsettings").findOne({ key: "knowledgeBasePrice" })) ||
            (await db.collection("systemsettings").findOne({ key: "knowledgebase" }));
        if (Array.isArray(row?.value?.slabs) && row.value.slabs.length) {
            slabs = row.value.slabs;
        }
        const fx = Number(row?.value?.inrPerUsd);
        if (Number.isFinite(fx) && fx > 0) inrPerUsd = fx;
    } catch {
        /* use legacy */
    }

    const inrRate = kbRateFromSlabs(words, slabs);
    if (!inrRate || !inrPerUsd) return 0;
    return roundSix(inrRate / inrPerUsd);
}

/**
 * Load campaign, or fall back to inboundconfigs when id is a bot config (inbound DID billing).
 * When an inbound bot config is present, prefer it over a campaign id — campaign DID
 * matches are often polluted with other accounts' draft/completed campaigns.
 */
async function loadBillingEntity(db, { campaignId, inboundConfigId, preferInboundConfig = false }) {
    const campId = String(campaignId || "").trim();
    const cfgId = String(inboundConfigId || "").trim();
    const preferConfig = preferInboundConfig === true || (!!cfgId && ObjectId.isValid(cfgId));

    const tryInbound = async (id) => {
        if (!id || !ObjectId.isValid(id)) return null;
        const cfg = await db.collection("inboundconfigs").findOne({ _id: new ObjectId(id) });
        if (!cfg) return null;
        const kbRate = await kbRatePerMinuteFromInboundConfig(db, cfg);
        const entity = {
            ...cfg,
            name: cfg.name || "Inbound",
            selectedPhoneNumber: cfg.phoneNumber || cfg.selectedPhoneNumber || null,
            kbRatePerMinute: kbRate,
        };
        return { entity, kind: "inbound_config", billingId: String(cfg._id) };
    };

    const tryCampaign = async (id) => {
        if (!id || !ObjectId.isValid(id)) return null;
        const campaign = await db.collection("campaigns").findOne({ _id: new ObjectId(id) });
        if (!campaign) return null;
        return { entity: campaign, kind: "campaign", billingId: String(campaign._id) };
    };

    if (preferConfig) {
        const fromCfg = await tryInbound(cfgId);
        if (fromCfg) return fromCfg;
        // cfgId may have been a mis-tagged campaign id — try as campaign last.
        const fromCampViaCfg = await tryCampaign(cfgId);
        if (fromCampViaCfg) return fromCampViaCfg;
    }

    const fromCamp = await tryCampaign(campId);
    if (fromCamp) return fromCamp;

    const fromCfgFallback = await tryInbound(cfgId || campId);
    if (fromCfgFallback) return fromCfgFallback;

    return null;
}

function callSidVariantsForUpdate(sid) {
    try {
        const { callSidLookupVariants } = require("./inboundDocAnchor");
        return callSidLookupVariants(sid);
    } catch {
        const raw = String(sid || "").trim();
        return raw ? [raw] : [];
    }
}

function computeCost(durationSec, ratePerMinute, brackets) {
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

/** Fire tenant CRM billed (+ optional completed status). Never throws. */
async function notifyOutboundTenantBilledBestEffort({
    userId,
    contactId,
    campaignId,
    callId,
    balanceAfter,
    creditsCharged,
    inboundLog,
    billingKind,
    alsoStatusCompleted = false,
}) {
    if (billingKind !== "campaign" || inboundLog || !userId) return;
    try {
        const {
            notifyTenantCallBilled,
            notifyTenantCallStatus,
        } = require("./notifyTenantCallStatus");
        const billedRes = await notifyTenantCallBilled({
            userId,
            contactId: contactId || null,
            campaignId: campaignId || null,
            callId,
            balanceAfter,
            creditsCharged,
        });
        if (!billedRes?.ok) {
            logger.warn("[Credit] tenant billed notify not ok", {
                callId,
                campaignId: campaignId != null ? String(campaignId) : null,
                result: billedRes,
            });
        }
        if (alsoStatusCompleted) {
            const statusRes = await notifyTenantCallStatus({
                userId,
                contactId: contactId || null,
                campaignId: campaignId || null,
                callId,
                callReceiveStatus: 3,
                status: "completed",
            });
            if (!statusRes?.ok) {
                logger.warn("[Credit] tenant status notify not ok", {
                    callId,
                    result: statusRes,
                });
            }
        }
    } catch (notifyErr) {
        logger.warn("[Credit] tenant billed/status notify failed", {
            error: notifyErr?.message,
            callId,
        });
    }
}

/**
 * Point 14: which billingOverride prices the per-minute rate. The WALLET is always the billing
 * user; the creator's (org member's) own override wins when it is enabled, else the wallet's.
 * Plan tier / country pricing are still taken from the billing user by the caller.
 * @returns {{ override: object|null, source: "creator"|"wallet"|null }}
 */
function pickRateOverride(creatorUser, billingUser) {
    const sameUser =
        creatorUser && billingUser && String(creatorUser._id) === String(billingUser._id);
    if (!sameUser && creatorUser?.billingOverride?.enabled === true) {
        return { override: creatorUser.billingOverride, source: "creator" };
    }
    if (billingUser?.billingOverride) {
        return { override: billingUser.billingOverride, source: "wallet" };
    }
    return { override: null, source: null };
}

async function resolveRatePerMinute(db, user, campaign, callLogDoc = null, options = {}) {
    const inbound = options.inbound === true;
    const voiceTier = campaign?.selectedVoice?.tier || "standard";
    let ratePerMinute = 0.08;
    let billingOverrideApplied = false;
    let destinationCountryIso = null;
    let countryRateApplied = false;
    let countryRateSource = null;

    const picked = options.rateOverride || pickRateOverride(null, user);
    const override = picked.override;
    let billingOverrideSource = null;
    if (override?.enabled === true) {
        let overrideRate = null;
        if (typeof override.flatRatePerMinute === "number" && override.flatRatePerMinute >= 0) {
            overrideRate = override.flatRatePerMinute;
        } else {
            // Per voice tier; basic uses basicRatePerMinute only (no fallback to standard).
            const map = {
                basic: override.basicRatePerMinute,
                standard: override.standardRatePerMinute,
                premium: override.premiumRatePerMinute,
                elite: override.eliteRatePerMinute,
            };
            const tierRate = map[voiceTier];
            if (typeof tierRate === "number" && tierRate >= 0) overrideRate = tierRate;
        }
        if (overrideRate !== null) {
            if (typeof override.chargeMultiplier === "number" && override.chargeMultiplier > 0) {
                overrideRate = roundSix(overrideRate * override.chargeMultiplier);
            }
            ratePerMinute = overrideRate;
            billingOverrideApplied = true;
            billingOverrideSource = picked.source || null;
        }
    }

    let targetPackageId = "starter";
    if (!billingOverrideApplied) {
        let currentTier = user?.creditPlan?.currentTier || "A";
        targetPackageId = TIER_TO_PACKAGE_ID[currentTier] || "starter";
        const pkg = await db.collection("creditpackages").findOne({ packageId: targetPackageId });
        if (pkg) {
            if (voiceTier === "standard" && typeof pkg.standardModelPrice === "number") {
                ratePerMinute = pkg.standardModelPrice;
            } else if (voiceTier === "premium" && typeof pkg.premiumModelPrice === "number") {
                ratePerMinute = pkg.premiumModelPrice;
            } else if (voiceTier === "elite" && typeof pkg.eliteModelPrice === "number") {
                ratePerMinute = pkg.eliteModelPrice;
            } else if (typeof pkg.pricePerMinute === "number") {
                ratePerMinute = pkg.pricePerMinute;
            }
        }
    }

    // Layer 2: country-based rate (country x plan-package x voice-tier) overrides the
    // package rate (Layer 3) resolved above. Number-based billing: uses the campaign's
    // own selected/purchased line (outbound) or the OWNED DID that was called (inbound),
    // not the other party's country — matches how the number itself was priced/purchased.
    if (!billingOverrideApplied) {
        try {
            const billingPhone = resolveCountryBillingPhone(callLogDoc, { inbound, campaign });
            const defaultCountry = resolveDefaultCountryIso(callLogDoc, campaign, { inbound });
            destinationCountryIso = countryIsoFromPhone(billingPhone, defaultCountry);
            const countryConfig = await loadCountryPricingConfig(db);
            let telephonyProvider =
                campaign?.numberPolicySnapshot?.provider ||
                campaign?.selectedPhoneProvider ||
                null;
            if (!telephonyProvider && billingPhone) {
                try {
                    const phoneDoc = await db.collection("phonenumbers").findOne(
                        { number: billingPhone },
                        { projection: { provider: 1 } }
                    );
                    telephonyProvider = phoneDoc?.provider || null;
                } catch {
                    /* ignore — fall back to country-level rates */
                }
            }
            const resolved = resolvePlanRate({
                config: countryConfig,
                countryIso: destinationCountryIso,
                provider: telephonyProvider,
                packageId: targetPackageId,
                voiceTier,
                fallbackRatePerMinute: ratePerMinute,
            });
            ratePerMinute = resolved.ratePerMinute;
            destinationCountryIso = resolved.countryIso;
            countryRateApplied = resolved.source !== "package";
            countryRateSource = resolved.source;
            if (inbound && countryRateApplied) {
                logger.info("[Credit] Inbound country rate applied", {
                    billingPhone,
                    destinationCountryIso,
                    countryRateSource,
                    ratePerMinute,
                });
            }
        } catch (countryErr) {
            logger.warn("[Credit] Country rate lookup failed", { message: countryErr?.message });
        }
    }

    // Multiplier with no override rate for this call: scale the normal plan / country
    // price (0 / empty = no multiplier). Same rule as the dialer, Ondial and Super Admin.
    if (!billingOverrideApplied && override?.enabled === true) {
        const multiplier = Number(override.chargeMultiplier);
        if (Number.isFinite(multiplier) && multiplier > 0) {
            ratePerMinute = roundSix(ratePerMinute * multiplier);
            billingOverrideApplied = true;
            billingOverrideSource = picked.source || null;
        }
    }

    const kbRate = kbRatePerMinuteFromCampaign(campaign);
    return {
        ratePerMinute: roundSix(ratePerMinute),
        kbRatePerMinute: kbRate,
        totalRatePerMinute: roundSix(ratePerMinute + kbRate),
        billingOverrideApplied,
        billingOverrideSource,
        destinationCountryIso,
        countryRateApplied,
        countryRateSource,
        inbound,
    };
}

/**
 * @returns {Promise<{ outcome: string, cost?: number, unpaid?: number }>}
 * outcome "partial_insufficient": the wallet covered only `cost` (balance → 0); `unpaid` is
 * the shortfall. The charge IS recorded (creditsDeducted:true) and org campaigns are paused.
 */
async function tryDeductCampaignCallCredits({
    callUniqueId,
    contactId,
    campaignId,
    inboundConfigId,
    preferInboundConfig,
    durationSec,
    callLogCollectionName,
    callLogLookupFilter,
    userId: explicitUserId,
}) {
    if (process.env.ONDIAL_CREDIT_DEDUCTION_ENABLED === "0") {
        return { outcome: "disabled" };
    }

    const callId = String(callUniqueId || "").trim();
    const dur = Math.max(0, Math.floor(Number(durationSec) || 0));
    if (!callId || dur <= 0) return { outcome: "skip_no_duration" };

    let campId = String(campaignId || "").trim();
    let configId = String(inboundConfigId || "").trim();
    if (!campId && contactId) {
        const { resolveCampaignIdFromContact } = require("./resolveCampaignId");
        campId = (await resolveCampaignIdFromContact(contactId)) || "";
    }
    if ((!campId || !ObjectId.isValid(campId)) && (!configId || !ObjectId.isValid(configId))) {
        logger.warn("[Credit] Missing campaign_id/inboundConfigId — cannot deduct", {
            callId,
            contactId,
        });
        return { outcome: "no_campaign_id" };
    }

    const db = getDb();
    const loaded = await loadBillingEntity(db, {
        campaignId: campId || null,
        inboundConfigId: configId || null,
        preferInboundConfig: preferInboundConfig === true || Boolean(configId),
    });
    if (!loaded) {
        logger.warn("[Credit] Billing entity not found", {
            callId,
            campaignId: campId || null,
            inboundConfigId: configId || null,
        });
        return { outcome: "campaign_not_found" };
    }

    const { entity: campaign, kind: billingKind, billingId } = loaded;
    const billingKey = `${billingId}:call:${callId}`;
    const logCollection = callLogCollectionName || CALLLOGS_COLLECTION;
    const inboundLog = logCollection === INBOUNDCALLLOG_COLLECTION;

    const existingTx = await db.collection("credittransactions").findOne({
        type: "call_deduction",
        "reference.billingKey": billingKey,
    });
    if (existingTx) {
        await notifyOutboundTenantBilledBestEffort({
            userId: existingTx.userId,
            contactId: contactId || existingTx.reference?.contactId || null,
            campaignId: billingKind === "campaign" ? billingId : null,
            callId,
            balanceAfter: existingTx.balanceAfter,
            creditsCharged: Math.abs(Number(existingTx.amount) || 0),
            inboundLog,
            billingKind,
        });
        return { outcome: "already_billed", cost: Math.abs(Number(existingTx.amount) || 0) };
    }

    // Also idempotent against Ondial inbound path (reference.callId, no billingKey).
    if (inboundLog) {
        const legacyTx = await db.collection("credittransactions").findOne({
            type: "call_deduction",
            "reference.callId": callId,
            $or: [
                { description: /^Inbound call/i },
                { "reference.source": /inbound/i },
            ],
        });
        if (legacyTx) {
            await notifyOutboundTenantBilledBestEffort({
                userId: legacyTx.userId,
                contactId: contactId || legacyTx.reference?.contactId || null,
                campaignId: null,
                callId,
                balanceAfter: legacyTx.balanceAfter,
                creditsCharged: Math.abs(Number(legacyTx.amount) || 0),
                inboundLog,
                billingKind,
            });
            return { outcome: "already_billed", cost: Math.abs(Number(legacyTx.amount) || 0) };
        }
    }

    const callLogFilter =
        callLogLookupFilter && typeof callLogLookupFilter === "object"
            ? callLogLookupFilter
            : callLogFilterForCollection(callId, logCollection);
    let callLogDoc = await db.collection(logCollection).findOne(callLogFilter);
    if (!callLogDoc) {
        const testDoc = await db.collection(TESTCALL_COLLECTION).findOne(callLogFilter);
        if (testDoc) {
            await db.collection(CALLLOGS_COLLECTION).updateOne(
                { lead_id: callId },
                {
                    $setOnInsert: {
                        lead_id: callId,
                        call_id: String(testDoc.call_id || callId),
                        campaign_id: testDoc.campaign_id != null ? String(testDoc.campaign_id) : "",
                        contact_id: testDoc.contact_id != null ? String(testDoc.contact_id) : "",
                        call_data: testDoc.call_data || { events: [] },
                        isTestCall: true,
                        createdAt: new Date().toISOString(),
                        recordingUrl: "",
                    },
                    $set: { updatedAt: new Date() },
                },
                { upsert: true }
            );
            callLogDoc = await db.collection(CALLLOGS_COLLECTION).findOne(callLogFilter);
        }
    }
    if (!callLogDoc) return { outcome: "call_log_not_found" };
    if (callLogDoc.creditsDeducted === true) {
        await notifyOutboundTenantBilledBestEffort({
            userId: callLogDoc.userId || campaign.userId || null,
            contactId: contactId || callLogDoc.contact_id || null,
            campaignId: billingKind === "campaign" ? billingId : null,
            callId,
            balanceAfter: null,
            creditsCharged: callLogDoc.creditsDeductedAmount ?? null,
            inboundLog,
            billingKind,
        });
        return { outcome: "already_billed", cost: callLogDoc.creditsDeductedAmount };
    }
    if (shouldSkipCreditDeduction({ callLogDoc })) {
        logger.info("[Credit] Skipping deduction — test call", { callId, contactId });
        return { outcome: "skip_test_call" };
    }

    const markCallLogCreditFields = async (fields) => {
        const sid = callLogDoc?.call_sid ? String(callLogDoc.call_sid) : "";
        if (inboundLog && sid) {
            const variants = callSidVariantsForUpdate(sid);
            if (variants.length) {
                await db.collection(logCollection).updateMany(
                    { call_sid: { $in: variants } },
                    { $set: fields }
                );
            }
            // Also patch the lookup doc when call_sid formats diverge (UUID vs call_<hex>).
            await db.collection(logCollection).updateOne(callLogFilter, { $set: fields });
            return;
        }
        await db.collection(logCollection).updateOne(callLogFilter, { $set: fields });
    };

    // Amount THIS attempt actually debited (may be < cost on a partial charge). Read by the
    // E11000 path so the refund matches what was taken, not the full call cost.
    let charged = 0;
    try {
        let user = null;
        // Org member the call belongs to (before the wallet rule) — its own billingOverride
        // can set the per-minute rate even though the billing user's wallet pays (point 14).
        let creatorUser = null;
        // Wave 5: shared wallet resolution (W1 parent). Shadow logs when BILLING_WALLET_SHADOW=1.
        try {
            const { resolveBillingUser } = require("./resolveBillingUser");
            const resolved = await resolveBillingUser(db, {
                campaign,
                callLogUserId: callLogDoc?.userId,
                explicitUserId,
            });
            user = resolved.user;
            creatorUser = resolved.creatorUser || null;
        } catch (walletErr) {
            logger.warn("[Credit] resolveBillingUser failed; falling back to legacy walk", {
                error: walletErr?.message || String(walletErr),
            });
        }
        if (!user) {
            const docUserId =
                objectIdToString(explicitUserId) || objectIdToString(callLogDoc?.userId);
            if (docUserId && ObjectId.isValid(docUserId)) {
                try {
                    user = await db.collection("users").findOne({ _id: new ObjectId(docUserId) });
                } catch {
                    /* ignore */
                }
            }
            if (!user && campaign.userId) {
                try {
                    user = await db.collection("users").findOne({ _id: new ObjectId(String(campaign.userId)) });
                } catch {
                    /* ignore */
                }
            }
            if (!user && campaign.createdBy) {
                user = await db.collection("users").findOne({ email: campaign.createdBy });
            }
            if (user) {
                creatorUser = user;
                // Same org wallet rule as resolveBillingUser: parent admin pays for an org user
                // unless the org is in billingWalletV1 "creator" mode.
                try {
                    const { applyBillingWalletPolicy } = require("./resolveBillingUser");
                    user = (await applyBillingWalletPolicy(db, user)) || user;
                } catch { /* ignore — keep creator wallet */ }
            }
        }
        if (!user) throw new Error("user_not_found");

        let brackets = FALLBACK_BRACKETS;
        const bracketSetting = await db.collection("systemsettings").findOne({ key: "callBillingBracketsV1" });
        if (bracketSetting?.value && Array.isArray(bracketSetting.value)) {
            brackets = bracketSetting.value;
        }

        const rates = await resolveRatePerMinute(db, user, campaign, callLogDoc, {
            inbound: inboundLog,
            rateOverride: pickRateOverride(creatorUser, user),
        });
        let cost = computeCost(dur, rates.totalRatePerMinute, brackets);
        let effectiveRate = rates.totalRatePerMinute;
        let liveCountryIso = rates.destinationCountryIso;
        let liveRateMeta = null;

        // Wave 4: LIVE destination / max_of_both for foreign only (default PRICING_BASIS=did → no-op).
        // Prefix-live when PRICING_DESTINATION_RATE=prefix (cost × commission, miss → country MAX).
        try {
            const { resolveForeignLiveRateSelection, applyPriceFloor } = require("./pricingShadow");
            const telephonyProvider =
                campaign?.numberPolicySnapshot?.provider ||
                campaign?.selectedPhoneProvider ||
                null;
            const sel = await resolveForeignLiveRateSelection({
                db,
                user,
                campaign,
                callLogDoc,
                liveDidRate: rates.totalRatePerMinute,
                liveDidIso: rates.destinationCountryIso,
                provider: telephonyProvider,
                inbound: inboundLog,
            });
            if (sel?.refuse) {
                await markCallLogCreditFields({
                    creditsDeducted: false,
                    creditDeductionError: "pricing_missing_country_m1",
                    processedAt: new Date(),
                    updatedAt: new Date(),
                });
                return { outcome: "error", error: "pricing_missing_country_m1" };
            }
            if (sel?.applied && Number.isFinite(sel.selectedRate)) {
                effectiveRate = sel.selectedRate;
                cost = computeCost(dur, effectiveRate, brackets);
                if (sel.basis === "destination") {
                    liveCountryIso = sel.destIso;
                }
                // Persist only dispute-useful fields (no ENV / commission copies).
                if (sel.rateSource === "prefix" || sel.rateSource === "country_max") {
                    liveRateMeta = {
                        pricingRateSource: sel.rateSource,
                        ...(sel.matchedPrefix ? { matchedPrefix: sel.matchedPrefix } : {}),
                        _providerCostUsdPerMin:
                            sel.providerCostUsdPerMin != null ? sel.providerCostUsdPerMin : null,
                    };
                } else if (sel.providerCostUsdPerMin != null) {
                    liveRateMeta = {
                        _providerCostUsdPerMin: sel.providerCostUsdPerMin,
                    };
                }
            }
            // Optional floor (default off, action log) — cost estimate not written to CallLog.
            const floored = applyPriceFloor({
                customerChargeUsd: cost,
                estimatedProviderCostUsd:
                    liveRateMeta?._providerCostUsdPerMin != null
                        ? liveRateMeta._providerCostUsdPerMin * (dur / 60)
                        : null,
                callId,
            });
            cost = floored.charge;
        } catch (liveSelErr) {
            logger.warn("[Credit] Foreign live rate selection failed (kept DID)", {
                error: liveSelErr?.message || String(liveSelErr),
                callId,
            });
        }

        // Wave 1: foreign shadow only — never changes cost / wallet.
        try {
            const { recordForeignPricingShadow } = require("./pricingShadow");
            const telephonyProvider =
                campaign?.numberPolicySnapshot?.provider ||
                campaign?.selectedPhoneProvider ||
                null;
            await recordForeignPricingShadow({
                db,
                callLogDoc,
                campaign,
                user,
                durationSec: dur,
                liveCharge: cost,
                liveRate: effectiveRate,
                liveCountryIso,
                callId,
                provider: telephonyProvider,
                brackets,
                inbound: inboundLog,
            });
        } catch (shadowErr) {
            logger.warn("[Credit] Pricing shadow failed (ignored)", {
                error: shadowErr?.message || String(shadowErr),
                callId,
            });
        }

        const pauseOrgCampaigns = async (reason) => {
            try {
                await pauseActiveCampaignsForInsufficientCredits(db, { user });
            } catch (pauseErr) {
                logger.warn(`[Credit] Failed to pause campaigns after ${reason}`, {
                    error: pauseErr?.message || String(pauseErr),
                    userId: objectIdToString(user._id),
                });
            }
        };

        // Atomic clamp debit: take min(balance, cost), never below 0. Wallets already at/below 0
        // are not matched (left untouched). returnDocument "before" gives the pre-debit balance.
        let unpaid = 0;
        let balanceAfter = roundSix(Number(user.credits) || 0);
        if (cost > 0) {
            const debitRes = await db.collection("users").findOneAndUpdate(
                { _id: user._id, credits: { $gt: 0 } },
                [
                    {
                        $set: {
                            credits: { $max: [0, { $subtract: ["$credits", cost] }] },
                            updatedAt: new Date(),
                        },
                    },
                ],
                { returnDocument: "before" }
            );
            // Driver >=6 returns the doc; older / includeResultMetadata returns { value, ok }.
            const before =
                debitRes &&
                Object.prototype.hasOwnProperty.call(debitRes, "ok") &&
                Object.prototype.hasOwnProperty.call(debitRes, "value")
                    ? debitRes.value
                    : debitRes;
            const balanceBefore = Number(before?.credits) || 0;
            charged = before ? roundSix(Math.min(Math.max(balanceBefore, 0), cost)) : 0;
            unpaid = roundSix(cost - charged);
            balanceAfter = before ? roundSix(Math.max(0, balanceBefore - cost)) : 0;
        }

        if (cost > 0 && charged <= 0) {
            await markCallLogCreditFields({
                creditsDeducted: false,
                creditsDeductedAmount: 0,
                creditDeductionError: "insufficient_credits",
                processedAt: new Date(),
                updatedAt: new Date(),
            });
            await pauseOrgCampaigns("insufficient_credits");
            return { outcome: "insufficient_credits", cost };
        }
        const partial = unpaid > 0;
        const inboundPhone = billingKind === "inbound_config" || !!inboundLog;
        let contactPhone = resolveContactPhoneForCredit(callLogDoc, { inbound: inboundPhone });
        // Foreign Twilio CallLogs often omit to/from; fall back to ContactProcessing.
        if (!contactPhone && contactId && ObjectId.isValid(String(contactId))) {
            try {
                const contactDoc = await db.collection("contactprocessings").findOne(
                    { _id: new ObjectId(String(contactId)) },
                    { projection: { mobileNumber: 1, phone: 1, mobile: 1 } }
                );
                contactPhone = firstPhoneToken(
                    contactDoc?.mobileNumber,
                    contactDoc?.phone,
                    contactDoc?.mobile
                );
            } catch {
                /* ignore */
            }
        }
        await db.collection("credittransactions").insertOne({
            userId: user._id,
            userEmail: user.email,
            type: "call_deduction",
            amount: -charged,
            balanceAfter,
            description:
                billingKind === "inbound_config"
                    ? `Inbound call - ${dur}s (${(dur / 60).toFixed(2)} min)`
                    : `Call usage - ${dur}s (webhook${inboundLog ? ", inbound" : ""})`,
            reference: {
                billingKey,
                fullCost: cost,
                unpaidAmount: unpaid,
                partial,
                campaignId: billingKind === "campaign" ? campaign._id : null,
                inboundConfigId: billingKind === "inbound_config" ? campaign._id : null,
                campaignName: campaign.name || campaign.campaignName,
                callDuration: dur,
                callId,
                contactId: contactId || null,
                contactPhone,
                planRatePerMinute: rates.ratePerMinute,
                kbRatePerMinute: rates.kbRatePerMinute,
                totalRatePerMinute: rates.totalRatePerMinute,
                destinationCountryIso: rates.destinationCountryIso || null,
                countryRateApplied: rates.countryRateApplied === true,
                countryRateSource: rates.countryRateSource || null,
                billingOverrideSource: rates.billingOverrideSource || null,
                ...(liveRateMeta?.pricingRateSource
                    ? { pricingRateSource: liveRateMeta.pricingRateSource }
                    : {}),
                ...(liveRateMeta?.matchedPrefix
                    ? { matchedPrefix: liveRateMeta.matchedPrefix }
                    : {}),
                source:
                    billingKind === "inbound_config"
                        ? "ondial-webhook-inbound-config"
                        : inboundLog
                          ? "ondial-webhook-inbound"
                          : "ondial-webhook",
            },
            createdAt: new Date(),
            updatedAt: new Date(),
        });

        const today = new Date().toISOString().split("T")[0];
        await db.collection("analytics").updateOne(
            { userId: user.email, campaignId: billingId, date: today },
            {
                $inc: {
                    totalMinutes: parseFloat((dur / 60).toFixed(4)),
                    totalCalls: 1,
                    connectedCalls: 1,
                },
                $set: { updatedAt: new Date() },
            },
            { upsert: true }
        );

        if (billingKind === "inbound_config") {
            try {
                await db.collection("inboundconfigs").updateOne(
                    { _id: campaign._id },
                    {
                        $inc: {
                            totalCreditsUsed: charged,
                            ...(partial ? { totalCreditsUnpaid: unpaid } : {}),
                        },
                        $set: { updatedAt: new Date() },
                    }
                );
            } catch (cfgErr) {
                logger.warn("[Credit] inboundconfigs totalCreditsUsed update failed", {
                    error: cfgErr?.message,
                });
            }
        }

        const creditFields = {
            creditsDeducted: true,
            creditsDeductedAmount: charged,
            creditsUnpaidAmount: unpaid,
            creditDeductionError: partial ? "insufficient_credits_partial" : null,
            planRatePerMinute: rates.ratePerMinute,
            kbRatePerMinute: rates.kbRatePerMinute,
            totalRatePerMinute: effectiveRate,
            destinationCountryIso: liveCountryIso || rates.destinationCountryIso || null,
            processedAt: new Date(),
            updatedAt: new Date(),
        };
        if (liveRateMeta?.pricingRateSource) {
            creditFields.pricingRateSource = liveRateMeta.pricingRateSource;
        }
        if (liveRateMeta?.matchedPrefix) {
            creditFields.matchedPrefix = liveRateMeta.matchedPrefix;
        }
        if (billingKind === "campaign") {
            creditFields.campaign_id = billingId;
        } else {
            creditFields.config_id = campaign._id;
        }

        await markCallLogCreditFields(creditFields);

        const deductedMsg = partial
            ? "[Credit] Partially deducted call credits (wallet exhausted)"
            : "[Credit] Deducted call credits";
        logger.info(deductedMsg, {
            callId,
            billingId,
            billingKind,
            contactId,
            durationSec: dur,
            cost,
            charged,
            unpaid,
            balanceAfter,
            billingKey,
            collection: logCollection,
        });

        // Wallet exhausted (partial) or below one minimum-rate minute → stop the org dialling.
        if (cost > 0 && (partial || balanceAfter < LOW_BALANCE_PAUSE_THRESHOLD)) {
            await pauseOrgCampaigns(partial ? "insufficient_credits_partial" : "low_balance");
        }

        // Wave 2: observe-only provider cost / margin (twilio/telnyx). Never changes charge.
        try {
            const { maybeRecordCallEconomics } = require("./callEconomics");
            const telephonyProvider =
                campaign?.numberPolicySnapshot?.provider ||
                campaign?.selectedPhoneProvider ||
                null;
            const destPhone =
                callLogDoc?.to_number ||
                callLogDoc?.phone_number ||
                callLogDoc?.mobileNumber ||
                callLogDoc?.twilio?.To ||
                callLogDoc?.twilio?.Called ||
                null;
            const providerBilled =
                callLogDoc?.telnyx?.billed_duration_secs != null
                    ? Number(callLogDoc.telnyx.billed_duration_secs)
                    : null;
            const actualCost =
                callLogDoc?.telnyx?.total_cost != null
                    ? Number(callLogDoc.telnyx.total_cost)
                    : null;
            await maybeRecordCallEconomics({
                db,
                callId,
                provider: telephonyProvider,
                destinationCountryIso: rates.destinationCountryIso,
                destinationPhone: destPhone,
                talkDurationSec: dur,
                providerBilledSeconds: providerBilled,
                customerChargeUsd: charged,
                actualProviderCostUsd: Number.isFinite(actualCost) ? actualCost : null,
            });
        } catch (econErr) {
            logger.warn("[Credit] Call economics failed (ignored)", {
                error: econErr?.message || String(econErr),
                callId,
            });
        }

        // Tenant CRM webhooks (outbound billed + credits.usage + completed status backup).
        // Inbound billed is handled in Ondial.
        await notifyOutboundTenantBilledBestEffort({
            userId: user._id,
            contactId: contactId || null,
            campaignId: campaign?._id || billingId,
            callId,
            balanceAfter,
            creditsCharged: charged,
            inboundLog,
            billingKind,
            alsoStatusCompleted: true,
        });

        if (partial) {
            return { outcome: "partial_insufficient", cost: charged, unpaid };
        }
        return { outcome: "deducted", cost: charged };
    } catch (err) {
        // Hangup + conversation can race; unique billingKey means the other path already charged.
        if (err?.code === 11000 || String(err?.message || "").includes("E11000")) {
            // E11000: refund the userId on the EXISTING winning transaction (Wave 5), not this path's user.
            try {
                const { refundExistingBillingKeyWinner } = require("./resolveBillingUser");
                // Refund only what THIS attempt debited (partial charges take < cost).
                await refundExistingBillingKeyWinner(db, { billingKey, cost: charged, callId });
            } catch (refundErr) {
                logger.error("[Credit] CRITICAL — failed to refund credits after E11000", {
                    callId,
                    billingKey,
                    amount: charged,
                    error: refundErr?.message,
                });
            }
            await markCallLogCreditFields({
                creditsDeducted: true,
                creditDeductionError: null,
                updatedAt: new Date(),
            });
            logger.info("[Credit] Already billed (duplicate billingKey)", { callId, billingKey });
            // Still fan-out tenant CRM billed webhooks (idempotent on Ondial).
            try {
                let ownerId = explicitUserId || callLogDoc?.userId || campaign?.userId || null;
                if (!ownerId && campaign?.createdBy) {
                    const u = await db.collection("users").findOne({ email: campaign.createdBy });
                    ownerId = u?._id || null;
                }
                await notifyOutboundTenantBilledBestEffort({
                    userId: ownerId,
                    contactId: contactId || callLogDoc?.contact_id || null,
                    campaignId: billingKind === "campaign" ? billingId : null,
                    callId,
                    balanceAfter: null,
                    creditsCharged: callLogDoc?.creditsDeductedAmount ?? null,
                    inboundLog,
                    billingKind,
                    alsoStatusCompleted: true,
                });
            } catch {
                /* ignore */
            }
            return { outcome: "already_billed" };
        }
        await markCallLogCreditFields({
            creditsDeducted: false,
            creditDeductionError: String(err.message || err),
            updatedAt: new Date(),
        });
        logger.error("[Credit] Deduction failed", { callId, error: err.message });
        return { outcome: "error", error: err.message };
    }
}

module.exports = { tryDeductCampaignCallCredits, pickRateOverride };
