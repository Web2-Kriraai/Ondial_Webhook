/**
 * Campaign outbound credit deduction on call_hangup (CallLogs).
 * Mirrors Calling_system1 worker bracket billing; idempotent via reference.billingKey.
 */
const { ObjectId } = require("mongodb");
const { getDb } = require("../db");
const { objectIdToString } = require("./mongoObjectId");
const logger = require("../logger");
const { shouldSkipCreditDeduction } = require("./shouldSkipCreditDeduction");
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
    return { $or: [{ lead_id: id }, { call_id: id }] };
}

const FALLBACK_BRACKETS = [
    { fromSecond: 1, toSecond: 15, percentOfRatePerMinute: 25 },
    { fromSecond: 16, toSecond: 30, percentOfRatePerMinute: 50 },
    { fromSecond: 31, toSecond: 45, percentOfRatePerMinute: 75 },
    { fromSecond: 46, toSecond: 60, percentOfRatePerMinute: 100 },
];

function roundSix(n) {
    return parseFloat(Number(n).toFixed(6));
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

async function resolveRatePerMinute(db, user, campaign, callLogDoc = null, options = {}) {
    const inbound = options.inbound === true;
    const voiceTier = campaign?.selectedVoice?.tier || "standard";
    let ratePerMinute = 0.08;
    let billingOverrideApplied = false;
    let destinationCountryIso = null;
    let countryRateApplied = false;
    let countryRateSource = null;

    const override = user?.billingOverride;
    if (override?.enabled === true) {
        let overrideRate = null;
        if (typeof override.flatRatePerMinute === "number" && override.flatRatePerMinute >= 0) {
            overrideRate = override.flatRatePerMinute;
        } else {
            const map = {
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

    const kbRate = kbRatePerMinuteFromCampaign(campaign);
    return {
        ratePerMinute: roundSix(ratePerMinute),
        kbRatePerMinute: kbRate,
        totalRatePerMinute: roundSix(ratePerMinute + kbRate),
        billingOverrideApplied,
        destinationCountryIso,
        countryRateApplied,
        countryRateSource,
        inbound,
    };
}

/**
 * @returns {Promise<{ outcome: string, cost?: number }>}
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
    if (existingTx) return { outcome: "already_billed" };

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
        if (legacyTx) return { outcome: "already_billed" };
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
    if (callLogDoc.creditsDeducted === true) return { outcome: "already_billed" };
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

    try {
        let user = null;
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
        if (!user) throw new Error("user_not_found");

        let brackets = FALLBACK_BRACKETS;
        const bracketSetting = await db.collection("systemsettings").findOne({ key: "callBillingBracketsV1" });
        if (bracketSetting?.value && Array.isArray(bracketSetting.value)) {
            brackets = bracketSetting.value;
        }

        const rates = await resolveRatePerMinute(db, user, campaign, callLogDoc, { inbound: inboundLog });
        const cost = computeCost(dur, rates.totalRatePerMinute, brackets);

        if ((user.credits || 0) < cost) {
            await markCallLogCreditFields({
                creditsDeducted: false,
                creditsDeductedAmount: 0,
                creditDeductionError: "insufficient_credits",
                processedAt: new Date(),
                updatedAt: new Date(),
            });
            return { outcome: "insufficient_credits", cost };
        }

        const deduct = await db.collection("users").updateOne(
            { _id: user._id, credits: { $gte: cost } },
            { $inc: { credits: -cost }, $set: { updatedAt: new Date() } }
        );
        if (deduct.modifiedCount === 0 && cost > 0) {
            throw new Error("credit_deduction_race");
        }

        const balanceAfter = roundSix((user.credits || 0) - cost);
        await db.collection("credittransactions").insertOne({
            userId: user._id,
            userEmail: user.email,
            type: "call_deduction",
            amount: -cost,
            balanceAfter,
            description:
                billingKind === "inbound_config"
                    ? `Inbound call - ${dur}s (${(dur / 60).toFixed(2)} min)`
                    : `Call usage - ${dur}s (webhook${inboundLog ? ", inbound" : ""})`,
            reference: {
                billingKey,
                campaignId: billingKind === "campaign" ? campaign._id : null,
                inboundConfigId: billingKind === "inbound_config" ? campaign._id : null,
                campaignName: campaign.name || campaign.campaignName,
                callDuration: dur,
                callId,
                contactId: contactId || null,
                contactPhone: callLogDoc?.from_number || callLogDoc?.caller_number || null,
                planRatePerMinute: rates.ratePerMinute,
                kbRatePerMinute: rates.kbRatePerMinute,
                totalRatePerMinute: rates.totalRatePerMinute,
                destinationCountryIso: rates.destinationCountryIso || null,
                countryRateApplied: rates.countryRateApplied === true,
                countryRateSource: rates.countryRateSource || null,
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
                    { $inc: { totalCreditsUsed: cost }, $set: { updatedAt: new Date() } }
                );
            } catch (cfgErr) {
                logger.warn("[Credit] inboundconfigs totalCreditsUsed update failed", {
                    error: cfgErr?.message,
                });
            }
        }

        const creditFields = {
            creditsDeducted: true,
            creditsDeductedAmount: cost,
            creditDeductionError: null,
            planRatePerMinute: rates.ratePerMinute,
            kbRatePerMinute: rates.kbRatePerMinute,
            totalRatePerMinute: rates.totalRatePerMinute,
            processedAt: new Date(),
            updatedAt: new Date(),
        };
        if (billingKind === "campaign") {
            creditFields.campaign_id = billingId;
        } else {
            creditFields.config_id = campaign._id;
        }

        await markCallLogCreditFields(creditFields);

        logger.info("[Credit] Deducted call credits", {
            callId,
            billingId,
            billingKind,
            contactId,
            durationSec: dur,
            cost,
            billingKey,
            collection: logCollection,
        });
        return { outcome: "deducted", cost };
    } catch (err) {
        await markCallLogCreditFields({
            creditsDeducted: false,
            creditDeductionError: String(err.message || err),
            updatedAt: new Date(),
        });
        logger.error("[Credit] Deduction failed", { callId, error: err.message });
        return { outcome: "error", error: err.message };
    }
}

module.exports = { tryDeductCampaignCallCredits };
