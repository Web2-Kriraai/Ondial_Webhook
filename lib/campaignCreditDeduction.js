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
    applyCountryRateToPackageRate,
    loadCountryCallingRates,
} = require("./countryCallingRates");
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

    if (!billingOverrideApplied) {
        let currentTier = user?.creditPlan?.currentTier || "A";
        const tierToId = { A: "starter", B: "professional", C: "enterprise", D: "premium" };
        const pkg = await db
            .collection("creditpackages")
            .findOne({ packageId: tierToId[currentTier] || "starter" });
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

    if (!billingOverrideApplied) {
        try {
            const billingPhone = resolveCountryBillingPhone(callLogDoc, { inbound });
            const defaultCountry = resolveDefaultCountryIso(callLogDoc, campaign, { inbound });
            destinationCountryIso = countryIsoFromPhone(billingPhone, defaultCountry);
            const countryConfig = await loadCountryCallingRates(db);
            const countryApplied = applyCountryRateToPackageRate(
                ratePerMinute,
                countryConfig,
                destinationCountryIso,
                voiceTier
            );
            ratePerMinute = countryApplied.ratePerMinute;
            destinationCountryIso = countryApplied.destinationCountryIso;
            countryRateApplied = countryApplied.countryRateApplied === true;
            countryRateSource = countryApplied.countryRateSource || null;
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
    if (!campId && contactId) {
        const { resolveCampaignIdFromContact } = require("./resolveCampaignId");
        campId = (await resolveCampaignIdFromContact(contactId)) || "";
    }
    if (!campId || !ObjectId.isValid(campId)) {
        logger.warn("[Credit] Missing campaign_id — cannot deduct", { callId, contactId });
        return { outcome: "no_campaign_id" };
    }

    const db = getDb();
    const billingKey = `${campId}:call:${callId}`;
    const logCollection = callLogCollectionName || CALLLOGS_COLLECTION;
    const inboundLog = logCollection === INBOUNDCALLLOG_COLLECTION;

    const existingTx = await db.collection("credittransactions").findOne({
        type: "call_deduction",
        "reference.billingKey": billingKey,
    });
    if (existingTx) return { outcome: "already_billed" };

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
            await db.collection(logCollection).updateMany({ call_sid: sid }, { $set: fields });
            return;
        }
        await db.collection(logCollection).updateOne(callLogFilter, { $set: fields });
    };

    try {
        const campaign = await db.collection("campaigns").findOne({ _id: new ObjectId(campId) });
        if (!campaign) throw new Error("campaign_not_found");

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
        if (!user) {
            user = await db.collection("users").findOne({ email: campaign.createdBy });
        }
        if (!user && campaign.userId) {
            try {
                user = await db.collection("users").findOne({ _id: new ObjectId(String(campaign.userId)) });
            } catch {
                /* ignore */
            }
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
            description: `Call usage - ${dur}s (webhook${inboundLog ? ", inbound" : ""})`,
            reference: {
                billingKey,
                campaignId: campaign._id,
                campaignName: campaign.name || campaign.campaignName,
                callDuration: dur,
                callId,
                contactId: contactId || null,
                planRatePerMinute: rates.ratePerMinute,
                kbRatePerMinute: rates.kbRatePerMinute,
                totalRatePerMinute: rates.totalRatePerMinute,
                destinationCountryIso: rates.destinationCountryIso || null,
                countryRateApplied: rates.countryRateApplied === true,
                countryRateSource: rates.countryRateSource || null,
                source: inboundLog ? "ondial-webhook-inbound" : "ondial-webhook",
            },
            createdAt: new Date(),
            updatedAt: new Date(),
        });

        const today = new Date().toISOString().split("T")[0];
        await db.collection("analytics").updateOne(
            { userId: user.email, campaignId: String(campaign._id), date: today },
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

        await markCallLogCreditFields({
            creditsDeducted: true,
            creditsDeductedAmount: cost,
            creditDeductionError: null,
            campaign_id: String(campId),
            planRatePerMinute: rates.ratePerMinute,
            kbRatePerMinute: rates.kbRatePerMinute,
            totalRatePerMinute: rates.totalRatePerMinute,
            processedAt: new Date(),
            updatedAt: new Date(),
        });

        logger.info("[Credit] Deducted campaign call credits", {
            callId,
            campaignId: campId,
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
