/**
 * Twilio outbound credit billing (same rules as India hangup via campaignCreditDeduction).
 */
const { ObjectId } = require("mongodb");
const { getDb } = require("../db");
const logger = require("../logger");
const { pickNonEmpty } = require("./customParameters");
const { tryDeductCampaignCallCredits } = require("./campaignCreditDeduction");
const { resolveCampaignIdFromContact } = require("./resolveCampaignId");
const { shouldSkipCreditDeduction } = require("./shouldSkipCreditDeduction");

function isMongoObjectIdString(s) {
    return typeof s === "string" && /^[a-fA-F0-9]{24}$/.test(s);
}

async function loadTwilioCallLogDoc(callSid, collectionName) {
    const db = getDb();
    const CALLLOGS_COLLECTION = process.env.CALLLOGS_COLLECTION || "CallLogs";
    const coll = collectionName || CALLLOGS_COLLECTION;
    return db.collection(coll).findOne(
        { "twilio.call_sid": callSid },
        {
            projection: {
                campaign_id: 1,
                contact_id: 1,
                creditsDeducted: 1,
                isTestCall: 1,
                "twilio.duration": 1,
                "twilio.status": 1,
                call_data: 1,
            },
        }
    );
}

/** Wizard Step 6 TestCall row for this exact Twilio CallSid only (not stale campaign+contact). */
async function isWizardTestCallLinked({ callSid }) {
    const sid = String(callSid || "").trim();
    if (!sid) return false;
    const db = getDb();
    const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";
    const row = await db.collection(TESTCALL_COLLECTION).findOne(
        {
            $or: [
                { call_id: sid },
                { provider_call_id: sid },
                { lead_id: `twilio:${sid}` },
            ],
        },
        { projection: { _id: 1 } }
    );
    return !!row;
}

/** Campaign wizard uses a dedicated contact row marked _wizardTestContact. */
async function isWizardTestContactId(contactId) {
    if (!isMongoObjectIdString(contactId)) return false;
    try {
        const db = getDb();
        const doc = await db.collection("contactprocessings").findOne(
            { _id: new ObjectId(String(contactId)) },
            { projection: { "contactData._wizardTestContact": 1 } }
        );
        return doc?.contactData?._wizardTestContact === true;
    } catch {
        return false;
    }
}

async function resolveTwilioBillingIds({ callSid, twilioMapping, body, collectionName }) {
    const doc = await loadTwilioCallLogDoc(callSid, collectionName);
    const contactId = pickNonEmpty(
        twilioMapping?.contact_id,
        body?.contact_id,
        doc?.contact_id
    );
    let campaignId = pickNonEmpty(
        twilioMapping?.campaign_id,
        body?.campaign_id,
        doc?.campaign_id
    );
    if (!campaignId && contactId) {
        campaignId = await resolveCampaignIdFromContact(contactId);
    }
    return { doc, contactId: contactId ? String(contactId).trim() : "", campaignId: campaignId ? String(campaignId).trim() : "" };
}

/**
 * Deduct campaign credits for a completed Twilio call (idempotent with worker/webhook India path).
 * @param {object} opts
 * @param {string} opts.callSid - Twilio CallSid (CallLogs.call_id)
 * @param {number} opts.durationSec - billable seconds
 * @param {object} [opts.twilioMapping]
 * @param {object} [opts.body] - webhook body fallbacks
 * @param {string} [opts.collectionName]
 */
async function maybeDeductTwilioCallCredits({ callSid, durationSec, twilioMapping, body, collectionName }) {
    const sid = String(callSid || "").trim();
    const dur = Math.max(0, Math.floor(Number(durationSec) || 0));
    if (!sid) return { outcome: "skip_no_call_sid" };
    if (dur <= 0) return { outcome: "skip_no_duration" };

    const { doc, contactId, campaignId } = await resolveTwilioBillingIds({
        callSid: sid,
        twilioMapping,
        body,
        collectionName,
    });

    if (doc?.creditsDeducted === true) {
        return { outcome: "already_billed" };
    }
    const wizardTest =
        (await isWizardTestCallLinked({ callSid: sid })) ||
        (await isWizardTestContactId(contactId)) ||
        shouldSkipCreditDeduction({
            callLogDoc: doc,
            body,
            mapping: twilioMapping,
            collectionName,
        });
    if (wizardTest) {
        logger.info("[Twilio] Skipping credit deduction — test call", {
            CallSid: sid,
            campaign_id: campaignId || null,
            contact_id: contactId || null,
        });
        return { outcome: "skip_test_call" };
    }
    if (!campaignId) {
        logger.warn("[Twilio] Skipping credit deduction — campaign_id unresolved", {
            CallSid: sid,
            contact_id: contactId || null,
        });
        return { outcome: "no_campaign_id" };
    }

    const creditResult = await tryDeductCampaignCallCredits({
        callUniqueId: sid,
        contactId: contactId || null,
        campaignId,
        durationSec: dur,
    });

    logger.info("[Twilio] Credit deduction", {
        CallSid: sid,
        contact_id: contactId || null,
        campaign_id: campaignId,
        durationSec: dur,
        ...creditResult,
    });

    return creditResult;
}

module.exports = {
    maybeDeductTwilioCallCredits,
    resolveTwilioBillingIds,
};
