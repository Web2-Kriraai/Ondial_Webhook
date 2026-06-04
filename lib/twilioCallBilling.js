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

/**
 * Sync contactprocessings.callReceiveStatus from Twilio CallStatus (2=in-progress, 3=completed, 1=busy).
 */
async function syncTwilioContactReceiveStatus(contactIdRaw, statusRaw) {
    const contactId = contactIdRaw != null ? String(contactIdRaw).trim() : "";
    if (!contactId || !isMongoObjectIdString(contactId)) return;

    const status = String(statusRaw || "").trim().toLowerCase();
    let callReceiveStatus = null;
    if (status === "in-progress") callReceiveStatus = 2;
    else if (status === "completed") callReceiveStatus = 3;
    else if (status === "busy") callReceiveStatus = 1;
    if (callReceiveStatus == null) return;

    try {
        const db = getDb();
        const result = await db.collection("contactprocessings").updateOne(
            { _id: new ObjectId(contactId) },
            { $set: { callReceiveStatus, updatedAt: new Date() } }
        );
        if (result.matchedCount === 0) {
            logger.warn("[Twilio] contact_id not found for status sync", {
                contact_id: contactId,
                callReceiveStatus,
                twilioStatus: statusRaw,
            });
            return;
        }
        logger.info("[Twilio] contactprocessings status synced", {
            contact_id: contactId,
            callReceiveStatus,
            twilioStatus: statusRaw,
        });
    } catch (err) {
        logger.error("[Twilio] Failed to sync contactprocessings status", {
            contact_id: contactId,
            twilioStatus: statusRaw,
            error: err.message,
        });
    }
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
            },
        }
    );
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
    if (
        shouldSkipCreditDeduction({
            callLogDoc: doc,
            body,
            mapping: twilioMapping,
            collectionName,
        })
    ) {
        logger.info("[Twilio] Skipping credit deduction — test call", { CallSid: sid });
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
    syncTwilioContactReceiveStatus,
    maybeDeductTwilioCallCredits,
    resolveTwilioBillingIds,
};
