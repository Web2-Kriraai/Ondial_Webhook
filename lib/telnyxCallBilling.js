/**
 * Telnyx outbound credit billing (mirrors Twilio path; rates use campaign number provider).
 */
const { ObjectId } = require("mongodb");
const { getDb } = require("../db");
const logger = require("../logger");
const { pickNonEmpty } = require("./customParameters");
const { tryDeductCampaignCallCredits } = require("./campaignCreditDeduction");
const { resolveCampaignIdFromContact } = require("./resolveCampaignId");
const {
    shouldSkipCreditDeduction,
    creditTestBypassEnabled,
} = require("./shouldSkipCreditDeduction");
const {
    resolveDialerCallId,
    buildBillingCallLogFilter,
} = require("./resolveBillingCallId");

function isMongoObjectIdString(s) {
    return typeof s === "string" && /^[a-fA-F0-9]{24}$/.test(s);
}

async function loadTelnyxCallLogDoc(callControlId, collectionName) {
    const db = getDb();
    const CALLLOGS_COLLECTION = process.env.CALLLOGS_COLLECTION || "CallLogs";
    const coll = collectionName || CALLLOGS_COLLECTION;
    return db.collection(coll).findOne(
        { "telnyx.call_control_id": callControlId },
        {
            projection: {
                campaign_id: 1,
                contact_id: 1,
                creditsDeducted: 1,
                isTestCall: 1,
                call_id: 1,
                call_unique_id: 1,
                lead_id: 1,
                "telnyx.duration": 1,
                "telnyx.status": 1,
                call_data: 1,
            },
        }
    );
}

async function isWizardTestCallLinked({ callControlId, dialerCallId }) {
    const id = String(callControlId || "").trim();
    const dialerId = String(dialerCallId || "").trim();
    if (!id && !dialerId) return false;
    const db = getDb();
    const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";
    const or = [];
    if (id) {
        or.push({ call_id: id }, { provider_call_id: id }, { lead_id: `telnyx:${id}` });
    }
    // Wizard rows are written by the dialer, so they carry the dialer id, not the carrier id.
    if (dialerId) {
        or.push(
            { call_id: dialerId },
            { lead_id: dialerId },
            { call_unique_id: dialerId }
        );
    }
    const row = await db.collection(TESTCALL_COLLECTION).findOne(
        { $or: or },
        { projection: { _id: 1 } }
    );
    return !!row;
}

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

async function resolveTelnyxBillingIds({ callControlId, telnyxMapping, body, collectionName }) {
    const doc = await loadTelnyxCallLogDoc(callControlId, collectionName);
    const contactId = pickNonEmpty(
        telnyxMapping?.contact_id,
        body?.contact_id,
        doc?.contact_id
    );
    let campaignId = pickNonEmpty(
        telnyxMapping?.campaign_id,
        body?.campaign_id,
        doc?.campaign_id
    );
    if (!campaignId && contactId) {
        campaignId = await resolveCampaignIdFromContact(contactId);
    }
    return {
        doc,
        contactId: contactId ? String(contactId).trim() : "",
        campaignId: campaignId ? String(campaignId).trim() : "",
        dialerCallId: resolveDialerCallId({
            carrierCallId: callControlId,
            mapping: telnyxMapping,
            body,
            doc,
        }),
    };
}

/**
 * Deduct campaign credits for a completed Telnyx call.
 * resolvePlanRate uses campaign number provider (telnyx overlay) inside tryDeductCampaignCallCredits.
 */
async function maybeDeductTelnyxCallCredits({
    callControlId,
    durationSec,
    telnyxMapping,
    body,
    collectionName,
}) {
    const id = String(callControlId || "").trim();
    const dur = Math.max(0, Math.floor(Number(durationSec) || 0));
    if (!id) return { outcome: "skip_no_call_control_id" };
    if (dur <= 0) return { outcome: "skip_no_duration" };

    const { doc, contactId, campaignId, dialerCallId } = await resolveTelnyxBillingIds({
        callControlId: id,
        telnyxMapping,
        body,
        collectionName,
    });

    if (doc?.creditsDeducted === true) {
        return { outcome: "already_billed" };
    }
    if (creditTestBypassEnabled()) {
        const wizardTest =
            (await isWizardTestCallLinked({ callControlId: id, dialerCallId })) ||
            (await isWizardTestContactId(contactId)) ||
            shouldSkipCreditDeduction({
                callLogDoc: doc,
                body,
                mapping: telnyxMapping,
                collectionName,
            });
        if (wizardTest) {
            logger.info("[Telnyx] Skipping credit deduction — test call", {
                call_control_id: id,
                campaign_id: campaignId || null,
                contact_id: contactId || null,
            });
            return { outcome: "skip_test_call" };
        }
    }
    if (!campaignId) {
        logger.warn("[Telnyx] Skipping credit deduction — campaign_id unresolved", {
            call_control_id: id,
            contact_id: contactId || null,
        });
        return { outcome: "no_campaign_id" };
    }

    const creditResult = await tryDeductCampaignCallCredits({
        callUniqueId: dialerCallId || id,
        contactId: contactId || null,
        campaignId,
        durationSec: dur,
        callLogCollectionName: collectionName,
        callLogLookupFilter: buildBillingCallLogFilter({
            carrierField: "telnyx.call_control_id",
            carrierCallId: id,
            dialerCallId,
        }),
    });

    logger.info("[Telnyx] Credit deduction", {
        call_control_id: id,
        billing_call_id: dialerCallId || id,
        contact_id: contactId || null,
        campaign_id: campaignId,
        durationSec: dur,
        ...creditResult,
    });

    return creditResult;
}

module.exports = {
    maybeDeductTelnyxCallCredits,
    resolveTelnyxBillingIds,
};
