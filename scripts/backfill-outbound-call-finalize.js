/**
 * Backfill one outbound CallLogs row that has hangup+conversation but missing
 * root duration/status (and optionally re-attempt credit).
 *
 * Usage:
 *   node scripts/backfill-outbound-call-finalize.js 83cf5f1f-3b88-4794-9ba0-46993977e31c
 *   node scripts/backfill-outbound-call-finalize.js 83cf5f1f-... --credit
 */
require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });
const { connectDB, getDb } = require("../db");
const { finalizeOutboundCallLog } = require("../lib/finalizeOutboundCallLog");
const { tryDeductCampaignCallCredits } = require("../lib/campaignCreditDeduction");

const CALLLOGS = process.env.CALLLOGS_COLLECTION || "CallLogs";

function hangupFromDoc(doc) {
    const events = doc?.call_data?.events || [];
    const hangup = [...events].reverse().find((e) => e?.event_type === "call_hangup");
    const data = hangup?.data || {};
    const duration = Math.max(
        0,
        Math.floor(Number(data.duration ?? data.durationSec ?? doc.duration) || 0)
    );
    const recordingUrl =
        hangup?.recordingUrl ||
        data.recordingUrl ||
        data._raw?.call?.recordingUrl ||
        doc.recordingUrl ||
        null;
    const callStatus = data.callStatus || (duration > 0 ? "ANSWERED" : null);
    return { duration, recordingUrl, callStatus };
}

async function main() {
    const callId = String(process.argv[2] || "").trim();
    const doCredit = process.argv.includes("--credit");
    if (!callId) {
        console.error(
            "Usage: node scripts/backfill-outbound-call-finalize.js <call_unique_id> [--credit]"
        );
        process.exit(1);
    }

    await connectDB();
    const db = getDb();
    const doc = await db.collection(CALLLOGS).findOne({
        $or: [{ lead_id: callId }, { call_id: callId }, { call_unique_id: callId }],
    });
    if (!doc) {
        console.error("CallLogs doc not found for", callId);
        process.exit(1);
    }

    const { duration, recordingUrl, callStatus } = hangupFromDoc(doc);
    console.log("Before:", {
        _id: String(doc._id),
        duration: doc.duration ?? null,
        status: doc.status ?? null,
        creditsDeducted: doc.creditsDeducted ?? null,
        recordingUrl: doc.recordingUrl ? "(set)" : null,
        contact_id: doc.contact_id || null,
        campaign_id: doc.campaign_id || null,
        hangup_duration: duration,
    });

    await finalizeOutboundCallLog({
        callUniqueId: callId,
        durationSec: duration,
        recordingUrl,
        callStatus,
    });

    let creditResult = null;
    if (doCredit && duration > 0 && doc.campaign_id) {
        creditResult = await tryDeductCampaignCallCredits({
            callUniqueId: callId,
            contactId: doc.contact_id,
            campaignId: String(doc.campaign_id),
            durationSec: duration,
        });
        console.log("Credit:", creditResult);
    }

    const after = await db.collection(CALLLOGS).findOne(
        { _id: doc._id },
        {
            projection: {
                duration: 1,
                duration_ms: 1,
                status: 1,
                recordingUrl: 1,
                creditsDeducted: 1,
                creditsDeductedAmount: 1,
                creditDeductionError: 1,
                to_number: 1,
                from_number: 1,
                conversation: 1,
            },
        }
    );
    console.log("After:", {
        duration: after?.duration ?? null,
        duration_ms: after?.duration_ms ?? null,
        status: after?.status ?? null,
        recordingUrl: after?.recordingUrl ? "(set)" : null,
        creditsDeducted: after?.creditsDeducted ?? null,
        creditsDeductedAmount: after?.creditsDeductedAmount ?? null,
        creditDeductionError: after?.creditDeductionError ?? null,
        has_conversation: Boolean(after?.conversation?.turns?.length),
    });
}

main().catch((err) => {
    console.error(err.message);
    process.exit(1);
});
