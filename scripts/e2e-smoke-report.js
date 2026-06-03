require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });
const { MongoClient } = require("mongodb");

const LIMIT = Math.max(1, Number.parseInt(process.argv[2] || "20", 10) || 20);
const CAMPAIGN_ID = (process.argv[3] || "").trim();

function readEvents(doc) {
    return Array.isArray(doc?.call_data?.events) ? doc.call_data.events : [];
}

function hasEvent(doc, eventType) {
    return readEvents(doc).some((e) => String(e?.event_type || "").toLowerCase() === eventType);
}

function latestEvent(doc, eventType) {
    const events = readEvents(doc).filter(
        (e) => String(e?.event_type || "").toLowerCase() === eventType
    );
    return events.length ? events[events.length - 1] : null;
}

function durationFromHangup(doc) {
    const h = latestEvent(doc, "call_hangup");
    const d = h?.data?.duration ?? h?.data?.durationSec ?? h?.data?._raw?.call?.durationSec;
    return Number.isFinite(Number(d)) ? Number(d) : 0;
}

function callStatusFromHangup(doc) {
    const h = latestEvent(doc, "call_hangup");
    return String(h?.data?.callStatus || h?.data?._raw?.call?.callStatus || "").toUpperCase();
}

(async () => {
    const client = new MongoClient(process.env.MONGODB_URI);
    await client.connect();
    const db = client.db();

    const filter = CAMPAIGN_ID ? { campaign_id: CAMPAIGN_ID } : {};
    const docs = await db
        .collection("CallLogs")
        .find(filter)
        .sort({ createdAt: -1 })
        .limit(LIMIT)
        .project({
            call_id: 1,
            lead_id: 1,
            campaign_id: 1,
            contact_id: 1,
            createdAt: 1,
            updatedAt: 1,
            creditsDeducted: 1,
            creditsDeductedAmount: 1,
            creditDeductionError: 1,
            isTestCall: 1,
            call_data: 1,
        })
        .toArray();

    const report = [];
    let passCount = 0;

    for (const d of docs) {
        const hasHangup = hasEvent(d, "call_hangup");
        const hasAnswered = hasEvent(d, "call_answered");
        const dur = durationFromHangup(d);
        const status = callStatusFromHangup(d);
        const campaignResolved = !!String(d?.campaign_id || "").trim();
        const expectedCharge = hasHangup && (status === "ANSWERED" || dur > 0) && campaignResolved;
        const charged = d?.creditsDeducted === true && Number(d?.creditsDeductedAmount || 0) > 0;

        const checks = {
            campaignResolved,
            hasHangup,
            hasAnsweredOrDuration: hasAnswered || dur > 0,
            expectedCharge,
            chargedWhenExpected: expectedCharge ? charged : true,
            noCreditError: !d?.creditDeductionError,
        };

        const ok =
            checks.campaignResolved &&
            checks.hasHangup &&
            checks.chargedWhenExpected &&
            checks.noCreditError;
        if (ok) passCount += 1;

        report.push({
            call_id: d.call_id || d.lead_id,
            campaign_id: d.campaign_id || null,
            contact_id: d.contact_id || null,
            isTestCall: d.isTestCall === true,
            durationSec: dur,
            callStatus: status || null,
            creditsDeducted: d.creditsDeducted === true,
            creditsDeductedAmount: Number(d.creditsDeductedAmount || 0),
            creditDeductionError: d.creditDeductionError || null,
            checks,
            ok,
            createdAt: d.createdAt || null,
        });
    }

    console.log(
        JSON.stringify(
            {
                scope: CAMPAIGN_ID ? { campaign_id: CAMPAIGN_ID } : { campaign_id: "ALL" },
                total: docs.length,
                passed: passCount,
                failed: docs.length - passCount,
                rows: report,
            },
            null,
            2
        )
    );

    await client.close();
})().catch((e) => {
    console.error(e);
    process.exit(1);
});

