#!/usr/bin/env node
/**
 * Wave 2 §5.6 — Twilio zero-duration / missed-bill recon (REPORT ONLY).
 * Finds CallLogs where provider=twilio, duration>0, and
 *   creditsDeducted != true OR call_economics missing.
 * Never auto-charges.
 *
 * Usage: node scripts/twilio-billing-recon-report.js [--days=7] [--limit=200]
 */
const { MongoClient } = require("mongodb");

function argNum(name, def) {
    const a = process.argv.find((x) => x.startsWith(`--${name}=`));
    if (!a) return def;
    const n = Number(a.split("=")[1]);
    return Number.isFinite(n) ? n : def;
}

async function main() {
    const uri = process.env.MONGODB_URI;
    if (!uri) {
        console.error("MONGODB_URI required");
        process.exit(1);
    }
    const days = argNum("days", 7);
    const limit = argNum("limit", 200);
    const since = new Date(Date.now() - days * 86400000);

    const client = new MongoClient(uri);
    await client.connect();
    const db = client.db();
    const collName = process.env.CALLLOGS_COLLECTION || "CallLogs";

    const candidates = await db
        .collection(collName)
        .find({
            createdAt: { $gte: since },
            duration: { $gt: 0 },
            $and: [
                {
                    $or: [{ provider: "twilio" }, { "twilio.call_sid": { $exists: true } }],
                },
                {
                    $or: [{ creditsDeducted: { $ne: true } }, { creditsDeducted: { $exists: false } }],
                },
            ],
        })
        .project({
            call_id: 1,
            call_unique_id: 1,
            "twilio.call_sid": 1,
            duration: 1,
            creditsDeducted: 1,
            creditsDeductedAmount: 1,
            campaign_id: 1,
            createdAt: 1,
        })
        .limit(limit)
        .toArray();

    const missingEconomics = [];
    for (const row of candidates) {
        const callId = String(row.call_unique_id || row.call_id || row.twilio?.call_sid || "").trim();
        if (!callId) continue;
        const econ = await db.collection("call_economics").findOne({ callId });
        if (!econ) missingEconomics.push({ callId, duration: row.duration, creditsDeducted: row.creditsDeducted });
    }

    console.log(
        JSON.stringify(
            {
                reportOnly: true,
                since: since.toISOString(),
                candidatesWithDurationNotBilled: candidates.length,
                missingEconomics: missingEconomics.length,
                sample: candidates.slice(0, 20).map((r) => ({
                    callId: r.call_unique_id || r.call_id || r.twilio?.call_sid,
                    duration: r.duration,
                    creditsDeducted: r.creditsDeducted,
                    campaign_id: r.campaign_id,
                })),
            },
            null,
            2
        )
    );
    await client.close();
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
