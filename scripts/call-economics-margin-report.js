#!/usr/bin/env node
/**
 * Wave 2 — Super-Admin-style margin report (read-only).
 * Aggregation of call_economics by provider x destinationCountryIso.
 *
 * Usage: node scripts/call-economics-margin-report.js [--days=30]
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
    const days = argNum("days", 30);
    const since = new Date(Date.now() - days * 86400000);
    const client = new MongoClient(uri);
    await client.connect();
    const db = client.db();

    const byBucket = await db
        .collection("call_economics")
        .aggregate([
            { $match: { createdAt: { $gte: since } } },
            {
                $group: {
                    _id: {
                        provider: "$provider",
                        destIso: "$destinationCountryIso",
                    },
                    calls: { $sum: 1 },
                    revenue: { $sum: "$customerChargeUsd" },
                    costEst: { $sum: "$estimatedProviderCostUsd" },
                    costActual: { $sum: { $ifNull: ["$actualProviderCostUsd", 0] } },
                    margin: { $sum: "$marginUsd" },
                },
            },
            { $sort: { margin: 1 } },
        ])
        .toArray();

    const negative = await db
        .collection("call_economics")
        .find({ createdAt: { $gte: since }, marginUsd: { $lt: 0 } })
        .project({ callId: 1, provider: 1, destinationCountryIso: 1, marginUsd: 1, customerChargeUsd: 1 })
        .limit(100)
        .toArray();

    console.log(JSON.stringify({ since: since.toISOString(), byBucket, negativeMargins: negative }, null, 2));
    await client.close();
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
