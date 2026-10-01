/**
 * Read-only aggregation over pricing_shadow_log.
 * Usage: node --env-file=.env scripts/shadow_report.js
 * Never writes customer credits.
 */
const { MongoClient } = require("mongodb");

async function main() {
    const uri = process.env.MONGODB_URI;
    if (!uri) {
        console.error("MONGODB_URI required");
        process.exit(1);
    }
    const sinceDays = Math.max(1, Number(process.env.SHADOW_REPORT_DAYS || 30) || 30);
    const since = new Date(Date.now() - sinceDays * 24 * 3600 * 1000);
    const client = new MongoClient(uri);
    await client.connect();
    const db = client.db();
    const rows = await db
        .collection("pricing_shadow_log")
        .aggregate([
            { $match: { createdAt: { $gte: since } } },
            {
                $group: {
                    _id: { provider: "$provider", destIso: "$destIso" },
                    calls: { $sum: 1 },
                    usdIfDid: { $sum: "$chargeDid" },
                    usdIfDest: { $sum: "$chargeDest" },
                    usdIfMax: { $sum: "$chargeMax" },
                    deltaDestMinusDid: {
                        $sum: { $subtract: ["$chargeDest", "$chargeDid"] },
                    },
                },
            },
            { $sort: { deltaDestMinusDid: -1 } },
        ])
        .toArray();
    console.log(JSON.stringify({ sinceDays, since: since.toISOString(), rows }, null, 2));
    await client.close();
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
