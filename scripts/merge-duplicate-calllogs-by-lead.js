/**
 * One-time cleanup: merge CallLogs that share the same non-empty lead_id.
 *
 * Why: concurrent webhook upserts can create two docs for one dial. db.js will not
 * enable lead_id_unique_nonempty while duplicates remain.
 *
 * Usage:
 *   node scripts/merge-duplicate-calllogs-by-lead.js --dry-run
 *   node scripts/merge-duplicate-calllogs-by-lead.js --apply
 *
 * Env: MONGODB_URI (or pass via process env already loaded by your deploy).
 */
const { MongoClient } = require("mongodb");

const uri = process.env.MONGODB_URI;
const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;
const LIMIT = Number(
    (process.argv.find((a) => a.startsWith("--limit=")) || "").split("=")[1] || 5000
);

function eventCount(doc) {
    return Array.isArray(doc?.call_data?.events) ? doc.call_data.events.length : 0;
}

function scoreDoc(doc) {
    const events = Array.isArray(doc?.call_data?.events) ? doc.call_data.events : [];
    const types = new Set(events.map((e) => String(e?.event_type || "").toLowerCase()));
    let score = events.length;
    if (types.has("call_answered") || types.has("call_ended") || types.has("call_hangup")) score += 100;
    if (doc?.status === "completed") score += 50;
    if (doc?.creditsDeducted) score += 25;
    if (doc?.recordingUrl) score += 10;
    return score;
}

function mergeEvents(docs) {
    const byKey = new Map();
    for (const doc of docs) {
        const events = Array.isArray(doc?.call_data?.events) ? doc.call_data.events : [];
        for (const ev of events) {
            const key = [
                String(ev?.event_type || ""),
                String(ev?.timestamp || ""),
                JSON.stringify(ev?.data || null),
            ].join("|");
            if (!byKey.has(key)) byKey.set(key, ev);
        }
    }
    return Array.from(byKey.values()).sort((a, b) =>
        String(a.timestamp || "").localeCompare(String(b.timestamp || ""))
    );
}

async function main() {
    if (!uri) {
        console.error("MONGODB_URI required");
        process.exit(1);
    }

    const client = new MongoClient(uri);
    await client.connect();
    const coll = client.db().collection(process.env.CALLLOGS_COLLECTION || "CallLogs");

    const groups = await coll
        .aggregate([
            { $match: { lead_id: { $type: "string", $gt: "" } } },
            {
                $group: {
                    _id: "$lead_id",
                    count: { $sum: 1 },
                    ids: { $push: "$_id" },
                },
            },
            { $match: { count: { $gt: 1 } } },
            { $limit: LIMIT },
        ])
        .toArray();

    console.log(
        `[merge-dups] found ${groups.length} duplicate lead_id groups (limit=${LIMIT}, mode=${DRY ? "dry-run" : "APPLY"})`
    );

    let merged = 0;
    let deleted = 0;

    for (const g of groups) {
        const docs = await coll.find({ _id: { $in: g.ids } }).toArray();
        if (docs.length < 2) continue;

        const ranked = [...docs].sort((a, b) => scoreDoc(b) - scoreDoc(a) || eventCount(b) - eventCount(a));
        const keeper = ranked[0];
        const losers = ranked.slice(1);
        const events = mergeEvents(ranked);

        const set = {
            "call_data.events": events,
            updatedAt: new Date(),
        };
        for (const loser of losers) {
            if (!keeper.recordingUrl && loser.recordingUrl) set.recordingUrl = loser.recordingUrl;
            if (!keeper.status && loser.status) set.status = loser.status;
            if (keeper.creditsDeducted == null && loser.creditsDeducted != null) {
                set.creditsDeducted = loser.creditsDeducted;
            }
            if (!keeper.provider_call_id && loser.provider_call_id) {
                set.provider_call_id = loser.provider_call_id;
            }
            if (!keeper.call_id && loser.call_id) set.call_id = loser.call_id;
        }

        console.log(
            `[merge-dups] lead_id=${g._id} keep=${keeper._id} drop=${losers.map((d) => d._id).join(",")} events=${events.length}`
        );

        if (!DRY) {
            await coll.updateOne({ _id: keeper._id }, { $set: set });
            const del = await coll.deleteMany({ _id: { $in: losers.map((d) => d._id) } });
            merged += 1;
            deleted += del.deletedCount || 0;
        }
    }

    console.log(`[merge-dups] done mergedGroups=${merged} deletedDocs=${deleted}`);
    await client.close();
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
