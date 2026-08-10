/**
 * Collapse duplicate outbound CallLogs that share the same dial identity.
 *
 * Concurrent India webhook upserts (call_initiated + call_ringing) can both insert
 * when lead_id is not unique yet — producing two docs with the same call_id and a
 * false "2nd attempt / Retry" in the Ondial UI.
 */
const logger = require("../logger");

function eventCount(doc) {
    return Array.isArray(doc?.call_data?.events) ? doc.call_data.events.length : 0;
}

function legCount(doc) {
    return Array.isArray(doc?.call_data?.legs) ? doc.call_data.legs.length : 0;
}

function scoreOutboundCallLog(doc) {
    if (!doc || typeof doc !== "object") return 0;
    const events = Array.isArray(doc?.call_data?.events) ? doc.call_data.events : [];
    const types = new Set(events.map((e) => String(e?.event_type || "").toLowerCase()));
    let score = events.length;
    score += legCount(doc) * 2;
    if (types.has("call_answered") || types.has("call_ended") || types.has("call_hangup")) {
        score += 100;
    }
    if (doc?.status === "completed") score += 50;
    if (doc?.creditsDeducted === true) score += 25;
    if (doc?.recordingUrl) score += 10;
    if (Number(doc?.duration) > 0 || Number(doc?.duration_ms) > 0) score += 8;
    if (doc?.updatedAt && doc?.createdAt && String(doc.updatedAt) !== String(doc.createdAt)) {
        score += 1;
    }
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

function pickPreferredField(keeper, losers, field) {
    if (keeper?.[field] != null && String(keeper[field]).trim() !== "") return undefined;
    for (const loser of losers) {
        if (loser?.[field] != null && String(loser[field]).trim() !== "") return loser[field];
    }
    return undefined;
}

/**
 * @param {import("mongodb").Collection} coll
 * @param {{ leadId?: string|null, callId?: string|null, callUniqueId?: string|null }} identity
 * @returns {Promise<object|null>} keeper doc after merge, or null if nothing to do
 */
async function mergeOutboundDuplicateCallLogs(coll, identity = {}) {
    const leadId = identity.leadId != null ? String(identity.leadId).trim() : "";
    const callId = identity.callId != null ? String(identity.callId).trim() : "";
    const callUniqueId =
        identity.callUniqueId != null ? String(identity.callUniqueId).trim() : "";

    const keys = [...new Set([leadId, callId, callUniqueId].filter(Boolean))];
    if (!keys.length) return null;

    const or = [];
    for (const key of keys) {
        or.push({ lead_id: key }, { call_id: key }, { call_unique_id: key });
    }

    const docs = await coll.find({ $or: or }).toArray();
    if (docs.length < 2) return docs[0] || null;

    // Only collapse docs that truly share an identity key (avoid merging unrelated contact rows).
    const keySet = new Set(keys);
    const related = docs.filter((doc) => {
        const ids = [doc.lead_id, doc.call_id, doc.call_unique_id]
            .filter((v) => v != null && String(v).trim() !== "")
            .map((v) => String(v).trim());
        return ids.some((id) => keySet.has(id));
    });
    if (related.length < 2) return related[0] || null;

    const ranked = [...related].sort(
        (a, b) =>
            scoreOutboundCallLog(b) - scoreOutboundCallLog(a) ||
            eventCount(b) - eventCount(a) ||
            String(b.updatedAt || "").localeCompare(String(a.updatedAt || ""))
    );
    const keeper = ranked[0];
    const losers = ranked.slice(1);
    const events = mergeEvents(ranked);

    const $set = {
        "call_data.events": events,
        updatedAt: new Date(),
    };

    const preferredLegs = ranked.find((d) => legCount(d) > 0)?.call_data?.legs;
    if (preferredLegs) $set["call_data.legs"] = preferredLegs;

    for (const field of [
        "recordingUrl",
        "status",
        "provider_call_id",
        "call_id",
        "call_unique_id",
        "lead_id",
        "campaign_id",
        "contact_id",
        "to_number",
        "from_number",
        "duration",
        "duration_ms",
        "conversation",
    ]) {
        const preferred = pickPreferredField(keeper, losers, field);
        if (preferred !== undefined) $set[field] = preferred;
    }
    if (keeper.creditsDeducted !== true) {
        const credited = losers.find((d) => d.creditsDeducted === true);
        if (credited) {
            $set.creditsDeducted = true;
            if (credited.creditsDeductedAmount != null) {
                $set.creditsDeductedAmount = credited.creditsDeductedAmount;
            }
        }
    }
    if (keeper.isTestCall !== true && losers.some((d) => d.isTestCall === true)) {
        $set.isTestCall = true;
    }

    await coll.updateOne({ _id: keeper._id }, { $set });
    const del = await coll.deleteMany({ _id: { $in: losers.map((d) => d._id) } });

    logger.info("[CallLog] Merged duplicate outbound CallLogs", {
        keeperId: String(keeper._id),
        deleted: del.deletedCount || 0,
        leadId: leadId || null,
        callId: callId || null,
        callUniqueId: callUniqueId || null,
        eventCount: events.length,
    });

    return coll.findOne({ _id: keeper._id });
}

module.exports = {
    mergeOutboundDuplicateCallLogs,
    scoreOutboundCallLog,
    mergeEvents,
};
