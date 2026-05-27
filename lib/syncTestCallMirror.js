/**
 * Keep wizard TestCall rows in sync when billing/events use CallLogs (campaign path).
 */
const { getDb } = require("../db");

const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";

function testCallMatchFilter(callId, leadId) {
    const ids = [callId, leadId]
        .filter((x) => x != null && String(x).trim() !== "")
        .map((x) => String(x).trim());
    const unique = [...new Set(ids)];
    if (unique.length === 0) return null;
    const or = [];
    for (const id of unique) {
        or.push({ call_id: id }, { lead_id: id });
    }
    return { $or: or };
}

async function syncTestCallMirror({ callId, leadId, set }) {
    const filter = testCallMatchFilter(callId, leadId);
    if (!filter || !set || typeof set !== "object") return;

    const db = getDb();
    await db.collection(TESTCALL_COLLECTION).updateOne(filter, {
        $set: { ...set, updatedAt: new Date() },
    });
}

module.exports = { syncTestCallMirror };
