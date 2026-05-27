/**
 * Root CallLog fields the Ondial dashboard expects (recording, status, duration).
 */
const { getDb } = require("../db");

const CALLLOGS_COLLECTION = process.env.CALLLOGS_COLLECTION || "CallLogs";
const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";

async function finalizeOutboundCallLog({
    callUniqueId,
    durationSec,
    recordingUrl,
    callStatus,
    collectionName = CALLLOGS_COLLECTION,
    lifecycleEvent,
}) {
    const callId = String(callUniqueId || "").trim();
    if (!callId) return;

    const set = { updatedAt: new Date() };
    const dur = Math.max(0, Math.floor(Number(durationSec) || 0));
    if (dur > 0) {
        set.duration = dur;
        set.duration_ms = dur * 1000;
    }

    const rec = recordingUrl != null ? String(recordingUrl).trim() : "";
    if (rec) set.recordingUrl = rec;

    const cs = callStatus != null ? String(callStatus).trim().toUpperCase() : "";
    if (cs === "ANSWER" || cs === "ANSWERED" || dur > 0) {
        set.status = "completed";
    }

    if (collectionName === TESTCALL_COLLECTION) {
        const now = new Date();
        if (lifecycleEvent === "call_answered") set.callAnsweredAt = now;
        if (lifecycleEvent === "call_hangup") set.callHangupAt = now;
    }

    const db = getDb();
    await db.collection(collectionName).updateOne(
        { $or: [{ lead_id: callId }, { call_id: callId }] },
        { $set: set }
    );
}

module.exports = { finalizeOutboundCallLog };
