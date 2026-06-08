/**
 * Root CallLog fields the Ondial dashboard expects (recording, status, duration).
 */
const { getDb } = require("../db");

const CALLLOGS_COLLECTION = process.env.CALLLOGS_COLLECTION || "CallLogs";
const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";
const INBOUNDCALLLOG_COLLECTION = process.env.INBOUNDCALLLOG_COLLECTION || "InboundConversation";

async function finalizeOutboundCallLog({
    callUniqueId,
    durationSec,
    recordingUrl,
    callStatus,
    collectionName = CALLLOGS_COLLECTION,
    lifecycleEvent,
    mongoFilter,
    mongoSyncFilter,
}) {
    const callId = String(callUniqueId || "").trim();
    if (!callId && !mongoFilter) return;

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
    const filter =
        mongoFilter ||
        (collectionName === INBOUNDCALLLOG_COLLECTION
            ? { call_id: callId }
            : { $or: [{ lead_id: callId }, { call_id: callId }] });

    if (collectionName === INBOUNDCALLLOG_COLLECTION && mongoSyncFilter?.call_sid) {
        await db.collection(collectionName).updateMany(mongoSyncFilter, { $set: set });
    } else {
        await db.collection(collectionName).updateOne(filter, { $set: set });
    }
}

module.exports = { finalizeOutboundCallLog };
