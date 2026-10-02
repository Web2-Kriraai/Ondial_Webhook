/**
 * When Twilio/Telnyx report a terminal call status, mark CallLogs + TestCall so
 * wizard "test call already in progress" does not stick on `initiated`.
 */
const { getDb } = require("../db");
const logger = require("../logger");

const CALLLOGS_COLLECTION = process.env.CALLLOGS_COLLECTION || "CallLogs";
const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";

const TERMINAL_STATUSES = new Set([
    "completed",
    "busy",
    "no-answer",
    "no_answer",
    "failed",
    "canceled",
    "cancelled",
]);

function mapProviderStatusToLogStatus(statusRaw) {
    const s = String(statusRaw || "")
        .trim()
        .toLowerCase();
    if (s === "completed") return "completed";
    if (s === "busy") return "busy";
    if (s === "no-answer" || s === "no_answer") return "no_answer";
    if (s === "failed") return "failed";
    if (s === "canceled" || s === "cancelled") return "canceled";
    return null;
}

function isTerminalProviderStatus(statusRaw) {
    const s = String(statusRaw || "")
        .trim()
        .toLowerCase();
    return TERMINAL_STATUSES.has(s);
}

/**
 * @param {{
 *   provider?: 'twilio'|'telnyx'|string,
 *   callSid?: string,
 *   callControlId?: string,
 *   callId?: string,
 *   providerStatus: string,
 *   at?: Date|string,
 * }} opts
 */
async function finalizeTerminalCallRecords({
    provider = "twilio",
    callSid,
    callControlId,
    callId,
    providerStatus,
    at = new Date(),
}) {
    const logStatus = mapProviderStatusToLogStatus(providerStatus);
    if (!logStatus) return { skipped: true, reason: "not_terminal" };

    const sid = callSid != null ? String(callSid).trim() : "";
    const ccid = callControlId != null ? String(callControlId).trim() : "";
    const cid = callId != null ? String(callId).trim() : "";
    if (!sid && !ccid && !cid) return { skipped: true, reason: "no_ids" };

    const when = at instanceof Date ? at : new Date(at);
    const atDate = Number.isFinite(when.getTime()) ? when : new Date();

    const db = getDb();
    const setFields = {
        status: logStatus,
        callHangupAt: atDate,
        concurrencyReleased: true,
        concurrencyReleasedAt: atDate,
        updatedAt: atDate,
    };

    const orFilters = [];
    if (sid) orFilters.push({ "twilio.call_sid": sid });
    if (ccid) orFilters.push({ "telnyx.call_control_id": ccid });
    if (cid) {
        orFilters.push({ call_id: cid }, { lead_id: cid });
    }

    const filter = { $or: orFilters };
    const [callLogs, testCalls] = await Promise.all([
        db.collection(CALLLOGS_COLLECTION).updateMany(filter, { $set: setFields }),
        db.collection(TESTCALL_COLLECTION).updateMany(filter, { $set: setFields }),
    ]);

    const label = String(provider || "provider").toLowerCase() === "telnyx" ? "Telnyx" : "Twilio";
    logger.info(`[${label}] Terminal status finalized on CallLogs/TestCall`, {
        provider: label.toLowerCase(),
        callSid: sid || null,
        callControlId: ccid || null,
        callId: cid || null,
        providerStatus,
        logStatus,
        callLogsMatched: callLogs.matchedCount,
        testCallsMatched: testCalls.matchedCount,
    });

    return {
        skipped: false,
        logStatus,
        callLogsMatched: callLogs.matchedCount,
        testCallsMatched: testCalls.matchedCount,
    };
}

/** @deprecated alias — prefer isTerminalProviderStatus */
function isTwilioTerminalStatus(statusRaw) {
    return isTerminalProviderStatus(statusRaw);
}

/** @deprecated alias — prefer mapProviderStatusToLogStatus */
function mapTwilioStatusToLogStatus(statusRaw) {
    return mapProviderStatusToLogStatus(statusRaw);
}

async function finalizeTwilioTerminalCallRecords({
    callSid,
    callId,
    twilioStatus,
    at = new Date(),
}) {
    return finalizeTerminalCallRecords({
        provider: "twilio",
        callSid,
        callId,
        providerStatus: twilioStatus,
        at,
    });
}

async function finalizeTelnyxTerminalCallRecords({
    callControlId,
    callId,
    telnyxStatus,
    at = new Date(),
}) {
    return finalizeTerminalCallRecords({
        provider: "telnyx",
        callControlId,
        callId,
        providerStatus: telnyxStatus,
        at,
    });
}

module.exports = {
    isTerminalProviderStatus,
    mapProviderStatusToLogStatus,
    finalizeTerminalCallRecords,
    isTwilioTerminalStatus,
    mapTwilioStatusToLogStatus,
    finalizeTwilioTerminalCallRecords,
    finalizeTelnyxTerminalCallRecords,
};
