/**
 * Sync contactprocessings from Twilio status / conversation (aligns with Calling_system1 worker).
 */
const { ObjectId } = require("mongodb");
const { getDb } = require("../db");
const logger = require("../logger");
const { pickNonEmpty } = require("./customParameters");

function isMongoObjectIdString(s) {
    return typeof s === "string" && /^[a-fA-F0-9]{24}$/.test(s);
}

function mapTwilioCallStatusToReceiveStatus(statusRaw) {
    const s = String(statusRaw || "").trim().toLowerCase();
    if (s === "in-progress" || s === "ringing") return 2;
    if (s === "completed") return 3;
    if (s === "busy" || s === "no-answer" || s === "failed" || s === "canceled" || s === "cancelled") {
        return 1;
    }
    return null;
}

function resolveTwilioContactId({ twilioMapping, body, storedDoc }) {
    return pickNonEmpty(twilioMapping?.contact_id, body?.contact_id, storedDoc?.contact_id);
}

/**
 * @returns {Promise<{ outcome: string, contact_id?: string, callReceiveStatus?: number }>}
 */
async function syncTwilioContactFromCall({
    contactIdRaw,
    twilioStatus,
    callSid,
    source = "twilio_webhook",
}) {
    const contactId = contactIdRaw != null ? String(contactIdRaw).trim() : "";
    if (!contactId || !isMongoObjectIdString(contactId)) {
        return { outcome: "skip_invalid_contact_id" };
    }

    const twilioStatusNorm = String(twilioStatus || "").trim().toLowerCase();
    const receiveStatus = mapTwilioCallStatusToReceiveStatus(twilioStatusNorm);
    if (receiveStatus == null) {
        return { outcome: "skip_unmapped_status", contact_id: contactId };
    }

    const db = getDb();
    const oid = new ObjectId(contactId);
    const contact = await db.collection("contactprocessings").findOne(
        { _id: oid },
        {
            projection: {
                status: 1,
                callReceiveStatus: 1,
                callAttempts: 1,
                retryCount: 1,
                isFollowUp: 1,
            },
        }
    );
    if (!contact) {
        logger.warn("[Twilio] contactprocessings not found for sync", {
            contact_id: contactId,
            twilioStatus: twilioStatusNorm,
            CallSid: callSid || null,
        });
        return { outcome: "contact_not_found", contact_id: contactId };
    }

    const prevStatus = String(contact.status || "");
    const prevReceive = Number(contact.callReceiveStatus) || 0;

    if (receiveStatus === 3 && prevReceive === 3 && prevStatus === "completed") {
        return {
            outcome: "already_completed",
            contact_id: contactId,
            callReceiveStatus: 3,
            status: "completed",
        };
    }

    if (
        receiveStatus !== 3 &&
        prevStatus === "completed" &&
        prevReceive === 3
    ) {
        logger.info("[Twilio] contact sync skipped — already finalized completed", {
            contact_id: contactId,
            attemptedReceiveStatus: receiveStatus,
            CallSid: callSid || null,
        });
        return {
            outcome: "blocked_downgrade",
            contact_id: contactId,
            callReceiveStatus: 3,
        };
    }

    const now = new Date();
    const attemptNum =
        (Array.isArray(contact.callAttempts) ? contact.callAttempts.length : 0) + 1;
    const historyEntry = {
        fromStatus: prevStatus,
        fromReceiveStatus: prevReceive,
        toReceiveStatus: receiveStatus,
        reason: source,
        twilioStatus: twilioStatusNorm,
        CallSid: callSid != null ? String(callSid) : null,
        timestamp: now,
    };

    let update = { $set: { updatedAt: now } };
    let filter = { _id: oid };

    if (receiveStatus === 3) {
        historyEntry.toStatus = "completed";
        update.$set.status = "completed";
        update.$set.callReceiveStatus = 3;
        update.$set.lastCallAttempt = now;
        update.$set.nextRetryAt = null;

        if (prevReceive !== 3) {
            update.$push = {
                callAttempts: {
                    attempt: attemptNum,
                    timestamp: now,
                    status: "completed",
                    message: "Twilio completed",
                },
                statusHistory: historyEntry,
            };
        } else {
            update.$push = { statusHistory: historyEntry };
        }
    } else if (receiveStatus === 1) {
        const maxRetries = Math.max(
            1,
            Number(process.env.TWILIO_CONTACT_MAX_RETRIES || process.env.DEFAULT_MAX_RETRY_ATTEMPTS || 3)
        );
        const retryDelayMin = Math.max(
            1,
            Number(process.env.TWILIO_CONTACT_RETRY_DELAY_MINUTES || 30)
        );
        const isFollowUp = contact.isFollowUp === true;
        const isRetryable = attemptNum < maxRetries;
        const nextStatus = isRetryable ? "retry" : "failed";

        historyEntry.toStatus = nextStatus;
        update.$set.status = nextStatus;
        update.$set.callReceiveStatus = 1;
        update.$set.lastCallAttempt = now;
        update.$set.nextRetryAt = isRetryable
            ? new Date(now.getTime() + retryDelayMin * 60 * 1000)
            : null;

        update.$push = {
            callAttempts: {
                attempt: attemptNum,
                timestamp: now,
                status: nextStatus,
                message:
                    twilioStatusNorm === "busy"
                        ? "Twilio busy"
                        : `Twilio ${twilioStatusNorm}`,
            },
            statusHistory: historyEntry,
        };

        if (isFollowUp) {
            update.$set.retryCount = 0;
        } else {
            update.$inc = { retryCount: 1 };
        }

        filter = { _id: oid, callReceiveStatus: { $ne: 3 } };
    } else if (receiveStatus === 2) {
        historyEntry.toStatus = "processing";
        update.$set.status = "processing";
        update.$set.callReceiveStatus = 2;
        if (prevReceive !== 2) {
            update.$push = { statusHistory: historyEntry };
        }
    }

    try {
        const result = await db.collection("contactprocessings").updateOne(filter, update);
        if (result.matchedCount === 0 && receiveStatus === 1) {
            return {
                outcome: "skipped_already_completed",
                contact_id: contactId,
                callReceiveStatus: 3,
            };
        }
        if (result.matchedCount === 0) {
            return { outcome: "no_match", contact_id: contactId };
        }

        logger.info("[Twilio] contactprocessings synced", {
            contact_id: contactId,
            twilioStatus: twilioStatusNorm,
            callReceiveStatus: receiveStatus,
            toStatus: historyEntry.toStatus,
            attempt: attemptNum,
            modified: result.modifiedCount > 0,
            source,
            CallSid: callSid || null,
        });

        return {
            outcome: result.modifiedCount > 0 ? "updated" : "unchanged",
            contact_id: contactId,
            callReceiveStatus: receiveStatus,
            status: historyEntry.toStatus,
        };
    } catch (err) {
        logger.error("[Twilio] contactprocessings sync failed", {
            contact_id: contactId,
            error: err.message,
            CallSid: callSid || null,
        });
        return { outcome: "error", contact_id: contactId, error: err.message };
    }
}

/** @deprecated Use syncTwilioContactFromCall — thin wrapper for call-status only */
async function syncTwilioContactReceiveStatus(contactIdRaw, statusRaw, callSid) {
    return syncTwilioContactFromCall({
        contactIdRaw,
        twilioStatus: statusRaw,
        callSid,
        source: "twilio_call_status",
    });
}

module.exports = {
    mapTwilioCallStatusToReceiveStatus,
    resolveTwilioContactId,
    syncTwilioContactFromCall,
    syncTwilioContactReceiveStatus,
};
