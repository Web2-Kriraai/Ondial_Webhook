const { parseNaiveDateTimeInZone } = require("./campaignIntlTimeZone");
const {
  resolveMaxFollowUpCallsPerContact,
  isFollowUpCapReached,
  followUpArmCapFilter,
  buildFollowUpCapReachedHistory,
} = require("../lib/followUpArmCap");

const BUSY_STATUSES = ["processing", "enqueued"];
// callReceiveStatus 2 = ringing / in-progress. 1 (busy / no-answer / failed) is a finished
// call and must stay armable, otherwise a callback request after a missed call is lost.
const BUSY_RECEIVE_STATUSES = [2];

function contactBusy(status, callReceiveStatus) {
  const st = String(status || "").toLowerCase();
  if (BUSY_STATUSES.includes(st)) return true;
  return BUSY_RECEIVE_STATUSES.includes(Number(callReceiveStatus));
}

function parseScheduledAt(raw, campaign = null) {
  if (raw == null || raw === "") return null;
  if (raw instanceof Date) return Number.isNaN(raw.getTime()) ? null : raw;
  // No offset / Z → wall-clock time in the campaign timezone (fallback Asia/Kolkata),
  // not the server's local zone.
  const naive = parseNaiveDateTimeInZone(raw, campaign?.timezone);
  if (naive) return naive;
  const direct = new Date(raw);
  return Number.isNaN(direct.getTime()) ? null : direct;
}

function resolveScheduledAt(raw, campaign, now = new Date()) {
  const parsed = parseScheduledAt(raw, campaign);
  // Never arm in the past: a parsed time <= now falls through to now + retry delay.
  if (parsed && parsed.getTime() > now.getTime()) return parsed;
  const fallbackMin = Math.max(
    1,
    parseInt(process.env.CALLBACK_SCHEDULE_FALLBACK_MINUTES || "5", 10)
  );
  const retryMin = Number(campaign?.retryDelayMinutes);
  const minutes = Number.isFinite(retryMin) && retryMin > 0 ? retryMin : fallbackMin;
  return new Date(now.getTime() + minutes * 60 * 1000);
}

function buildWhatsappCallbackPatches({
  contact = {},
  analysis = null,
  campaign = null,
  callbackUpdate = null,
  now = new Date(),
} = {}) {
  if (!callbackUpdate || typeof callbackUpdate !== "object") {
    return { applied: false, reason: "missing" };
  }

  const status = callbackUpdate.status === true;
  const acknowledged = callbackUpdate.acknowledged !== false;
  const analysisId = analysis?._id || null;

  if (!status) {
    const wasFollowUp = contact.isFollowUp === true;
    const contactSet = { updatedAt: now };
    if (wasFollowUp && !contactBusy(contact.status)) {
      contactSet.isFollowUp = false;
      contactSet.scheduledAt = null;
      contactSet.nextRetryAt = null;
      if (String(contact.status || "") === "pending") {
        contactSet.status = "completed";
      }
    }
    return {
      applied: true,
      action: "cancel",
      contactSet,
      contactPush: {
        statusHistory: {
          fromStatus: contact.status || "",
          toStatus: contactSet.status || contact.status || "",
          reason: "callback-cancelled-whatsapp",
          analysisId,
          timestamp: now,
        },
      },
      analysisSet: {
        "analysis_data.callback_requested": {
          status: false,
          scheduled_at: null,
          acknowledged,
          cancelled_at: now.toISOString(),
          source: "whatsapp_ai",
        },
        updatedAt: now,
      },
    };
  }

  if (contactBusy(contact.status, contact.callReceiveStatus)) {
    return buildAcknowledgeOnlyPatches({ acknowledged, now, reason: "contact_busy" });
  }

  // A follow-up that is already armed and not yet dialed: re-timing it is not a new arm,
  // so it neither consumes nor is blocked by the per-contact cap.
  const isPendingFollowUp =
    contact.isFollowUp === true && String(contact.status || "") === "pending";

  const keepExisting =
    !callbackUpdate.scheduled_at &&
    contact.scheduledAt &&
    new Date(contact.scheduledAt).getTime() > now.getTime();
  const scheduledAt = keepExisting
    ? new Date(contact.scheduledAt)
    : resolveScheduledAt(callbackUpdate.scheduled_at, campaign, now);

  const analysisSet = {
    "analysis_data.callback_requested": {
      status: true,
      scheduled_at: scheduledAt.toISOString(),
      window: callbackUpdate.scheduled_at || null,
      acknowledged,
      source: "whatsapp_ai",
      updated_at: now.toISOString(),
    },
    updatedAt: now,
  };

  // F9: already pending at the same future time → nothing changes on the contact.
  if (keepExisting && isPendingFollowUp) {
    return {
      applied: true,
      action: "acknowledge",
      scheduledAt,
      contactSet: null,
      analysisSet,
    };
  }

  if (!isPendingFollowUp && isFollowUpCapReached(contact, campaign)) {
    return buildCapReachedPatches({ contact, campaign, analysisId, acknowledged, now });
  }

  const cap = resolveMaxFollowUpCallsPerContact(campaign);
  return {
    applied: true,
    action: keepExisting ? "acknowledge" : contact.isFollowUp ? "reschedule" : "schedule",
    scheduledAt,
    // Conditional arm (TOCTOU): only when the contact is still not busy, and — for a new arm —
    // still below the follow-up cap. matchedCount 0 → acknowledge-only fallback.
    contactFilter: {
      status: { $nin: BUSY_STATUSES },
      callReceiveStatus: { $nin: BUSY_RECEIVE_STATUSES },
      ...(isPendingFollowUp
        ? { isFollowUp: true, status: "pending" }
        : followUpArmCapFilter(cap)),
    },
    contactInc: isPendingFollowUp ? null : { followUpArmCount: 1 },
    contactSet: {
      status: "pending",
      scheduledAt,
      isFollowUp: true,
      lastFollowUpAt: now,
      retryCount: 0,
      nextRetryAt: null,
      callReceiveStatus: 0,
      priority: "High",
      updatedAt: now,
    },
    contactPush: {
      statusHistory: {
        fromStatus: contact.status || "",
        toStatus: "pending",
        reason: "callback-requested",
        analysisId,
        callbackScheduledAt: scheduledAt,
        acknowledged,
        source: "whatsapp_ai",
        timestamp: now,
      },
    },
    analysisSet,
  };
}

function buildAcknowledgeOnlyPatches({ acknowledged, now, reason }) {
  return {
    applied: true,
    action: "acknowledge_only",
    reason,
    contactSet: { updatedAt: now },
    analysisSet: {
      "analysis_data.callback_requested.status": true,
      "analysis_data.callback_requested.acknowledged": acknowledged,
      "analysis_data.callback_requested.source": "whatsapp_ai",
      updatedAt: now,
    },
  };
}

function buildCapReachedPatches({ contact, campaign, analysisId, acknowledged, now }) {
  const ack = buildAcknowledgeOnlyPatches({ acknowledged, now, reason: "follow-up-cap-reached" });
  return {
    ...ack,
    action: "cap_reached",
    contactPush: {
      statusHistory: buildFollowUpCapReachedHistory({
        contact,
        campaign,
        analysisId,
        source: "whatsapp_ai",
      }),
    },
    analysisSet: {
      ...ack.analysisSet,
      "analysis_data.callback_requested.cap_reached": true,
    },
  };
}

async function applyWhatsappCallbackUpdate(
  db,
  { contact, analysis, campaign, callbackUpdate, now = new Date() } = {}
) {
  let patches = buildWhatsappCallbackPatches({
    contact,
    analysis,
    campaign,
    callbackUpdate,
    now,
  });
  if (!patches.applied || !db) return patches;

  if (contact?._id && patches.contactSet) {
    const update = { $set: patches.contactSet };
    if (patches.contactPush) update.$push = patches.contactPush;
    if (patches.contactInc) update.$inc = patches.contactInc;
    const filter = { ...(patches.contactFilter || {}), _id: contact._id };
    const result = await db.collection("contactprocessings").updateOne(filter, update);

    if (patches.contactFilter && result.matchedCount === 0) {
      // Lost a race (dialer picked the contact up, or a concurrent arm hit the cap):
      // fall back to acknowledge-only, recording the cap if that is why.
      const latest =
        (await db.collection("contactprocessings").findOne({ _id: contact._id })) || contact;
      const capped =
        patches.contactInc && isFollowUpCapReached(latest, campaign);
      patches = capped
        ? buildCapReachedPatches({
            contact: latest,
            campaign,
            analysisId: analysis?._id || null,
            acknowledged: callbackUpdate.acknowledged !== false,
            now,
          })
        : buildAcknowledgeOnlyPatches({
            acknowledged: callbackUpdate.acknowledged !== false,
            now,
            reason: "contact_busy",
          });
      const fallbackUpdate = { $set: patches.contactSet };
      if (patches.contactPush) fallbackUpdate.$push = patches.contactPush;
      await db.collection("contactprocessings").updateOne({ _id: contact._id }, fallbackUpdate);
    }
  }

  if (analysis?._id && patches.analysisSet) {
    await db.collection("call_analysis").updateOne(
      { _id: analysis._id },
      { $set: patches.analysisSet }
    );
  }

  return patches;
}

module.exports = {
  parseScheduledAt,
  resolveScheduledAt,
  buildWhatsappCallbackPatches,
  applyWhatsappCallbackUpdate,
};
