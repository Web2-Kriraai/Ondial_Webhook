function contactBusy(status) {
  const st = String(status || "").toLowerCase();
  return st === "processing" || st === "enqueued";
}

function parseScheduledAt(raw) {
  if (raw == null || raw === "") return null;
  if (raw instanceof Date) return Number.isNaN(raw.getTime()) ? null : raw;
  const direct = new Date(raw);
  return Number.isNaN(direct.getTime()) ? null : direct;
}

function resolveScheduledAt(raw, campaign, now = new Date()) {
  const parsed = parseScheduledAt(raw);
  if (parsed) return parsed;
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

  if (contactBusy(contact.status)) {
    return {
      applied: true,
      action: "acknowledge_only",
      reason: "contact_busy",
      contactSet: { updatedAt: now },
      analysisSet: {
        "analysis_data.callback_requested.status": true,
        "analysis_data.callback_requested.acknowledged": acknowledged,
        "analysis_data.callback_requested.source": "whatsapp_ai",
        updatedAt: now,
      },
    };
  }

  const keepExisting =
    !callbackUpdate.scheduled_at &&
    contact.scheduledAt &&
    new Date(contact.scheduledAt).getTime() > now.getTime();
  const scheduledAt = keepExisting
    ? new Date(contact.scheduledAt)
    : resolveScheduledAt(callbackUpdate.scheduled_at, campaign, now);

  return {
    applied: true,
    action: keepExisting ? "acknowledge" : contact.isFollowUp ? "reschedule" : "schedule",
    scheduledAt,
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
    analysisSet: {
      "analysis_data.callback_requested": {
        status: true,
        scheduled_at: scheduledAt.toISOString(),
        window: callbackUpdate.scheduled_at || null,
        acknowledged,
        source: "whatsapp_ai",
        updated_at: now.toISOString(),
      },
      updatedAt: now,
    },
  };
}

async function applyWhatsappCallbackUpdate(
  db,
  { contact, analysis, campaign, callbackUpdate, now = new Date() } = {}
) {
  const patches = buildWhatsappCallbackPatches({
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
    await db.collection("contactprocessings").updateOne({ _id: contact._id }, update);
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
  buildWhatsappCallbackPatches,
  applyWhatsappCallbackUpdate,
};
