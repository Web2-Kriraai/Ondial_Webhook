const { ObjectId } = require("mongodb");

const STATUS_MAP = {
  sent: "sent",
  delivered: "delivered",
  read: "read",
  failed: "failed",
};

const MARKETING_CAP_CODE = "131049";
const MEDIA_UPLOAD_ERROR_CODE = "131053";

/**
 * Extract delivery events from a Meta Cloud API webhook payload.
 */
function parseMetaStatusUpdates(payload) {
  if (!payload?.entry || !Array.isArray(payload.entry)) return [];

  return payload.entry.flatMap((entry) =>
    (entry.changes || []).flatMap((change) =>
      (change.value?.statuses || [])
        .map((status) => {
          const mappedStatus = STATUS_MAP[String(status?.status || "").toLowerCase()];
          const messageId = String(status?.id || "").trim();
          if (!mappedStatus || !messageId) return null;
          const error = status?.errors?.[0];
          const errorCode = error?.code != null ? String(error.code) : null;
          return {
            messageId,
            status: mappedStatus,
            errorCode,
            error:
              mappedStatus === "failed" && error
                ? `${error.code || "unknown"}: ${error.title || error.message || "Unknown delivery error"}`
                : null,
          };
        })
        .filter(Boolean)
    )
  );
}

function is131049(update) {
  if (String(update?.errorCode || "") === MARKETING_CAP_CODE) return true;
  return /\b131049\b/.test(String(update?.error || ""));
}

function is131053(update) {
  if (String(update?.errorCode || "") === MEDIA_UPLOAD_ERROR_CODE) return true;
  return (
    /\b131053\b/.test(String(update?.error || "")) ||
    /media upload error/i.test(String(update?.error || ""))
  );
}

function shouldRefundTemplateCredits(update) {
  return is131049(update) || is131053(update);
}

/**
 * Refund template credits when Meta async-fails with marketing cap 131049
 * or media upload error 131053.
 * Idempotent via reference.billingKey = wa:refund:<code>:<messageId>
 */
async function refundTemplateCreditsForMetaFailure(db, { messageId, contact, errorCode } = {}) {
  const mid = String(messageId || "").trim();
  const code = String(errorCode || MARKETING_CAP_CODE).trim() || MARKETING_CAP_CODE;
  if (!mid) return { refunded: false, reason: "no_message_id" };

  const refundKey = `wa:refund:${code}:${mid}`;
  const already = await db.collection("credittransactions").findOne({
    type: "whatsapp_template_refund",
    "reference.billingKey": refundKey,
  });
  if (already) {
    return { refunded: false, reason: "already_refunded" };
  }

  const deduction = await db.collection("credittransactions").findOne({
    type: "whatsapp_template_deduction",
    $or: [
      { "reference.whatsappMessageId": mid },
      { "reference.messageId": mid },
    ],
  });
  if (!deduction) {
    return { refunded: false, reason: "no_deduction" };
  }

  const amount = Math.abs(
    Number(deduction.reference?.creditsCharged) || Number(deduction.amount) || 0
  );
  if (!Number.isFinite(amount) || amount <= 0) {
    return { refunded: false, reason: "zero_amount" };
  }

  const userId =
    deduction.userId instanceof ObjectId
      ? deduction.userId
      : new ObjectId(String(deduction.userId));

  const updateRes = await db.collection("users").findOneAndUpdate(
    { _id: userId },
    { $inc: { credits: amount }, $set: { updatedAt: new Date() } },
    { returnDocument: "after" }
  );
  const updatedUser = updateRes?.value ?? updateRes;
  if (!updatedUser?._id) {
    return { refunded: false, reason: "user_not_found" };
  }

  const balanceAfter = parseFloat(Number(updatedUser.credits || 0).toFixed(6));
  const description =
    code === MEDIA_UPLOAD_ERROR_CODE
      ? "WhatsApp template refund (Meta 131053 media upload error)"
      : "WhatsApp template refund (Meta 131049 marketing limit)";

  await db.collection("credittransactions").insertOne({
    userId,
    userEmail: deduction.userEmail || updatedUser.email || "",
    type: "whatsapp_template_refund",
    amount,
    balanceAfter,
    description,
    reference: {
      billingKey: refundKey,
      refundOfBillingKey: deduction.reference?.billingKey || null,
      whatsappMessageId: mid,
      messageId: mid,
      campaignId: deduction.reference?.campaignId || null,
      contactId:
        deduction.reference?.contactId ||
        (contact?._id ? String(contact._id) : null),
      phone: deduction.reference?.phone || contact?.mobileNumber || null,
      creditsCharged: amount,
      metaErrorCode: code,
    },
    createdAt: new Date(),
    updatedAt: new Date(),
  });

  return { refunded: true, amount, balanceAfter };
}

/**
 * Update only the contact that owns Meta's message id.
 */
async function applyMetaStatusUpdates(db, payload) {
  const updates = parseMetaStatusUpdates(payload);
  if (!updates.length) return { processed: 0, matched: 0, refunds: 0 };

  let matched = 0;
  let refunds = 0;
  for (const update of updates) {
    const set = {
      whatsappStatus: update.status,
      updatedAt: new Date(),
    };
    if (update.status === "failed") {
      set.whatsappError = update.error || "Unknown delivery error";
      if (is131049(update)) {
        set.lastMarketingFailedAt = new Date();
      }
    } else {
      set.whatsappError = null;
    }

    const result = await db.collection("contactprocessings").findOneAndUpdate(
      { whatsappMessageId: update.messageId },
      { $set: set },
      { returnDocument: "after" }
    );
    const contact = result?.value ?? result;
    if (contact?._id) matched += 1;

    if (update.status === "failed" && shouldRefundTemplateCredits(update)) {
      try {
        const refund = await refundTemplateCreditsForMetaFailure(db, {
          messageId: update.messageId,
          contact,
          errorCode:
            update.errorCode ||
            (is131053(update) ? MEDIA_UPLOAD_ERROR_CODE : MARKETING_CAP_CODE),
        });
        if (refund.refunded) refunds += 1;
      } catch (err) {
        console.error(
          "[metaStatusUpdates] template refund failed:",
          err?.message || err
        );
      }
    }
  }

  return { processed: updates.length, matched, refunds };
}

module.exports = {
  parseMetaStatusUpdates,
  applyMetaStatusUpdates,
  refundTemplateCreditsForMetaFailure,
  refundTemplateCreditsFor131049: (db, args) =>
    refundTemplateCreditsForMetaFailure(db, {
      ...args,
      errorCode: MARKETING_CAP_CODE,
    }),
};
