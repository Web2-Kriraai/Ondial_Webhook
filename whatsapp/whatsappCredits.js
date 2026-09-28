const { ObjectId } = require("mongodb");

const DEFAULT_PRICING = {
  enabled: true,
  platform: { templateMessageCredits: 1, sessionMessageCredits: 0.5 },
  ownAccount: { templateMessageCredits: 0, sessionMessageCredits: 0 },
};

const SESSION_BILLING_WINDOW_MS = 24 * 60 * 60 * 1000;

function cleanLabel(value) {
  const s = String(value ?? "").trim();
  return s || null;
}

async function loadWhatsappPricing(db) {
  const doc = await db.collection("systemsettings").findOne({ key: "whatsapp_pricing" });
  const value = doc?.value && typeof doc.value === "object" ? doc.value : {};
  return {
    enabled: value.enabled !== false,
    platform: {
      templateMessageCredits: Number(
        value.platform?.templateMessageCredits ?? DEFAULT_PRICING.platform.templateMessageCredits
      ),
      sessionMessageCredits: Number(
        value.platform?.sessionMessageCredits ?? DEFAULT_PRICING.platform.sessionMessageCredits
      ),
    },
    ownAccount: {
      templateMessageCredits: Number(
        value.ownAccount?.templateMessageCredits ?? DEFAULT_PRICING.ownAccount.templateMessageCredits
      ),
      sessionMessageCredits: Number(
        value.ownAccount?.sessionMessageCredits ?? DEFAULT_PRICING.ownAccount.sessionMessageCredits
      ),
    },
  };
}

function resolveRate(pricing, usesPlatformAccount, kind) {
  const bucket = usesPlatformAccount ? pricing.platform : pricing.ownAccount;
  const key = kind === "session" ? "sessionMessageCredits" : "templateMessageCredits";
  const rate = Number(bucket?.[key] ?? 0);
  return Number.isFinite(rate) ? Math.max(0, rate) : 0;
}

function toUserObjectId(user) {
  if (!user?._id) return null;
  return user._id instanceof ObjectId ? user._id : new ObjectId(String(user._id));
}

/** First AI session reply in 24h is billed; later replies in that window are free. */
async function hasBilledWhatsappSessionInWindow(db, { user, contactId, phone } = {}) {
  const userId = toUserObjectId(user);
  if (!userId) return false;
  const contactKey = contactId ? String(contactId) : "";
  const phoneKey = String(phone || "").replace(/[\s+\-()]/g, "");
  if (!contactKey && !phoneKey) return false;

  const query = {
    userId,
    type: "whatsapp_session_deduction",
    createdAt: { $gte: new Date(Date.now() - SESSION_BILLING_WINDOW_MS) },
  };
  if (contactKey) {
    query["reference.contactId"] = contactKey;
  } else {
    query.$or = [
      { "reference.phone": phoneKey },
      { "reference.contactPhone": phoneKey },
      { "reference.phone": phone },
      { "reference.contactPhone": phone },
    ];
  }

  const existing = await db.collection("credittransactions").findOne(query);
  return Boolean(existing);
}

/**
 * Deduct WhatsApp credits after a successful send.
 * Writes Ondial dashboard-compatible reference fields (contactPhone, campaignName, …).
 */
async function deductWhatsappCredits(db, {
  user,
  cost,
  kind,
  senderMode,
  campaignId,
  campaignName,
  contactId,
  messageId,
  phone,
  templateName,
}) {
  const amount = Number(cost);
  if (!user?._id || !Number.isFinite(amount) || amount <= 0) {
    return { ok: true, skipped: true, amount: 0 };
  }

  if (kind === "session") {
    const alreadyBilled = await hasBilledWhatsappSessionInWindow(db, { user, contactId, phone });
    if (alreadyBilled) {
      return { ok: true, skipped: true, amount: 0, windowFree: true };
    }
  }

  const phoneKey = cleanLabel(phone);
  const campaignLabel = cleanLabel(campaignName);
  const templateLabel = cleanLabel(templateName);
  const mid = cleanLabel(messageId);

  const billingKey = mid
    ? `wa:${kind}:${campaignId || "none"}:${contactId || phoneKey || "unknown"}:${mid}`
    : null;

  if (!billingKey) {
    const logger = require("../logger");
    logger.warn("[WhatsApp] Refusing credit deduction without stable messageId", {
      kind,
      campaignId: campaignId || null,
      contactId: contactId || null,
    });
    return { ok: false, error: "missing_message_id", amount: 0 };
  }

  const existing = await db.collection("credittransactions").findOne({
    type: kind === "session" ? "whatsapp_session_deduction" : "whatsapp_template_deduction",
    "reference.billingKey": billingKey,
  });
  if (existing) {
    return { ok: true, skipped: true, amount: 0, duplicate: true };
  }

  const userId = toUserObjectId(user);
  const updateRes = await db.collection("users").findOneAndUpdate(
    { _id: userId, credits: { $gte: amount } },
    { $inc: { credits: -amount }, $set: { updatedAt: new Date() } },
    { returnDocument: "after" }
  );

  const updatedUser = updateRes?.value ?? updateRes;
  if (!updatedUser?._id) {
    return { ok: false, error: "insufficient_credits", amount };
  }

  const balanceAfter = parseFloat(Number(updatedUser.credits || 0).toFixed(6));
  const type =
    kind === "session" ? "whatsapp_session_deduction" : "whatsapp_template_deduction";

  const description =
    kind === "session"
      ? `WhatsApp AI session reply${campaignLabel ? ` — ${campaignLabel}` : ""}`
      : `WhatsApp template follow-up${templateLabel ? `: ${templateLabel}` : ""}${
          campaignLabel ? ` — ${campaignLabel}` : ""
        }`;

  try {
    await db.collection("credittransactions").insertOne({
      userId,
      userEmail: user.email || updatedUser.email || "",
      type,
      amount: -amount,
      balanceAfter,
      description,
      reference: {
        billingKey,
        campaignId: campaignId ? String(campaignId) : null,
        campaignName: campaignLabel,
        contactId: contactId ? String(contactId) : null,
        messageId: mid,
        whatsappMessageId: mid,
        phone: phoneKey,
        contactPhone: phoneKey,
        templateName: templateLabel,
        whatsappKind: kind,
        senderMode: senderMode === "own" ? "own" : "platform",
        sessionWindowMs: kind === "session" ? SESSION_BILLING_WINDOW_MS : undefined,
        creditsCharged: amount,
      },
      createdAt: new Date(),
      updatedAt: new Date(),
    });
  } catch (insertErr) {
    if (insertErr?.code === 11000 || String(insertErr?.message || "").includes("E11000")) {
      try {
        await db.collection("users").updateOne(
          { _id: userId },
          { $inc: { credits: amount }, $set: { updatedAt: new Date() } }
        );
      } catch (refundErr) {
        const logger = require("../logger");
        logger.error("[WhatsApp] CRITICAL — failed to refund credits after E11000", {
          billingKey,
          amount,
          error: refundErr?.message,
        });
      }
      return { ok: true, skipped: true, amount: 0, duplicate: true };
    }
    throw insertErr;
  }

  return { ok: true, amount, balanceAfter };
}

async function getWhatsappSendCost(db, usesPlatformAccount, kind) {
  const pricing = await loadWhatsappPricing(db);
  if (!pricing.enabled) return 0;
  return resolveRate(pricing, usesPlatformAccount, kind);
}

module.exports = {
  deductWhatsappCredits,
  getWhatsappSendCost,
  hasBilledWhatsappSessionInWindow,
  SESSION_BILLING_WINDOW_MS,
};
