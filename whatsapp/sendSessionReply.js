const { deductWhatsappCredits, getWhatsappSendCost, hasBilledWhatsappSessionInWindow } = require("./whatsappCredits");
const {
  buildMetaClientFromProfile,
  getProfileAccessToken,
  getProfileBusinessNumberId,
} = require("./metaClient");
const {
  normalizeWhatsAppProvider,
  assertProfileMatchesActiveProvider,
} = require("./providerResolve");

/**
 * Meta free-text session reply (24h window). AiSensy session sends stay on CS1.
 */
async function sendWhatsappSessionReply(db, {
  user,
  profile,
  phone,
  text,
  campaignId,
  campaignName,
  contactId,
  conversationWindowOpensUntil = null,
  // Wallet that pays (org admin for org users). Defaults to the creator `user`.
  billingUser = null,
}) {
  const payer = billingUser || user;
  if (conversationWindowOpensUntil && new Date(conversationWindowOpensUntil).getTime() <= Date.now()) {
    return { success: false, error: "conversation_window_closed" };
  }

  const usesPlatform = Boolean(profile?.usesPlatformAccount ?? true);
  const alreadyBilled =
    payer &&
    (await hasBilledWhatsappSessionInWindow(db, { user: payer, contactId, phone }));
  const cost = alreadyBilled ? 0 : await getWhatsappSendCost(db, usesPlatform, "session");
  if (cost > 0 && (Number(payer?.credits) || 0) < cost) {
    return { success: false, error: "insufficient_credits" };
  }

  const provider = normalizeWhatsAppProvider(profile?.provider || "meta");
  if (user) {
    const providerCheck = assertProfileMatchesActiveProvider(user, profile || { provider });
    if (!providerCheck.ok) {
      return { success: false, error: providerCheck.error };
    }
  }

  if (provider !== "meta") {
    return { success: false, error: "Non-Meta session replies are not handled by Ondial_Webhook" };
  }

  if (!getProfileAccessToken(profile) || !getProfileBusinessNumberId(profile)) {
    return { success: false, error: "Meta credentials not configured on profile" };
  }

  const client = buildMetaClientFromProfile(profile);
  const response = await client.sendTextMessage({ to: phone, text });
  if (!response.ok) {
    return { success: false, error: response.error || "Meta session send failed" };
  }

  const messageId = response.messageId || null;
  if (payer) {
    await deductWhatsappCredits(db, {
      user: payer,
      cost,
      kind: "session",
      senderMode: usesPlatform ? "platform" : "own",
      campaignId,
      campaignName,
      contactId,
      messageId,
      phone,
    });
  }

  return { success: true, messageId, via: "meta-graph", provider: "meta" };
}

module.exports = {
  sendWhatsappSessionReply,
};
