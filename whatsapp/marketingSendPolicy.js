/**
 * Marketing send policy helpers (shared pattern with Ondial dashboard).
 * Used if Webhook paths send Marketing templates; inbox cooldown primarily lives in Ondial.
 */

const MARKETING_COOLDOWN_MS = 24 * 60 * 60 * 1000;

function normalizeWhatsappTemplateCategory(raw) {
  const c = String(raw || "")
    .trim()
    .toUpperCase();
  if (c === "MARKETING" || c === "UTILITY" || c === "AUTHENTICATION") return c;
  if (/^auth/i.test(c)) return "AUTHENTICATION";
  return c || "";
}

function isMarketingCategory(raw) {
  return normalizeWhatsappTemplateCategory(raw) === "MARKETING";
}

function is131049Error(errorText) {
  return (
    /\b131049\b/.test(String(errorText || "")) ||
    /healthy ecosystem engagement/i.test(String(errorText || ""))
  );
}

function assertMarketingSendAllowed(contact, templateCategory, { now = new Date() } = {}) {
  if (!isMarketingCategory(templateCategory)) return { ok: true };

  const err = String(contact?.whatsappError || "");
  if (!is131049Error(err)) return { ok: true };

  const failedAtRaw =
    contact?.lastMarketingFailedAt ||
    contact?.lastWhatsappSentAt ||
    contact?.updatedAt ||
    null;
  const failedAt = failedAtRaw ? new Date(failedAtRaw).getTime() : NaN;
  if (!Number.isFinite(failedAt)) {
    return {
      ok: false,
      code: "marketing_cooldown",
      error:
        "This contact recently hit Meta marketing limit (131049). Wait 24 hours or use a Utility template.",
    };
  }

  const elapsed = now.getTime() - failedAt;
  if (elapsed < MARKETING_COOLDOWN_MS) {
    const hoursLeft = Math.max(1, Math.ceil((MARKETING_COOLDOWN_MS - elapsed) / (60 * 60 * 1000)));
    return {
      ok: false,
      code: "marketing_cooldown",
      error: `Meta marketing limit (131049) applies to this contact. Wait about ${hoursLeft}h or send a Utility template instead.`,
    };
  }

  return { ok: true };
}

module.exports = {
  MARKETING_COOLDOWN_MS,
  normalizeWhatsappTemplateCategory,
  isMarketingCategory,
  is131049Error,
  assertMarketingSendAllowed,
};
