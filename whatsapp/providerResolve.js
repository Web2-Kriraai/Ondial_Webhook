function normalizeWhatsAppProvider(value) {
  const raw = String(value || "")
    .trim()
    .toLowerCase();
  return raw === "meta" ? "meta" : "aisensy";
}

function resolveActiveWhatsAppProvider(user) {
  const settings = user?.omniChannelSettings || {};
  const fromWhatsapp = settings?.whatsapp?.provider;
  if (fromWhatsapp) return normalizeWhatsAppProvider(fromWhatsapp);

  const profiles = Array.isArray(settings?.whatsappProfiles) ? settings.whatsappProfiles : [];
  const defaultProfile = profiles.find((p) => p?.isDefault) || profiles[0];
  return normalizeWhatsAppProvider(defaultProfile?.provider || "aisensy");
}

function assertProfileMatchesActiveProvider(user, profile) {
  const active = resolveActiveWhatsAppProvider(user);
  const profileProvider = normalizeWhatsAppProvider(profile?.provider || "aisensy");
  if (profileProvider !== active) {
    return {
      ok: false,
      error: `WhatsApp profile provider (${profileProvider}) does not match active provider (${active})`,
      active,
      profileProvider,
    };
  }
  return { ok: true, active, profileProvider };
}

module.exports = {
  normalizeWhatsAppProvider,
  resolveActiveWhatsAppProvider,
  assertProfileMatchesActiveProvider,
};
