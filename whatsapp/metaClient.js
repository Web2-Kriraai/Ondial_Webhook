const { tryDecryptToken } = require("./tokenDecrypt");
const { normalizeWhatsAppProvider } = require("./providerResolve");

const TIMEOUT_MS = 20_000;

function getPlatformMetaAccessToken() {
  return String(process.env.WHATSAPP_API_TOKEN || "").trim();
}

function getPlatformMetaPhoneNumberId() {
  return String(
    process.env.WHATSAPP_PHONE_NUMBER_ID || process.env.WHATSAPP_BUSINESS_NUMBER_ID || ""
  ).trim();
}

function getMetaApiVersion() {
  return String(process.env.WHATSAPP_API_VERSION || "v21.0").trim() || "v21.0";
}

function graphBaseUrl() {
  return `https://graph.facebook.com/${getMetaApiVersion()}`;
}

function getProfileAccessToken(profile) {
  if (!profile) return getPlatformMetaAccessToken();
  if (normalizeWhatsAppProvider(profile.provider) !== "meta") return "";
  if (profile.usesPlatformAccount) return getPlatformMetaAccessToken();
  return tryDecryptToken(profile.accessToken) || String(profile.accessToken || "").trim() || "";
}

function getProfileBusinessNumberId(profile) {
  if (!profile) return getPlatformMetaPhoneNumberId();
  const fromProfile = String(profile.businessNumberId || profile.numberId || "").trim();
  if (fromProfile) return fromProfile;
  if (profile.usesPlatformAccount) return getPlatformMetaPhoneNumberId();
  return "";
}

class MetaWhatsappClient {
  constructor({ accessToken, phoneNumberId } = {}) {
    this.accessToken = String(accessToken || "").trim();
    this.phoneNumberId = String(phoneNumberId || "").trim();
  }

  async sendTextMessage({ to, text }) {
    if (!this.accessToken) {
      return { ok: false, error: "Meta access token is not configured" };
    }
    if (!this.phoneNumberId) {
      return { ok: false, error: "Meta phone number ID is not configured" };
    }
    const sanitizedTo = String(to || "")
      .replace(/\+/g, "")
      .replace(/\s/g, "");
    const body = String(text || "").trim();
    if (!sanitizedTo || !body) {
      return { ok: false, error: "to and text are required" };
    }

    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
      let response;
      try {
        response = await fetch(`${graphBaseUrl()}/${this.phoneNumberId}/messages`, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${this.accessToken}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            messaging_product: "whatsapp",
            to: sanitizedTo,
            type: "text",
            text: { body },
          }),
          signal: controller.signal,
        });
      } finally {
        clearTimeout(timer);
      }

      const data = await response.json().catch(() => ({}));
      if (response.status >= 200 && response.status < 300) {
        return {
          ok: true,
          messageId: data?.messages?.[0]?.id || null,
          data,
          via: "meta-graph",
        };
      }
      return {
        ok: false,
        error:
          data?.error?.message || data?.message || `Meta send failed (${response.status})`,
        data,
        status: response.status,
      };
    } catch (error) {
      return {
        ok: false,
        error: error.name === "AbortError" ? "Meta send timed out" : error.message,
        status: 502,
      };
    }
  }
}

function buildMetaClientFromProfile(profile) {
  return new MetaWhatsappClient({
    accessToken: getProfileAccessToken(profile),
    phoneNumberId: getProfileBusinessNumberId(profile),
  });
}

module.exports = {
  MetaWhatsappClient,
  buildMetaClientFromProfile,
  getPlatformMetaAccessToken,
  getPlatformMetaPhoneNumberId,
  getProfileAccessToken,
  getProfileBusinessNumberId,
  getMetaApiVersion,
};
