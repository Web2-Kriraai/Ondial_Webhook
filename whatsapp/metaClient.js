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

function mapLanguageToMetaCode(codeOrName) {
  const raw = String(codeOrName || "en").trim();
  if (!raw) return "en_US";
  if (raw.includes("_")) return raw;
  const lower = raw.toLowerCase();
  if (lower === "en" || lower === "english") return "en_US";
  if (lower === "hi" || lower === "hindi") return "hi";
  return raw;
}

function buildMetaTemplateComponents(params = [], options = {}) {
  const values = (Array.isArray(params) ? params : []).map((p) => String(p ?? "").trim() || "—");
  const components = [];
  if (values.length) {
    components.push({
      type: "body",
      parameters: values.map((text) => ({ type: "text", text })),
    });
  }

  const headerType = String(options.headerType || "").toUpperCase();
  const mediaUrl = String(options.headerMediaUrl || options.mediaUrl || "").trim();
  if (["IMAGE", "VIDEO", "DOCUMENT"].includes(headerType) && mediaUrl) {
    const mediaType = headerType.toLowerCase();
    components.push({
      type: "header",
      parameters: [{ type: mediaType, [mediaType]: { link: mediaUrl } }],
    });
  }

  const headerParams = Array.isArray(options.headerParams) ? options.headerParams : [];
  if (headerType === "TEXT" && headerParams.length) {
    components.push({
      type: "header",
      parameters: headerParams.map((text) => ({
        type: "text",
        text: String(text ?? "").trim() || "—",
      })),
    });
  }

  return components;
}

function variableCount(text) {
  const indices = [...String(text || "").matchAll(/\{\{(\d+)\}\}/g)]
    .map((match) => Number(match[1]))
    .filter((index) => Number.isInteger(index) && index > 0);
  return indices.length ? Math.max(...indices) : 0;
}

function validateMetaTemplateSend({ template, languageCode, bodyParams, options = {} } = {}) {
  const language = mapLanguageToMetaCode(languageCode || template?.language || "en_US");
  if (!/^[a-z]{2,3}(?:_[A-Z]{2})?$/.test(language)) {
    return { ok: false, error: `Invalid Meta template language "${language || "(empty)"}"` };
  }
  const status = String(template?.status || "active").toLowerCase();
  if (!["active", "approved"].includes(status)) {
    return { ok: false, error: `Meta template status is ${status} (must be active/approved)` };
  }

  const expectedBody = variableCount(template?.bodyText);
  const values = Array.isArray(bodyParams) ? bodyParams : [];
  if (values.length !== expectedBody) {
    return {
      ok: false,
      error: `Meta template body expects ${expectedBody} parameter(s), received ${values.length}`,
    };
  }

  const headerType = String(template?.headerType || "").toUpperCase();
  if (["IMAGE", "VIDEO", "DOCUMENT"].includes(headerType)) {
    const mediaUrl = String(options.headerMediaUrl || options.mediaUrl || "").trim();
    if (!mediaUrl) {
      return { ok: false, error: `Meta ${headerType} header requires a public media URL` };
    }
  }

  return { ok: true, bodyVariableCount: expectedBody, languageCode: language };
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

  async sendTemplateMessage({ to, templateName, languageCode = "en_US", components = [] }) {
    if (!this.accessToken) {
      return { ok: false, error: "Meta access token is not configured" };
    }
    if (!this.phoneNumberId) {
      return { ok: false, error: "Meta phone number ID is not configured" };
    }
    const sanitizedTo = String(to || "")
      .replace(/\+/g, "")
      .replace(/\s/g, "");
    const name = String(templateName || "").trim();
    if (!sanitizedTo) return { ok: false, error: "destination phone is required" };
    if (!name) return { ok: false, error: "templateName is required" };

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
            type: "template",
            template: {
              name,
              language: { code: languageCode || "en_US" },
              components: Array.isArray(components) ? components : [],
            },
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
        error: data?.error?.message || data?.message || `Meta send failed (${response.status})`,
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
  buildMetaTemplateComponents,
  validateMetaTemplateSend,
  mapLanguageToMetaCode,
  getPlatformMetaAccessToken,
  getPlatformMetaPhoneNumberId,
  getProfileAccessToken,
  getProfileBusinessNumberId,
  getMetaApiVersion,
};
