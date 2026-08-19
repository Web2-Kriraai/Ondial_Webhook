const { ObjectId } = require("mongodb");
const { deductWhatsappCredits, getWhatsappSendCost } = require("./whatsappCredits");
const { appendSessionHistory, getOrCreateSession } = require("./whatsappAiSessions");
const { sendWhatsappSessionReply } = require("./sendSessionReply");
const { assertProfileMatchesActiveProvider, normalizeWhatsAppProvider } = require("./providerResolve");
const {
  buildMetaClientFromProfile,
  buildMetaTemplateComponents,
  getProfileAccessToken,
  getProfileBusinessNumberId,
  mapLanguageToMetaCode,
  validateMetaTemplateSend,
} = require("./metaClient");

function getPrimaryFollowupWhatsappTemplateId(campaign) {
  const arr = campaign?.followupWhatsappTemplateIds;
  if (Array.isArray(arr) && arr.length > 0) {
    const first = arr[0];
    if (first != null && String(first).trim() !== "") {
      return String(first._id ?? first).trim();
    }
  }
  return null;
}

function normalizePhone(raw) {
  const digits = String(raw || "").replace(/[\s+\-()]/g, "");
  if (!digits) return { valid: false, error: "Phone required" };
  let phone = digits;
  if (phone.length === 10) phone = `91${phone}`;
  if (phone.length < 10 || phone.length > 15) {
    return { valid: false, error: "Invalid phone length" };
  }
  return { valid: true, phone };
}

function findWhatsappProfile(user, profileId) {
  const profiles = user?.omniChannelSettings?.whatsappProfiles || [];
  const id = String(profileId || "").trim();
  if (id) {
    return profiles.find((p) => String(p._id) === id) || null;
  }
  return (
    profiles.find((p) => p.isDefault && p.verified) ||
    profiles.find((p) => p.usesPlatformAccount) ||
    profiles.find((p) => p.verified) ||
    profiles[0] ||
    null
  );
}

function getContactName(contact) {
  if (!contact?.contactData) return "";
  for (const key of Object.keys(contact.contactData)) {
    const k = key.toLowerCase();
    if (k === "name" || k === "fullname" || k === "first name") {
      return String(contact.contactData[key] || "")
        .trim()
        .split(" ")[0];
    }
  }
  return "";
}

function getWhatsappDraftFromAnalysis(analysisData) {
  if (!analysisData || typeof analysisData !== "object") return null;
  const next = analysisData.next_action || analysisData.Next_Action || null;
  if (!next || typeof next !== "object") return null;
  const draft =
    next.WhatsApp_Draft ||
    next.Whatsapp_Draft ||
    next.whatsapp_draft ||
    next.whatsappDraft ||
    next.WhatsAppDraft ||
    null;
  return draft && typeof draft === "object" ? draft : null;
}

function resolveTemplateParamsFromDraft(draft, expectedCount = 0) {
  if (!draft || typeof draft !== "object") return null;

  if (Array.isArray(draft.template_params) && draft.template_params.length > 0) {
    const params = draft.template_params.map((p) => String(p ?? "").trim() || "—");
    const want = Number(draft.template_variable_count || expectedCount || 0);
    if (want > 0) {
      while (params.length < want) params.push("—");
      return params.slice(0, want);
    }
    return params;
  }

  const sampleValues = draft.sample_values || draft.sampleValues;
  if (sampleValues && typeof sampleValues === "object" && !Array.isArray(sampleValues)) {
    const indices = Object.keys(sampleValues)
      .map((k) => {
        const m = String(k).match(/(\d+)/);
        return m ? Number(m[1]) : null;
      })
      .filter((n) => Number.isFinite(n) && n > 0);
    const maxIdx = indices.length ? Math.max(...indices) : 0;
    const want = Number(draft.template_variable_count || expectedCount || maxIdx || 0);
    if (want <= 0) return null;
    const params = [];
    for (let i = 1; i <= want; i += 1) {
      const key = `{{${i}}}`;
      const alt = String(i);
      const value = sampleValues[key] ?? sampleValues[alt] ?? "—";
      params.push(String(value ?? "").trim() || "—");
    }
    return params;
  }

  return null;
}

function buildFollowupTemplateParams(contact, template, analysisData = null) {
  const draft = getWhatsappDraftFromAnalysis(analysisData);
  const expectedCount = Number(draft?.template_variable_count || template.variableCount || 0);
  const fromDraft = resolveTemplateParamsFromDraft(draft, expectedCount);
  if (fromDraft && fromDraft.length > 0) return fromDraft;

  const count = Number(template.variableCount || 0);
  if (count <= 0) return [];
  const name = getContactName(contact);
  const params = [];
  for (let i = 1; i <= count; i += 1) {
    if (i === 1 && name) params.push(name);
    else params.push("—");
  }
  return params;
}

function timeMs(value) {
  if (!value) return 0;
  const date = value instanceof Date ? value : new Date(value);
  const ms = date.getTime();
  return Number.isNaN(ms) ? 0 : ms;
}

/** True when customer replied within Meta's 24h session window. */
function isWhatsappConversationWindowOpen(contact, nowMs = Date.now()) {
  const until = timeMs(contact?.conversationWindowOpensUntil);
  if (until > nowMs) return true;
  const lastReply = timeMs(contact?.lastCustomerWhatsappReplyAt);
  return lastReply > 0 && nowMs - lastReply < 24 * 60 * 60 * 1000;
}

async function sendWhatsappSessionFollowup(db, { user, campaign, contact, analysis, text }) {
  const profile = findWhatsappProfile(user, campaign?.followupWhatsappProfileId);
  if (!profile) {
    return { success: false, error: "WhatsApp profile not found on campaign" };
  }

  const phoneResult = normalizePhone(contact?.mobileNumber);
  if (!phoneResult.valid) {
    return { success: false, error: phoneResult.error };
  }

  const body = String(text || "").trim();
  if (!body) {
    return { success: false, error: "session_message_empty" };
  }

  const sessionResult = await sendWhatsappSessionReply(db, {
    user,
    profile,
    phone: phoneResult.phone,
    text: body,
    campaignId: campaign?._id,
    contactId: contact?._id,
    conversationWindowOpensUntil: contact?.conversationWindowOpensUntil || null,
  });
  if (!sessionResult.success) {
    return sessionResult;
  }

  const bodyPreview = body;
  try {
    const session = await getOrCreateSession(db, {
      phone: phoneResult.phone,
      campaignId: campaign._id,
      contactId: contact._id,
      userId: user._id,
      userName: getContactName(contact),
      callId: analysis?.call_id || analysis?.callId || contact?.callId || null,
      analysisId: analysis?._id || null,
    });
    await appendSessionHistory(db, session.sessionKey, {
      role: "assistant",
      text: bodyPreview,
      messageId: sessionResult.messageId || "",
      isAiGenerated: false,
      kind: "session_followup",
    });
  } catch {
    /* ignore */
  }

  return {
    success: true,
    provider: "meta",
    messageId: sessionResult.messageId || null,
    phone: phoneResult.phone,
    bodyPreview,
    via: sessionResult.via || "meta-graph",
    kind: "session_followup",
  };
}

/**
 * Window open → free-text from analysis draft; window closed → approved template.
 */
async function sendWhatsappFollowupOrSession(db, { user, campaign, contact, analysis = null, forceTemplate = false }) {
  if (!forceTemplate && isWhatsappConversationWindowOpen(contact)) {
    const analysisData = analysis?.analysis_data || analysis?.analysisData || null;
    const draft = getWhatsappDraftFromAnalysis(analysisData);
    const text = String(draft?.message || "").trim();
    if (text) {
      const sessionResult = await sendWhatsappSessionFollowup(db, {
        user,
        campaign,
        contact,
        analysis,
        text,
      });
      if (sessionResult.success) return sessionResult;
    }
  }
  return sendWhatsappFollowup(db, { user, campaign, contact, analysis });
}

async function resolveFollowupWhatsappTemplate(db, user, campaign, preferredTemplateId = null) {
  const templateId =
    String(preferredTemplateId || "").trim() || getPrimaryFollowupWhatsappTemplateId(campaign);
  if (!templateId) {
    return { ok: false, message: "No WhatsApp template selected on campaign" };
  }

  const userId = user?._id;
  try {
    const oid = ObjectId.isValid(templateId) ? new ObjectId(templateId) : null;
    if (oid) {
      const managed = await db.collection("whatsapptemplates").findOne({
        _id: oid,
        userId: userId instanceof ObjectId ? userId : new ObjectId(String(userId)),
      });
      if (managed) {
        const status = String(managed.status || "active").toLowerCase();
        if (status !== "active") {
          return { ok: false, message: `WhatsApp template status is ${status} (must be active)` };
        }
        return { ok: true, template: managed, source: "whatsapptemplates" };
      }
    }
  } catch {
    /* continue */
  }

  let platformTpl = null;
  try {
    if (ObjectId.isValid(templateId)) {
      platformTpl = await db.collection("platform_whatsapp_templates").findOne({
        _id: new ObjectId(templateId),
      });
    }
  } catch {
    platformTpl = null;
  }

  if (!platformTpl) {
    return { ok: false, message: "WhatsApp template not found" };
  }

  const status = String(platformTpl.status || "active").toLowerCase();
  if (status !== "active") {
    return { ok: false, message: `WhatsApp template status is ${status} (must be active)` };
  }

  const ownerId = platformTpl.ownerUserId ? String(platformTpl.ownerUserId) : "";
  const isOwner = ownerId && String(userId) === ownerId;

  if (platformTpl.source === "user_created" || ownerId) {
    if (!isOwner) {
      return { ok: false, message: "WhatsApp template is not owned by this user" };
    }
    return { ok: true, template: platformTpl, source: "platform_user_created" };
  }

  const assigned = await db.collection("platform_whatsapp_template_assignments").findOne({
    userId: userId instanceof ObjectId ? userId : new ObjectId(String(userId)),
    platformTemplateId: platformTpl._id,
    isActive: { $ne: false },
  });
  if (!assigned) {
    return { ok: false, message: "WhatsApp template is not assigned to this user" };
  }

  return { ok: true, template: platformTpl, source: "platform_assigned" };
}

/**
 * Send campaign WhatsApp template (Meta). Used for first follow-up and window-closed re-engagement.
 */
async function sendWhatsappFollowup(db, { user, campaign, contact, analysis = null }) {
  const profile = findWhatsappProfile(user, campaign?.followupWhatsappProfileId);
  if (!profile) {
    return { success: false, error: "WhatsApp profile not found on campaign" };
  }
  if (profile.verified === false) {
    return { success: false, error: "WhatsApp profile is not verified" };
  }

  const providerCheck = assertProfileMatchesActiveProvider(user, profile);
  if (!providerCheck.ok) {
    return { success: false, error: providerCheck.error };
  }
  if (normalizeWhatsAppProvider(profile.provider) !== "meta") {
    return { success: false, error: "Template follow-up on Ondial_Webhook requires Meta provider" };
  }

  const analysisData = analysis?.analysis_data || analysis?.analysisData || null;
  const draft = getWhatsappDraftFromAnalysis(analysisData);
  const preferredTemplateId =
    draft?.selected_whatsapp_template_id || draft?.selectedWhatsappTemplateId || null;

  const resolved = await resolveFollowupWhatsappTemplate(db, user, campaign, preferredTemplateId);
  if (!resolved.ok || !resolved.template) {
    return { success: false, error: resolved.message || "No WhatsApp template selected" };
  }
  const template = resolved.template;

  const draftPhone = String(draft?.user_mobile || draft?.userMobile || "").trim();
  const phoneResult = normalizePhone(draftPhone || contact.mobileNumber);
  if (!phoneResult.valid) {
    return { success: false, error: phoneResult.error };
  }

  const usesPlatform = Boolean(profile.usesPlatformAccount);
  const cost = await getWhatsappSendCost(db, usesPlatform, "template");
  if (cost > 0 && (Number(user?.credits) || 0) < cost) {
    return { success: false, error: "insufficient_credits" };
  }

  const templateParams = buildFollowupTemplateParams(contact, template, analysisData);
  const templateName = String(
    draft?.selected_whatsapp_template_name ||
      draft?.selectedWhatsappTemplateName ||
      template.templateName ||
      template.name ||
      ""
  ).trim();
  if (!templateName) {
    return { success: false, error: "Template missing Meta template name" };
  }

  const accessToken = getProfileAccessToken(profile);
  const businessNumberId = getProfileBusinessNumberId(profile);
  if (!accessToken) {
    return { success: false, error: "Meta access token not configured on profile" };
  }
  if (!businessNumberId) {
    return { success: false, error: "Meta business number not configured on profile" };
  }

  const componentOptions = {
    headerType: template.headerType,
    headerMediaUrl: template.headerMediaUrl || template.mediaUrl,
    headerParams: draft?.header_params || draft?.headerParams,
    buttonParams: draft?.button_params || draft?.buttonParams,
  };
  const preflight = validateMetaTemplateSend({
    template,
    languageCode: mapLanguageToMetaCode(template.language || "en_US"),
    bodyParams: templateParams,
    options: componentOptions,
  });
  if (!preflight.ok) {
    return { success: false, error: preflight.error, provider: "meta" };
  }

  const client = buildMetaClientFromProfile(profile);
  const components = buildMetaTemplateComponents(
    templateParams.slice(0, preflight.bodyVariableCount),
    componentOptions
  );
  const response = await client.sendTemplateMessage({
    to: phoneResult.phone,
    templateName,
    languageCode: preflight.languageCode || mapLanguageToMetaCode(template.language || "en_US"),
    components,
  });
  if (!response.ok) {
    return { success: false, error: response.error || "Meta send failed", provider: "meta" };
  }

  const messageId = response.messageId || null;
  const deduct = await deductWhatsappCredits(db, {
    user,
    cost,
    kind: "template",
    senderMode: usesPlatform ? "platform" : "own",
    campaignId: campaign._id,
    contactId: contact._id,
    messageId,
    phone: phoneResult.phone,
  });
  if (!deduct.ok && !deduct.skipped) {
    return { success: false, error: deduct.error || "credit_deduction_failed" };
  }

  const bodyPreview =
    String(draft?.message || template.bodyText || template.name || templateName).trim() ||
    templateName;

  try {
    const session = await getOrCreateSession(db, {
      phone: phoneResult.phone,
      campaignId: campaign._id,
      contactId: contact._id,
      userId: user._id,
      userName: getContactName(contact),
      callId: analysis?.call_id || analysis?.callId || contact?.callId || null,
      analysisId: analysis?._id || null,
    });
    await appendSessionHistory(db, session.sessionKey, {
      role: "assistant",
      text: bodyPreview,
      messageId: messageId || "",
      isAiGenerated: false,
      kind: "template",
    });
  } catch {
    /* ignore session append errors */
  }

  return {
    success: true,
    provider: "meta",
    messageId,
    phone: phoneResult.phone,
    bodyPreview,
    via: "meta-graph",
    creditsDeducted: deduct.amount || 0,
    kind: "first_template",
  };
}

module.exports = {
  sendWhatsappFollowup,
  sendWhatsappFollowupOrSession,
  sendWhatsappSessionFollowup,
  isWhatsappConversationWindowOpen,
  getPrimaryFollowupWhatsappTemplateId,
  resolveFollowupWhatsappTemplate,
  buildFollowupTemplateParams,
};
