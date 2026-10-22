/**
 * Follow-up part of the inbound call-analysis request, so the analysis AI can choose a template for the
 * caller exactly like it does for an outbound campaign: `features_enabled.email_followup / whatsapp_followup`
 * (the owner's selected templates) and `user_contact_info`. Same shapes as the outbound request
 * (lib/analysis_services/shared.js). Keep in parity with Ondial/lib/inbound/inboundFollowupAnalysisFeatures.pure.js.
 *
 * The analysis response is read by Calling_system1: `next_action.type`, `next_action.whatsapp_draft`
 * (`selected_whatsapp_template_id`, `template_params`, …) and `next_action.email_draft`.
 */
const {
  buildWhatsappSampleValues,
  mapWhatsappTemplateDocForAnalysis,
  resolveWhatsappTemplateDescription,
} = require("../analysis_services/whatsappSampleValues");

function asString(value) {
  return value == null ? "" : String(value).trim();
}

/** True when the agent uses AI to choose the follow-up (follow-up on, selection not 'all'). */
function inboundFollowupUsesAi(config) {
  return config?.followup === true && config?.followupSelection !== "all";
}

/** Fill the placeholders the call already answers; leave the rest for the AI. */
function injectKnownVariables(text, vars) {
  if (!text) return "";
  return String(text).replace(/\{\{([\w_]+)(?::"([^"]*)")?\}\}/g, (match, key) => {
    const k = String(key || "").trim().toLowerCase();
    const v = vars[k];
    return v ? `{{${k}:"${String(v).replace(/"/g, '\\"')}"}}` : match;
  });
}

function inOrder(templates, ids) {
  const byId = new Map((templates || []).map((t) => [asString(t._id || t.id), t]));
  return (Array.isArray(ids) ? ids : [])
    .map((id) => byId.get(asString(id && id._id ? id._id : id)))
    .filter(Boolean);
}

function mapEmailTemplate(raw, vars) {
  return {
    id: asString(raw._id || raw.id),
    title: asString(raw.title || raw.name || raw.subject),
    description: asString(raw.description || raw.title || raw.name),
    subject: injectKnownVariables(raw.subject, vars),
    body: injectKnownVariables(raw.body, vars),
  };
}

function mapWhatsappTemplate(raw, context) {
  const t = mapWhatsappTemplateDocForAnalysis(raw);
  const title = asString(t.title || t.name || t.templateName);
  const campaignName = asString(
    t.aisensyCampaignName || t.campaignName || t.campaign_name || t.templateName || title
  );
  return {
    id: t.id,
    title: title || campaignName,
    description: resolveWhatsappTemplateDescription(t),
    // Meta has no AiSensy campaign: the template name stands in (the analysis requires a non-empty value).
    campaign_name: campaignName || title || "whatsapp_template",
    sample_values: buildWhatsappSampleValues(t, context),
  };
}

/**
 * @returns {{ features_enabled: object, user_contact_info: object } | null} null when this agent does not use
 *   the AI choice, or none of its selected templates could be loaded.
 */
function buildInboundFollowupAnalysisFeatures({
  config,
  emailTemplates = [],
  whatsappTemplates = [],
  caller = {},
  companyName = "",
  agentName = "",
} = {}) {
  if (!inboundFollowupUsesAi(config)) return null;
  const channels = Array.isArray(config.followupChannels) ? config.followupChannels : [];
  const contactName = asString(caller.name);
  const vars = {
    contact_name: contactName || "Customer",
    company_name: asString(companyName),
    agent_name: asString(agentName),
  };

  const emailList = channels.includes("email")
    ? inOrder(emailTemplates, config.followupEmailTemplateIds).map((t) => mapEmailTemplate(t, vars))
    : [];
  const waContext = { contactName, companyName: asString(companyName), agentName: asString(agentName) };
  const waList = channels.includes("whatsapp")
    ? inOrder(whatsappTemplates, config.followupWhatsappTemplateIds).map((t) =>
        mapWhatsappTemplate(t, waContext)
      )
    : [];
  if (emailList.length === 0 && waList.length === 0) return null;

  return {
    features_enabled: {
      is_followup_enabled: true,
      email_followup: emailList.length ? { status: true, templates: emailList } : { status: false },
      whatsapp_followup: waList.length ? { status: true, templates: waList } : { status: false },
      // Callbacks are not part of inbound follow-up.
      callback_scheduling: { status: false },
    },
    user_contact_info: {
      contact_name: contactName,
      contact_mobile: asString(caller.phone),
      contact_email: asString(caller.email),
    },
  };
}

module.exports = { inboundFollowupUsesAi, buildInboundFollowupAnalysisFeatures };
