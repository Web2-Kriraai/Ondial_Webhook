/**
 * WhatsApp template sample_values for POST /v1/analysis/call.
 * Keep in parity with Calling_system1/worker-service/analysis_services/whatsappSampleValues.js
 */

const PLATFORM_WHATSAPP_SAMPLE_PRESETS = {
  appointment_reminder_en: ['Rahul', '20 July 2026', '3:00 PM'],
  followup_call_summary_en: ['Rahul', 'your insurance plan'],
  order_confirmation_demo: ['Rahul', 'ORD-10245', 'Premium Plan', 'Rs 2,499', '22 July 2026'],
  order_confirmation_image: ['Rahul', 'ORD-10245', 'Premium Plan', 'Rs 2,499', 'Jul 16, 2026'],
  product_demo_video: ['Rahul', 'Ondial AI'],
  shopsphere_followup_en: ['Rahul', 'your recent order'],
  card_offer_campaign: ['Rahul', 'Premium Plan', 'Rs 2,499', '50% off', '31 August 2026'],
  test_template: ['Rahul', 'ShopSphere'],
};

function isPlaceholderValue(value) {
  const s = String(value ?? '').trim();
  return !s || s === '—' || s === '-' || /^sample\s*\d*$/i.test(s);
}

/** Ondial Meta sync stores category as description ("Meta MARKETING") — not the template body. */
function isMetaSyncDescriptionLabel(value) {
  const s = String(value ?? '').trim();
  if (!s) return false;
  return /^meta(\s+(cloud\s+api|marketing|utility|authentication))?$/i.test(s);
}

/**
 * Analysis `description` must be the WhatsApp body copy (what {{1}}/{{2}} sit in).
 * Never send Meta category labels like "Meta MARKETING".
 */
function resolveWhatsappTemplateDescription(template = {}) {
  const body = String(template.bodyText || template.body || '').trim();
  const stored = String(template.description || '').trim();
  if (body) return body;
  if (stored && !isMetaSyncDescriptionLabel(stored)) return stored;
  return String(template.title || template.name || template.templateName || '').trim();
}

function templateKey(template = {}) {
  const raw = String(
    template.templateName ||
      template.name ||
      template.title ||
      template.aisensyCampaignName ||
      template.campaign_name ||
      ''
  )
    .trim()
    .toLowerCase()
    .replace(/_v\d+$/, '');
  return raw;
}

function countTemplateVariables(template = {}) {
  const declared = Number(template.variableCount) || 0;
  const body = resolveWhatsappTemplateDescription(template);
  const matches = [...body.matchAll(/\{\{(\d+)\}\}/g)].map((m) => Number(m[1])).filter((n) => n > 0);
  const fromBody = matches.length ? Math.max(...matches) : 0;
  const arrLen = Math.max(
    Array.isArray(template.sampleValues) ? template.sampleValues.length : 0,
    Array.isArray(template.variableDefaults) ? template.variableDefaults.length : 0
  );
  return Math.max(declared, fromBody, arrLen);
}

function normalizeSampleObject(existing) {
  if (!existing || typeof existing !== 'object' || Array.isArray(existing)) return null;
  const out = {};
  let meaningful = 0;
  for (const [rawKey, rawVal] of Object.entries(existing)) {
    const k = String(rawKey || '').trim();
    let key = null;
    if (/^\{\{\d+\}\}$/.test(k)) key = k;
    else if (/^\d+$/.test(k)) key = `{{${k}}}`;
    else {
      const m = k.match(/(\d+)/);
      if (m) key = `{{${m[1]}}}`;
    }
    if (!key) continue;
    const val = String(rawVal ?? '').trim();
    out[key] = val;
    if (!isPlaceholderValue(val)) meaningful += 1;
  }
  if (!Object.keys(out).length) return null;
  return meaningful > 0 ? out : null;
}

function normalizeSampleArray(arr = []) {
  const list = arr.map((v) => String(v ?? '').trim());
  if (!list.length) return null;
  return list.some((v) => !isPlaceholderValue(v)) ? list : null;
}

function extractSampleValuesFromMetaComponents(components = []) {
  const list = Array.isArray(components) ? components : [];
  const samples = [];

  for (const comp of list) {
    const type = String(comp?.type || '').toUpperCase();
    const example = comp?.example;
    if (!example || typeof example !== 'object') continue;

    if (type === 'BODY' && Array.isArray(example.body_text)) {
      const row = example.body_text[0];
      if (Array.isArray(row)) {
        samples.push(...row.map((v) => String(v ?? '').trim()).filter(Boolean));
      }
    }
    if (type === 'HEADER' && Array.isArray(example.header_text)) {
      const row = example.header_text[0];
      const val = Array.isArray(row) ? row[0] : row;
      if (val && samples.length === 0) samples.push(String(val).trim());
    }
  }

  return samples.filter(Boolean);
}

function firstName(fullName = '') {
  return String(fullName || '').trim().split(/\s+/)[0] || '';
}

function contextualSampleValues(count, context = {}) {
  const contactName =
    firstName(context.contactName) ||
    firstName(context.contact?.contactData?.name) ||
    firstName(context.contact?.name) ||
    'Customer';
  const companyName = String(
    context.companyName ||
      context.campaign?.companyName ||
      context.campaign?.selectedCompany?.name ||
      'our team'
  ).trim();
  const agentName = String(
    context.agentName || context.campaign?.agentName || context.campaign?.agent?.name || 'our team'
  ).trim();
  const pool = [contactName, companyName, agentName, 'your enquiry', 'details'];
  const out = [];
  for (let i = 0; i < count; i += 1) {
    out.push(pool[i] || `Value ${i + 1}`);
  }
  return out;
}

function presetForTemplate(template = {}) {
  const key = templateKey(template);
  if (!key) return [];
  return PLATFORM_WHATSAPP_SAMPLE_PRESETS[key] || [];
}

function resolveWhatsappTemplateSampleArray(template = {}, context = {}) {
  const count = countTemplateVariables(template);
  if (count <= 0) return [];

  const fromObject = normalizeSampleObject(template.sample_values ?? template.sampleValues);
  if (fromObject) {
    const arr = [];
    for (let i = 1; i <= count; i += 1) {
      arr.push(fromObject[`{{${i}}}`] ?? fromObject[String(i)] ?? '');
    }
    if (arr.some((v) => !isPlaceholderValue(v))) return arr;
  }

  const arrSource =
    normalizeSampleArray(template.sampleValues) ||
    normalizeSampleArray(template.variableDefaults) ||
    normalizeSampleArray(template.lastApprovedSnapshot?.sampleValues) ||
    normalizeSampleArray(extractSampleValuesFromMetaComponents(template.components)) ||
    presetForTemplate(template);

  const contextual = contextualSampleValues(count, context);
  const out = [];
  for (let i = 0; i < count; i += 1) {
    let candidate = arrSource[i] ?? contextual[i] ?? '';
    if (i === 0 && contextual[0] && contextual[0] !== 'Customer') {
      candidate = contextual[0];
    }
    out.push(isPlaceholderValue(candidate) ? contextual[i] || `Sample ${i + 1}` : candidate);
  }
  return out;
}

function buildWhatsappSampleValues(template = {}, context = {}) {
  const arr = resolveWhatsappTemplateSampleArray(template, context);
  if (!arr.length) return {};
  const out = {};
  arr.forEach((val, idx) => {
    out[`{{${idx + 1}}}`] = String(val ?? '').trim() || `Sample ${idx + 1}`;
  });
  return out;
}

function mapWhatsappTemplateDocForAnalysis(raw = {}) {
  const title = String(raw.name || raw.templateName || raw.aisensyCampaignName || '').trim();
  const templateName = String(raw.templateName || raw.name || title).trim();
  const bodyText = String(raw.bodyText || raw.body || '').trim();
  const description = resolveWhatsappTemplateDescription({
    ...raw,
    title,
    templateName,
    bodyText,
  });
  return {
    id: String(raw._id || raw.id || ''),
    title: title || templateName,
    name: title || templateName,
    templateName,
    description,
    bodyText,
    aisensyCampaignName: String(raw.aisensyCampaignName || '').trim(),
    campaignName: String(raw.campaignName || raw.campaign_name || '').trim(),
    provider: String(raw.provider || '').trim(),
    variableCount: Number(raw.variableCount) || 0,
    sampleValues: Array.isArray(raw.sampleValues)
      ? raw.sampleValues
      : Array.isArray(raw.variableDefaults)
        ? raw.variableDefaults
        : [],
    variableDefaults: Array.isArray(raw.variableDefaults)
      ? raw.variableDefaults
      : Array.isArray(raw.sampleValues)
        ? raw.sampleValues
        : [],
    components: Array.isArray(raw.components) ? raw.components : [],
    lastApprovedSnapshot:
      raw.lastApprovedSnapshot && typeof raw.lastApprovedSnapshot === 'object'
        ? raw.lastApprovedSnapshot
        : null,
  };
}

module.exports = {
  PLATFORM_WHATSAPP_SAMPLE_PRESETS,
  isMetaSyncDescriptionLabel,
  resolveWhatsappTemplateDescription,
  extractSampleValuesFromMetaComponents,
  resolveWhatsappTemplateSampleArray,
  buildWhatsappSampleValues,
  mapWhatsappTemplateDocForAnalysis,
};
