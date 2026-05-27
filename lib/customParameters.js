/**
 * Telephony providers pass dialer metadata in different shapes:
 * - nested object: call.customParameters
 * - JSON string: custom_parameters (Calling_system1 / initiate-campaign-call)
 * - flat fields on legacy payloads
 */

function pickNonEmpty(...values) {
    for (const v of values) {
        if (v == null) continue;
        const s = String(v).trim();
        if (s !== "") return s;
    }
    return null;
}

function parseJsonObject(raw) {
    if (raw == null) return null;
    if (typeof raw === "object" && !Array.isArray(raw)) return raw;
    if (typeof raw !== "string") return null;
    const trimmed = raw.trim();
    if (!trimmed || trimmed[0] !== "{") return null;
    try {
        const parsed = JSON.parse(trimmed);
        return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : null;
    } catch {
        return null;
    }
}

/**
 * @param {object} call - Provider `call` object (or flat body)
 * @param {object} [body] - Full webhook body for legacy flat fields
 */
function extractCustomParameters(call, body) {
    const c = call && typeof call === "object" ? call : {};
    const root = body && typeof body === "object" ? body : {};

    const fromNested =
        parseJsonObject(c.customParameters) ||
        parseJsonObject(c.custom_parameters) ||
        (c.customParameters && typeof c.customParameters === "object" ? c.customParameters : null) ||
        (c.custom_parameters && typeof c.custom_parameters === "object" ? c.custom_parameters : null);

    const fromRoot =
        parseJsonObject(root.customParameters) ||
        parseJsonObject(root.custom_parameters) ||
        (root.customParameters && typeof root.customParameters === "object" ? root.customParameters : null);

    const merged = { ...(fromRoot || {}), ...(fromNested || {}) };

    const contact_id = pickNonEmpty(
        merged.contact_id,
        merged.contactId,
        root.contact_id,
        root.contactId,
        c.contact_id,
        c.contactId
    );

    const call_unique_id = pickNonEmpty(
        merged.call_unique_id,
        merged.callUniqueId,
        merged.Call_UniqueId,
        root.call_unique_id,
        root.callUniqueId,
        root.Call_UniqueId,
        c.call_unique_id,
        c.callUniqueId
    );

    const campaign_id = pickNonEmpty(
        merged.campaign_id,
        merged.campaignId,
        root.campaign_id,
        root.campaignId,
        c.campaign_id,
        c.campaignId
    );

    const lead_id = pickNonEmpty(
        merged.lead_id,
        merged.leadId,
        root.lead_id,
        root.leadId,
        c.lead_id,
        c.leadId
    );

    return {
        contact_id,
        call_unique_id,
        campaign_id,
        lead_id,
        raw: merged,
    };
}

module.exports = {
    extractCustomParameters,
    pickNonEmpty,
};
