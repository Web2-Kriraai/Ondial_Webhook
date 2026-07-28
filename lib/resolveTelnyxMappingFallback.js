/**
 * When Mission Control posts Telnyx events without our mapping (or before /api/telnyx-mapping),
 * recover campaign/contact/dialer ids from outbound phone mapping or a recent CallLogs shell.
 */
const { getDb } = require("../db");
const logger = require("../logger");
const {
    lookupTelnyxCallControlMapping,
    lookupMappingByPhone,
    registerTelnyxCallControlMapping,
    normalizePhone,
    normalizeCallId,
} = require("../callMapping");

function phoneLookupVariants(raw) {
    const digits = String(raw || "").replace(/\D/g, "");
    const ten = normalizePhone(raw);
    const out = new Set();
    if (raw) out.add(String(raw).trim());
    if (digits) {
        out.add(digits);
        out.add(`+${digits}`);
    }
    if (ten) {
        out.add(ten);
        out.add(`91${ten}`);
        out.add(`+91${ten}`);
        out.add(`1${ten}`);
        out.add(`+1${ten}`);
    }
    return [...out].filter(Boolean);
}

async function findRecentDialerCallLogByTo({ to, collectionName, maxAgeMs = 30 * 60 * 1000 }) {
    const variants = phoneLookupVariants(to);
    if (!variants.length) return null;
    const db = getDb();
    const coll = collectionName || process.env.CALLLOGS_COLLECTION || "CallLogs";
    const since = new Date(Date.now() - maxAgeMs);
    const sinceIso = since.toISOString();

    const doc = await db.collection(coll).findOne(
        {
            $and: [
                {
                    $or: [
                        { to_number: { $in: variants } },
                        { phone_number: { $in: variants } },
                        { contact_phone: { $in: variants } },
                        { "telnyx.to": { $in: variants } },
                    ],
                },
                {
                    $or: [
                        { createdAt: { $gte: since } },
                        { createdAt: { $gte: sinceIso } },
                        { updatedAt: { $gte: since } },
                    ],
                },
                { campaign_id: { $exists: true, $nin: [null, ""] } },
            ],
        },
        { sort: { createdAt: -1 } }
    );
    return doc || null;
}

/**
 * @returns {Promise<object|null>} telnyx mapping-shaped entry
 */
async function resolveTelnyxMappingWithFallbacks({
    callControlId,
    to,
    collectionName,
}) {
    const sid = String(callControlId || "").trim();
    if (!sid) return null;

    let mapping = await lookupTelnyxCallControlMapping(sid);
    if (mapping?.campaign_id && mapping?.contact_id) return mapping;

    const phoneMap = to ? await lookupMappingByPhone(to) : null;
    const dialerDoc =
        !phoneMap?.campaign_id && to
            ? await findRecentDialerCallLogByTo({ to, collectionName })
            : null;

    const callId = normalizeCallId(
        phoneMap?.call_id ||
            phoneMap?.call_unique_id ||
            dialerDoc?.call_unique_id ||
            dialerDoc?.call_id ||
            dialerDoc?.lead_id ||
            mapping?.call_id ||
            ""
    );
    const campaignId = String(
        phoneMap?.campaign_id || dialerDoc?.campaign_id || mapping?.campaign_id || ""
    ).trim();
    const contactId = String(
        phoneMap?.contact_id || dialerDoc?.contact_id || mapping?.contact_id || ""
    ).trim();
    const leadId = String(
        phoneMap?.lead_id || dialerDoc?.lead_id || callId || mapping?.lead_id || ""
    ).trim();
    const isTest =
        phoneMap?.is_test_call === true ||
        dialerDoc?.isTestCall === true ||
        mapping?.is_test_call === true;

    if (!campaignId && !contactId && !callId) {
        return mapping;
    }

    const resolved = {
        call_control_id: sid,
        telnyx_call_control_id: sid,
        call_id: callId || "",
        lead_id: leadId || callId || "",
        campaign_id: campaignId || "",
        contact_id: contactId || "",
        collectionName: collectionName || phoneMap?.collectionName || "",
        is_test_call: isTest,
        recovered_from: phoneMap ? "outbound_phone_mapping" : dialerDoc ? "recent_calllog" : "partial",
    };

    try {
        await registerTelnyxCallControlMapping({
            call_control_id: sid,
            call_id: resolved.call_id,
            lead_id: resolved.lead_id,
            campaign_id: resolved.campaign_id,
            contact_id: resolved.contact_id,
            collectionName: resolved.collectionName,
            is_test_call: resolved.is_test_call,
        });
        logger.info("[Telnyx] Recovered call_control mapping", {
            call_control_id: sid,
            call_id: resolved.call_id || null,
            campaign_id: resolved.campaign_id || null,
            contact_id: resolved.contact_id || null,
            recovered_from: resolved.recovered_from,
            to: to || null,
        });
    } catch (err) {
        logger.warn("[Telnyx] Failed to persist recovered mapping", {
            call_control_id: sid,
            error: err.message,
        });
    }

    return resolved;
}

module.exports = {
    resolveTelnyxMappingWithFallbacks,
    findRecentDialerCallLogByTo,
};
