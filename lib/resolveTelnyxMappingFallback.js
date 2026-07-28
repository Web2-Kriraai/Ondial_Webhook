/**
 * When Mission Control posts Telnyx events without our mapping (or before /api/telnyx-mapping),
 * recover campaign/contact/dialer ids from a recent CallLogs shell and/or outbound phone mapping.
 *
 * Prefer the newest unbound dialer CallLog over Redis phone mapping — the phone index is often
 * stale from a previous call to the same number (caused wrong campaign/contact billing).
 *
 * Pure helpers (chooseRecoverySource) are importable without Mongo/.env for unit tests.
 */
const logger = require("../logger");

/** Phone mappings older than this are treated as stale vs a newer CallLogs shell. */
const PHONE_MAP_FRESH_MS = Number(process.env.TELNYX_PHONE_MAP_FRESH_MS || 90_000);

function normalizeCallId(callId) {
    if (!callId) return null;
    return String(callId).replace(/^cid_/, "");
}

function normalizePhone(num) {
    if (!num) return null;
    const s = String(num).replace(/\D/g, "");
    if (s.length === 12 && s.startsWith("91")) return s.slice(2);
    if (s.length === 10) return s;
    return s;
}

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

function createdAtMs(doc) {
    if (!doc?.createdAt) return 0;
    const t = new Date(doc.createdAt).getTime();
    return Number.isFinite(t) ? t : 0;
}

async function findRecentDialerCallLogByTo({ to, collectionName, maxAgeMs = 30 * 60 * 1000 }) {
    const variants = phoneLookupVariants(to);
    if (!variants.length) return null;
    const { getDb } = require("../db");
    const db = getDb();
    const coll = collectionName || process.env.CALLLOGS_COLLECTION || "CallLogs";
    const since = new Date(Date.now() - maxAgeMs);
    const sinceIso = since.toISOString();

    // Prefer shells that are not yet bound to a Telnyx control id (wizard/test insert).
    const unbound = await db.collection(coll).findOne(
        {
            $and: [
                {
                    $or: [
                        { to_number: { $in: variants } },
                        { phone_number: { $in: variants } },
                        { contact_phone: { $in: variants } },
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
                {
                    $or: [
                        { "telnyx.call_control_id": { $exists: false } },
                        { "telnyx.call_control_id": null },
                        { "telnyx.call_control_id": "" },
                    ],
                },
            ],
        },
        { sort: { createdAt: -1 } }
    );
    if (unbound) return unbound;

    return db.collection(coll).findOne(
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
}

function pickFromDialerDoc(doc) {
    if (!doc) return null;
    const callId = normalizeCallId(doc.call_unique_id || doc.call_id || doc.lead_id || "") || "";
    return {
        call_id: callId,
        lead_id: String(doc.lead_id || callId || ""),
        campaign_id: String(doc.campaign_id || "").trim(),
        contact_id: String(doc.contact_id || "").trim(),
        collectionName: "",
        is_test_call: doc.isTestCall === true,
        recovered_from: "recent_calllog",
    };
}

function pickFromPhoneMap(phoneMap) {
    if (!phoneMap) return null;
    const callId = normalizeCallId(phoneMap.call_id || phoneMap.call_unique_id || "") || "";
    return {
        call_id: callId,
        lead_id: String(phoneMap.lead_id || callId || ""),
        campaign_id: String(phoneMap.campaign_id || "").trim(),
        contact_id: String(phoneMap.contact_id || "").trim(),
        collectionName: phoneMap.collectionName || "",
        is_test_call: phoneMap.is_test_call === true,
        recovered_from: "outbound_phone_mapping",
        updatedAt: Number(phoneMap.updatedAt) || 0,
    };
}

/**
 * Choose dialer CallLog vs Redis phone map. Never let a stale phone map win over a
 * newer CallLogs shell for the same callee.
 */
function chooseRecoverySource(dialerDoc, phoneMap) {
    const fromDialer = pickFromDialerDoc(dialerDoc);
    const fromPhone = pickFromPhoneMap(phoneMap);

    if (fromDialer?.campaign_id && fromDialer?.contact_id) {
        if (!fromPhone?.campaign_id) return fromDialer;

        const phoneAge = fromPhone.updatedAt ? Date.now() - fromPhone.updatedAt : Number.POSITIVE_INFINITY;
        const phoneFresh = phoneAge <= PHONE_MAP_FRESH_MS;
        const dialerMs = createdAtMs(dialerDoc);
        const phoneMs = fromPhone.updatedAt || 0;

        // Same contact — prefer dialer for dialer call_id, else phone if fresher.
        if (String(fromDialer.contact_id) === String(fromPhone.contact_id)) {
            if (fromDialer.call_id) return fromDialer;
            return phoneFresh ? { ...fromPhone, call_id: fromPhone.call_id || fromDialer.call_id } : fromDialer;
        }

        // Different contact: CallLog newer or phone map stale → CallLog wins.
        if (!phoneFresh || dialerMs >= phoneMs - 2000) {
            return fromDialer;
        }
        return fromPhone;
    }

    if (fromPhone?.campaign_id && fromPhone?.contact_id) {
        const phoneAge = fromPhone.updatedAt ? Date.now() - fromPhone.updatedAt : Number.POSITIVE_INFINITY;
        if (phoneAge <= PHONE_MAP_FRESH_MS) return fromPhone;
        // Stale phone map alone — still better than nothing, but mark it.
        return { ...fromPhone, recovered_from: "outbound_phone_mapping_stale" };
    }

    return fromDialer || fromPhone || null;
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

    const {
        lookupTelnyxCallControlMapping,
        lookupMappingByPhone,
        registerTelnyxCallControlMapping,
    } = require("../callMapping");

    let mapping = await lookupTelnyxCallControlMapping(sid);

    const dialerDoc = to ? await findRecentDialerCallLogByTo({ to, collectionName }) : null;
    const phoneMap = to ? await lookupMappingByPhone(to) : null;
    const recovered = chooseRecoverySource(dialerDoc, phoneMap);

    // Complete Redis mapping is trusted only when it matches the newest dialer shell
    // (or no shell exists yet). Prevents a stale phone-map write on call.initiated
    // from locking hangup to the wrong campaign after the CallLogs shell appears.
    if (mapping?.campaign_id && mapping?.contact_id && mapping?.call_id) {
        const dialerCallId = normalizeCallId(
            dialerDoc?.call_unique_id || dialerDoc?.call_id || dialerDoc?.lead_id || ""
        );
        const sameDialer =
            !dialerCallId || String(dialerCallId) === String(mapping.call_id);
        const sameContact =
            !dialerDoc?.contact_id ||
            String(dialerDoc.contact_id) === String(mapping.contact_id);
        if (sameDialer && sameContact) {
            return mapping;
        }
        // Conflict: prefer recovered dialer shell below.
    }

    if (!recovered && mapping?.campaign_id && mapping?.contact_id) {
        return mapping;
    }
    if (!recovered) return mapping;

    // If Redis telnyx map exists but lacks call_id / is stale, merge recovered dialer id.
    const callId = normalizeCallId(recovered.call_id || mapping?.call_id || "") || "";
    const campaignId = String(recovered.campaign_id || mapping?.campaign_id || "").trim();
    const contactId = String(recovered.contact_id || mapping?.contact_id || "").trim();
    const leadId = String(recovered.lead_id || callId || mapping?.lead_id || "").trim();
    const isTest = recovered.is_test_call === true || mapping?.is_test_call === true;

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
        collectionName: collectionName || recovered.collectionName || "",
        is_test_call: isTest,
        recovered_from: recovered.recovered_from || "partial",
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
    chooseRecoverySource,
    PHONE_MAP_FRESH_MS,
};
