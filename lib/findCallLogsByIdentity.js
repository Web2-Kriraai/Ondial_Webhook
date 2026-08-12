/**
 * Indexed CallLogs / TestCall identity lookups.
 *
 * Atlas Query Targeting: a single `$or` on { lead_id, call_id, call_unique_id }
 * (same UUID) scanned ~50k docs when `call_unique_id` had no index. Prefer
 * sequential equality finds on indexed fields, then carrier fields, then
 * prefixed lead_id fallbacks.
 */

function normalizeKey(value) {
    if (value == null) return "";
    const s = String(value).trim();
    return s;
}

function uniqKeys(values) {
    const out = [];
    const seen = new Set();
    for (const raw of values) {
        const k = normalizeKey(raw);
        if (!k || seen.has(k)) continue;
        seen.add(k);
        out.push(k);
    }
    return out;
}

/**
 * Merge docs by `_id` string, preserving first-seen order.
 * @param {object[][]} batches
 */
function mergeDocsById(batches) {
    const byId = new Map();
    for (const batch of batches) {
        if (!Array.isArray(batch)) continue;
        for (const doc of batch) {
            if (!doc || doc._id == null) continue;
            const id = String(doc._id);
            if (!byId.has(id)) byId.set(id, doc);
        }
    }
    return [...byId.values()];
}

/**
 * Find CallLog-like docs by dialer / carrier identity without a 3-way COLLSCAN `$or`.
 *
 * @param {import('mongodb').Collection} coll
 * @param {string|string[]|{ leadId?: string, callId?: string, callUniqueId?: string, includeCarrier?: boolean }} identity
 * @param {{ limitPerQuery?: number, includeCarrier?: boolean }} [options]
 * @returns {Promise<object[]>}
 */
async function findCallLogsByIdentity(coll, identity, options = {}) {
    if (!coll) return [];

    let keys = [];
    let includeCarrier = options.includeCarrier !== false;

    if (typeof identity === "string") {
        keys = uniqKeys([identity]);
    } else if (Array.isArray(identity)) {
        keys = uniqKeys(identity);
    } else if (identity && typeof identity === "object") {
        keys = uniqKeys([identity.leadId, identity.callId, identity.callUniqueId, identity.key]);
        if (identity.includeCarrier === false) includeCarrier = false;
    }

    if (options.includeCarrier === false) includeCarrier = false;
    if (!keys.length) return [];

    const limitPerQuery = Math.max(1, Number(options.limitPerQuery) || 8);
    const batches = [];

    // 1) Exact indexed singles (call_id, call_unique_id, lead_id)
    for (const key of keys) {
        const [byCallId, byUnique, byLead] = await Promise.all([
            coll.find({ call_id: key }).limit(limitPerQuery).toArray(),
            coll.find({ call_unique_id: key }).limit(limitPerQuery).toArray(),
            coll.find({ lead_id: key }).limit(limitPerQuery).toArray(),
        ]);
        batches.push(byCallId, byUnique, byLead);
    }

    let docs = mergeDocsById(batches);
    if (docs.length > 0 || !includeCarrier) {
        return docs;
    }

    // 2) Carrier indexed fields (only when primary miss)
    const carrierBatches = [];
    for (const key of keys) {
        const [byTwilio, byTelnyx, byTelnyxExt, byTwilioExt] = await Promise.all([
            coll.find({ "twilio.call_sid": key }).limit(limitPerQuery).toArray(),
            coll.find({ "telnyx.call_control_id": key }).limit(limitPerQuery).toArray(),
            coll.find({ "telnyx.external_call_id": key }).limit(limitPerQuery).toArray(),
            coll.find({ "twilio.external_call_id": key }).limit(limitPerQuery).toArray(),
        ]);
        carrierBatches.push(byTwilio, byTelnyx, byTelnyxExt, byTwilioExt);
    }
    docs = mergeDocsById(carrierBatches);
    if (docs.length > 0) return docs;

    // 3) Prefixed lead_id fallbacks (legacy)
    const prefixBatches = [];
    for (const key of keys) {
        const [twilioLead, telnyxLead] = await Promise.all([
            coll.find({ lead_id: `twilio:${key}` }).limit(limitPerQuery).toArray(),
            coll.find({ lead_id: `telnyx:${key}` }).limit(limitPerQuery).toArray(),
        ]);
        prefixBatches.push(twilioLead, telnyxLead);
    }
    return mergeDocsById(prefixBatches);
}

/**
 * @param {import('mongodb').Collection} coll
 * @param {Parameters<typeof findCallLogsByIdentity>[1]} identity
 * @param {Parameters<typeof findCallLogsByIdentity>[2]} [options]
 */
async function findOneCallLogByIdentity(coll, identity, options = {}) {
    const docs = await findCallLogsByIdentity(coll, identity, {
        ...options,
        limitPerQuery: options.limitPerQuery || 4,
    });
    return docs[0] || null;
}

module.exports = {
    findCallLogsByIdentity,
    findOneCallLogByIdentity,
    uniqKeys,
    normalizeKey,
};
