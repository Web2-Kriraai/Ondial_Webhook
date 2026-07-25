/**
 * Billing identity shared with the Calling_system1 worker.
 *
 * The worker builds its billingKey as `${campaignId}:call:${call_unique_id}` using the id the
 * dialer generated. If this service bills the same call under the carrier id (Twilio CallSid or
 * Telnyx call_control_id) instead, the two keys differ and the customer is charged twice.
 * So always prefer the dialer's call_unique_id and fall back to the carrier id only when the
 * Redis mapping and the log doc have both lost it.
 */

const { pickNonEmpty } = require("./customParameters");

/**
 * @param {object} opts
 * @param {string} opts.carrierCallId - Twilio CallSid or Telnyx call_control_id
 * @param {object} [opts.mapping] - Redis mapping entry written by the worker
 * @param {object} [opts.body] - webhook body
 * @param {object} [opts.doc] - CallLog document
 * @returns {string} dialer call_unique_id, or "" when unavailable
 */
function resolveDialerCallId({ carrierCallId, mapping, body, doc }) {
    const candidate = pickNonEmpty(
        mapping?.call_id,
        body?.call_unique_id,
        body?.call_id,
        doc?.call_unique_id,
        doc?.call_id
    );
    const id = candidate ? String(candidate).trim() : "";
    // The CallLog upsert writes the carrier id into call_id when no mapping existed, so a
    // match here means "no dialer id known", not a real dialer id.
    if (!id || id === String(carrierCallId || "").trim()) return "";
    return id;
}

/**
 * Mongo filter that finds the log doc by carrier id (always present) or by dialer id.
 * @param {object} opts
 * @param {string} opts.carrierField - e.g. "twilio.call_sid"
 * @param {string} opts.carrierCallId
 * @param {string} [opts.dialerCallId]
 */
function buildBillingCallLogFilter({ carrierField, carrierCallId, dialerCallId }) {
    const or = [{ [carrierField]: carrierCallId }];
    if (dialerCallId) {
        or.push({ call_id: dialerCallId }, { lead_id: dialerCallId });
    }
    return { $or: or };
}

module.exports = { resolveDialerCallId, buildBillingCallLogFilter };
