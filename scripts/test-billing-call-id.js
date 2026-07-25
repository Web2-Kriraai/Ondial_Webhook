/**
 * Offline checks for the billing identity shared with the Calling_system1 worker (no Mongo/Redis).
 * Run: node scripts/test-billing-call-id.js
 */
const assert = require("assert");
const {
    resolveDialerCallId,
    buildBillingCallLogFilter,
} = require("../lib/resolveBillingCallId");

const SID = "CA1234567890abcdef1234567890abcdef";
const CCID = "v3:abcdEFGH1234";
const DIALER = "1774064469.3997258";

function run() {
    // The Redis mapping written by the worker is the most trustworthy source.
    assert.strictEqual(
        resolveDialerCallId({
            carrierCallId: SID,
            mapping: { call_id: DIALER },
            doc: { call_id: SID },
        }),
        DIALER
    );

    // Mapping expired: fall back to the log doc's call_unique_id.
    assert.strictEqual(
        resolveDialerCallId({
            carrierCallId: CCID,
            mapping: null,
            doc: { call_unique_id: DIALER, call_id: CCID },
        }),
        DIALER
    );

    // Webhook body may carry it when Redis is cold.
    assert.strictEqual(
        resolveDialerCallId({ carrierCallId: SID, body: { call_unique_id: DIALER } }),
        DIALER
    );

    // No dialer id anywhere — caller must fall back to the carrier id itself.
    assert.strictEqual(resolveDialerCallId({ carrierCallId: SID, doc: {} }), "");

    // call_id echoing the carrier id is the "mapping was missing" upsert value, not a dialer id.
    assert.strictEqual(
        resolveDialerCallId({ carrierCallId: SID, doc: { call_id: SID } }),
        ""
    );
    assert.strictEqual(
        resolveDialerCallId({ carrierCallId: CCID, mapping: { call_id: `  ${CCID}  ` } }),
        ""
    );

    // Empty-string mapping values must not shadow the doc.
    assert.strictEqual(
        resolveDialerCallId({
            carrierCallId: SID,
            mapping: { call_id: "" },
            doc: { call_id: DIALER },
        }),
        DIALER
    );

    // The log doc stays findable by carrier id even though billing is keyed on the dialer id.
    assert.deepStrictEqual(
        buildBillingCallLogFilter({
            carrierField: "twilio.call_sid",
            carrierCallId: SID,
            dialerCallId: DIALER,
        }),
        {
            $or: [
                { "twilio.call_sid": SID },
                { call_id: DIALER },
                { lead_id: DIALER },
            ],
        }
    );

    assert.deepStrictEqual(
        buildBillingCallLogFilter({
            carrierField: "telnyx.call_control_id",
            carrierCallId: CCID,
            dialerCallId: "",
        }),
        { $or: [{ "telnyx.call_control_id": CCID }] }
    );

    console.log(
        "OK — billing call id tests passed (dialer call_unique_id preferred over CallSid/call_control_id)"
    );
}

run();
