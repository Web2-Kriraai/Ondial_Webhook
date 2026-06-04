/**
 * Central guard: campaign credit deduction skips wizard/test calls.
 * Set ONDIAL_SKIP_CREDIT_TEST_CALLS=0 to allow billing test rows (dev only).
 */
const { inferIsTestCallFromWebhookBody } = require("./inferTestCall");

const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";

function creditTestBypassEnabled() {
    return process.env.ONDIAL_SKIP_CREDIT_TEST_CALLS !== "0";
}

function eventMarksTestCall(events) {
    if (!Array.isArray(events)) return false;
    for (const e of events) {
        const d = e?.data;
        if (!d || typeof d !== "object") continue;
        const cp = d.customParameters || d.custom_parameters;
        if (cp?.is_test_call === true || cp?.is_test_call === "true") return true;
        if (cp?.isTestCall === true || cp?.isTestCall === "true") return true;
        if (d.is_test_call === true || d.is_test_call === "true") return true;
        if (d.isTestCall === true || d.isTestCall === "true") return true;
    }
    return false;
}

function callLogDocIsTestCall(doc) {
    if (!doc) return false;
    if (doc.isTestCall === true) return true;
    return eventMarksTestCall(doc.call_data?.events);
}

/**
 * @param {object} opts
 * @param {object} [opts.callLogDoc]
 * @param {object} [opts.body]
 * @param {object} [opts.mapping] - Redis call / twilio mapping
 * @param {string} [opts.collectionName]
 */
function shouldSkipCreditDeduction(opts = {}) {
    if (!creditTestBypassEnabled()) return false;

    const { callLogDoc, body, mapping, collectionName } = opts;

    if (collectionName && String(collectionName) === TESTCALL_COLLECTION) {
        return true;
    }
    if (mapping?.is_test_call === true) return true;
    if (inferIsTestCallFromWebhookBody(body)) return true;
    if (body?.is_test_call === true || body?.is_test_call === "true") return true;
    if (body?.isTestCall === true || body?.isTestCall === "true") return true;

    return callLogDocIsTestCall(callLogDoc);
}

module.exports = {
    creditTestBypassEnabled,
    callLogDocIsTestCall,
    eventMarksTestCall,
    shouldSkipCreditDeduction,
};
