/**
 * Offline checks for credit idempotency hints + test-call bypass (no Mongo/Redis).
 * Run: node scripts/test-billing-guards.js
 */
const assert = require("assert");
const {
    shouldSkipCreditDeduction,
    callLogDocIsTestCall,
    creditTestBypassEnabled,
} = require("../lib/shouldSkipCreditDeduction");

function run() {
    const prev = process.env.ONDIAL_SKIP_CREDIT_TEST_CALLS;
    process.env.ONDIAL_SKIP_CREDIT_TEST_CALLS = "1";

    assert.strictEqual(creditTestBypassEnabled(), true);

    assert.strictEqual(
        shouldSkipCreditDeduction({
            callLogDoc: { isTestCall: true, campaign_id: "abc" },
        }),
        true
    );

    assert.strictEqual(
        shouldSkipCreditDeduction({
            body: { is_test_call: true },
        }),
        true
    );

    assert.strictEqual(
        shouldSkipCreditDeduction({
            mapping: { is_test_call: true },
        }),
        true
    );

    assert.strictEqual(
        shouldSkipCreditDeduction({
            collectionName: "TestCall",
        }),
        true
    );

    assert.strictEqual(
        shouldSkipCreditDeduction({
            callLogDoc: {
                campaign_id: "507f1f77bcf86cd799439011",
                call_data: {
                    events: [
                        {
                            event_type: "call_initiated",
                            data: { customParameters: { is_test_call: "true" } },
                        },
                    ],
                },
            },
        }),
        true
    );

    assert.strictEqual(
        shouldSkipCreditDeduction({
            callLogDoc: { campaign_id: "507f1f77bcf86cd799439011" },
            body: { CallStatus: "completed", CallDuration: 30 },
        }),
        false
    );

    process.env.ONDIAL_SKIP_CREDIT_TEST_CALLS = "0";
    assert.strictEqual(
        shouldSkipCreditDeduction({ callLogDoc: { isTestCall: true } }),
        false
    );

    assert.strictEqual(callLogDocIsTestCall({ isTestCall: true }), true);
    assert.strictEqual(callLogDocIsTestCall({ isTestCall: false }), false);

    if (prev === undefined) delete process.env.ONDIAL_SKIP_CREDIT_TEST_CALLS;
    else process.env.ONDIAL_SKIP_CREDIT_TEST_CALLS = prev;

    console.log("OK — billing guard tests passed (test-call bypass + production call allowed)");
}

run();
