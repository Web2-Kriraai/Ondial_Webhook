/**
 * Unit tests for chooseRecoverySource — stale phone map must not beat a newer CallLogs shell.
 * Run: node scripts/test-telnyx-mapping-fallback.js
 */
const assert = require("assert");
const { chooseRecoverySource } = require("../lib/resolveTelnyxMappingFallback");

let passed = 0;
let failed = 0;
function check(name, fn) {
    try {
        fn();
        console.log(`PASS  ${name}`);
        passed += 1;
    } catch (err) {
        console.log(`FAIL  ${name}\n        ${err.message}`);
        failed += 1;
    }
}

const now = Date.now();

check("newer CallLog wins over stale phone map (different contact)", () => {
    const dialer = {
        call_unique_id: "new-dialer-uuid",
        campaign_id: "campaign-new",
        contact_id: "contact-new",
        isTestCall: true,
        createdAt: new Date(now - 5_000).toISOString(),
    };
    const phone = {
        call_id: "old-dialer-uuid",
        campaign_id: "campaign-old",
        contact_id: "contact-old",
        updatedAt: now - 300_000, // 5 min ago — stale
    };
    const got = chooseRecoverySource(dialer, phone);
    assert.strictEqual(got.campaign_id, "campaign-new");
    assert.strictEqual(got.contact_id, "contact-new");
    assert.strictEqual(got.call_id, "new-dialer-uuid");
    assert.strictEqual(got.recovered_from, "recent_calllog");
    assert.strictEqual(got.is_test_call, true);
});

check("fresh phone map with same contact keeps dialer call_id when present", () => {
    const dialer = {
        call_unique_id: "dialer-uuid",
        campaign_id: "c1",
        contact_id: "ct1",
        createdAt: new Date(now - 2_000).toISOString(),
    };
    const phone = {
        call_id: "dialer-uuid",
        campaign_id: "c1",
        contact_id: "ct1",
        updatedAt: now - 1_000,
    };
    const got = chooseRecoverySource(dialer, phone);
    assert.strictEqual(got.call_id, "dialer-uuid");
    assert.strictEqual(got.recovered_from, "recent_calllog");
});

check("phone map alone when no CallLog", () => {
    const phone = {
        call_id: "only-phone",
        campaign_id: "c-phone",
        contact_id: "ct-phone",
        updatedAt: now - 10_000,
    };
    const got = chooseRecoverySource(null, phone);
    assert.strictEqual(got.campaign_id, "c-phone");
    assert.ok(String(got.recovered_from).startsWith("outbound_phone_mapping"));
});

check("CallLog alone when no phone map", () => {
    const dialer = {
        call_unique_id: "only-dialer",
        campaign_id: "c-d",
        contact_id: "ct-d",
        createdAt: new Date().toISOString(),
    };
    const got = chooseRecoverySource(dialer, null);
    assert.strictEqual(got.call_id, "only-dialer");
    assert.strictEqual(got.recovered_from, "recent_calllog");
});

check("completed dialer shell is ignored by pickFromDialerDoc", () => {
    const { isTerminalDialerShell, chooseRecoverySource } = require("../lib/resolveTelnyxMappingFallback");
    assert.strictEqual(
        isTerminalDialerShell({ status: "completed", campaign_id: "c", contact_id: "x" }),
        true
    );
    const got = chooseRecoverySource(
        {
            call_unique_id: "old",
            campaign_id: "old-c",
            contact_id: "old-ct",
            status: "completed",
            createdAt: new Date().toISOString(),
        },
        {
            call_id: "fresh",
            campaign_id: "new-c",
            contact_id: "new-ct",
            updatedAt: Date.now() - 1000,
        }
    );
    assert.strictEqual(got.campaign_id, "new-c");
    assert.strictEqual(got.recovered_from, "outbound_phone_mapping");
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
