/**
 * Offline checks for provider UUID ↔ call_sid linking on inbound docs.
 * Run: node scripts/test-inbound-doc-anchor.js
 */
const assert = require("assert");
const {
    providerCallIdToCallSid,
    callSidToProviderCallId,
} = require("../lib/inboundDocAnchor");

const PROVIDER_ID = "92bd645e-e976-4a14-969a-5176d7fed547";
const CALL_SID = "call_92bd645ee9764a14969a5176d7fed547";

function run() {
    assert.strictEqual(providerCallIdToCallSid(PROVIDER_ID), CALL_SID);
    assert.strictEqual(callSidToProviderCallId(CALL_SID), PROVIDER_ID);
    assert.strictEqual(providerCallIdToCallSid(CALL_SID), CALL_SID);
    assert.strictEqual(
        providerCallIdToCallSid("92bd645ee9764a14969a5176d7fed547"),
        CALL_SID
    );
    console.log("test-inbound-doc-anchor: ok");
}

run();
