/**
 * Offline checks for provider UUID ↔ call_sid linking on inbound docs.
 * Run: node scripts/test-inbound-doc-anchor.js
 */
const assert = require("assert");
const {
    providerCallIdToCallSid,
    callSidToProviderCallId,
    inboundDocFilterFor,
    isLikelyValidateUiDoc,
    pickBestInboundDoc,
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

    const stub = {
        _id: "stub",
        call_id: PROVIDER_ID,
        call_sid: CALL_SID,
    };
    assert.deepStrictEqual(inboundDocFilterFor(stub), { call_sid: CALL_SID });

    const validate = {
        call_id: "6a23e04eda8ac5b859f845d6",
        call_sid: CALL_SID,
        source: "validate",
        userId: "68d7749bc50f55ec1dfa4198",
    };
    assert.deepStrictEqual(inboundDocFilterFor(validate), { call_id: "6a23e04eda8ac5b859f845d6" });
    assert.strictEqual(isLikelyValidateUiDoc(validate), true);
    assert.strictEqual(isLikelyValidateUiDoc(stub), false);

    const picked = pickBestInboundDoc([stub, validate]);
    assert.strictEqual(picked.call_id, validate.call_id);

    console.log("test-inbound-doc-anchor: ok");
}

run();
