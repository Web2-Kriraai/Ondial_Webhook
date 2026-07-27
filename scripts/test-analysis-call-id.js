/**
 * Offline checks for analysis call id resolution (Twilio CallSid / Telnyx dialer UUID).
 * Run: node scripts/test-analysis-call-id.js
 */
const assert = require("assert");
const {
    resolveLegacyAnalysisCallId,
    resolveTwilioCallSidForAnalysis,
    resolveTelnyxDialerCallIdForAnalysis,
    isTelnyxCallControlId,
} = require("../lib/resolveLegacyAnalysisCallId");

const SID = "CAdb4019c83c01ad7a24db5735918ca543";
const UUID = "04b7df0f-10e4-476d-babe-c0b8e9aedd6b";
const TELNYX = "v3:EnxLtuHjstvQAwI-Ty5KgF5o_AdcGttZkdOBE5PS-Gyw7M5BOGexRg";

function run() {
    assert.strictEqual(
        resolveTwilioCallSidForAnalysis(
            {
                twilio: { call_sid: SID, external_call_id: UUID },
                lead_id: `twilio:${SID}`,
                call_id: SID,
                call_unique_id: UUID,
            },
            SID
        ),
        SID
    );

    assert.strictEqual(
        resolveLegacyAnalysisCallId(
            {
                twilio: { call_sid: SID, external_call_id: UUID },
                lead_id: `twilio:${SID}`,
                call_id: SID,
                call_unique_id: UUID,
            },
            SID
        ),
        SID
    );

    assert.strictEqual(
        resolveLegacyAnalysisCallId(
            {
                call_unique_id: UUID,
                call_id: UUID,
                lead_id: UUID,
            },
            UUID
        ),
        UUID
    );

    assert.strictEqual(resolveLegacyAnalysisCallId(null, SID), SID);

    assert.strictEqual(isTelnyxCallControlId(TELNYX), true);
    assert.strictEqual(isTelnyxCallControlId(SID), false);
    assert.strictEqual(
        resolveTelnyxDialerCallIdForAnalysis(
            {
                call_unique_id: UUID,
                call_id: TELNYX,
                telnyx: { call_control_id: TELNYX, external_call_id: UUID },
            },
            TELNYX
        ),
        UUID
    );
    assert.strictEqual(
        resolveLegacyAnalysisCallId(
            {
                call_unique_id: UUID,
                call_id: TELNYX,
                telnyx: { call_control_id: TELNYX, external_call_id: UUID },
            },
            TELNYX
        ),
        UUID
    );

    console.log("OK — analysis call id resolution (Twilio CallSid / Telnyx dialer UUID)");
}

run();
