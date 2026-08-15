/**
 * Unit checks for outbound CallLog duplicate merge scoring/event merge.
 * Run: node scripts/test-merge-outbound-duplicate-calllogs.js
 */
const assert = require("assert");
const {
    scoreOutboundCallLog,
    mergeEvents,
} = require("../lib/mergeOutboundDuplicateCallLogs");

function testScorePrefersRicherDoc() {
    const stub = {
        call_data: { events: [{ event_type: "call_initiated", timestamp: "a", data: {} }] },
    };
    const rich = {
        call_data: {
            events: [
                { event_type: "call_initiated", timestamp: "a", data: {} },
                { event_type: "call_hangup", timestamp: "b", data: {} },
            ],
            legs: [{ id: 1 }],
        },
        duration: 12,
        recordingUrl: "https://x",
    };
    assert.ok(scoreOutboundCallLog(rich) > scoreOutboundCallLog(stub));
}

function testMergeEventsDedupes() {
    const docs = [
        {
            call_data: {
                events: [{ event_type: "call_initiated", timestamp: "1", data: { a: 1 } }],
            },
        },
        {
            call_data: {
                events: [
                    { event_type: "call_initiated", timestamp: "1", data: { a: 1 } },
                    { event_type: "call_hangup", timestamp: "2", data: { b: 2 } },
                ],
            },
        },
    ];
    const merged = mergeEvents(docs);
    assert.equal(merged.length, 2);
    assert.equal(merged[0].event_type, "call_initiated");
    assert.equal(merged[1].event_type, "call_hangup");
}

testScorePrefersRicherDoc();
testMergeEventsDedupes();
console.log("ok: merge-outbound-duplicate-calllogs");
