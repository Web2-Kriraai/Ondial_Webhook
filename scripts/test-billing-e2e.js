/**
 * Integration test for the webhook <-> worker billing idempotency contract.
 *
 * Runs the REAL billing code against the REAL Mongo server, but inside a throwaway
 * database that is dropped at the end, so no live data is touched.
 *
 * Proves: the webhook now bills under the dialer's call_unique_id, which is the same
 * billingKey the Calling_system1 worker computes, so the worker's pre-billed guard sees it
 * and the call is not charged twice.
 *
 * Run: node scripts/tmp-e2e-billing.js
 */
require("dotenv").config();

const assert = require("assert");
const { MongoClient, ObjectId } = require("mongodb");

const PROBE_DB = "ondial_e2e_probe_billing";

function withDbName(uri, dbName) {
    const at = uri.lastIndexOf("@");
    const qm = uri.indexOf("?", at === -1 ? 0 : at);
    const query = qm === -1 ? "" : uri.slice(qm);
    const beforeQuery = qm === -1 ? uri : uri.slice(0, qm);
    const slash = beforeQuery.indexOf("/", at === -1 ? 0 : at);
    const base = slash === -1 ? beforeQuery : beforeQuery.slice(0, slash);
    return `${base}/${dbName}${query}`;
}

const realUri = String(process.env.MONGODB_URI || "").trim();
if (!realUri) {
    console.error("MONGODB_URI not set");
    process.exit(1);
}
// Must be set before ../db is required — it reads MONGODB_URI at import time.
process.env.MONGODB_URI = withDbName(realUri, PROBE_DB);
process.env.CALLLOGS_COLLECTION = "CallLogs";
process.env.ONDIAL_CREDIT_DEDUCTION_ENABLED = "1";

const { connectDB } = require("../db");
const { maybeDeductTelnyxCallCredits } = require("../lib/telnyxCallBilling");
const { maybeDeductTwilioCallCredits } = require("../lib/twilioCallBilling");

const DIALER_ID = "1774064469.3997258";
const CALL_CONTROL_ID = "v3:e2eProbeCallControlId";
const CALL_SID = "CAe2eprobe0000000000000000000001";
const START_CREDITS = 100;

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

async function seed(db, { userId, campaignId, contactId, carrierField, carrierId }) {
    await db.collection("users").insertOne({
        _id: userId,
        email: `e2e-probe-${userId}@example.invalid`,
        credits: START_CREDITS,
        creditPlan: { currentTier: "A" },
    });
    await db.collection("campaigns").insertOne({
        _id: campaignId,
        name: "E2E Probe Campaign",
        userId,
        selectedVoice: { tier: "standard" },
        selectedPhoneNumber: "+15551230000",
        companyCountryIso: "US",
    });
    await db.collection("CallLogs").insertOne({
        lead_id: DIALER_ID,
        call_id: DIALER_ID,
        call_unique_id: DIALER_ID,
        campaign_id: String(campaignId),
        contact_id: String(contactId),
        userId,
        from_number: "+15551230000",
        to_number: "+15557654321",
        [carrierField]: carrierId,
        call_data: { events: [] },
        createdAt: new Date().toISOString(),
    });
}

/** Replica of the worker's pre-billed guard (Calling_system1/worker-service/worker.js). */
function workerPreBilledQuery({ campaignId, dialerCallUniqueId, carrierIds }) {
    const billingKey = `${String(campaignId)}:call:${dialerCallUniqueId}`;
    const or = [{ type: "call_deduction", "reference.billingKey": billingKey }];
    if (dialerCallUniqueId) {
        or.push({ type: "call_deduction", "reference.callId": dialerCallUniqueId });
        or.push({
            type: "call_deduction",
            "reference.billingKey": `${String(campaignId)}:call:${dialerCallUniqueId}`,
        });
    }
    for (const raw of carrierIds || []) {
        const id = raw ? String(raw).trim() : "";
        if (!id) continue;
        or.push({ type: "call_deduction", "reference.callId": id });
        or.push({
            type: "call_deduction",
            "reference.billingKey": `${String(campaignId)}:call:${id}`,
        });
    }
    return { $or: or };
}

async function scenarioTelnyxWithMapping(db) {
    console.log("\n--- Telnyx hangup, worker mapping present (the normal path) ---");
    const userId = new ObjectId();
    const campaignId = new ObjectId();
    const contactId = new ObjectId();
    await seed(db, {
        userId,
        campaignId,
        contactId,
        carrierField: "telnyx",
        carrierId: undefined,
    });
    await db.collection("CallLogs").updateOne(
        { call_id: DIALER_ID },
        { $set: { telnyx: { call_control_id: CALL_CONTROL_ID, status: "completed", duration: 42 } } }
    );

    const result = await maybeDeductTelnyxCallCredits({
        callControlId: CALL_CONTROL_ID,
        durationSec: 42,
        telnyxMapping: {
            call_id: DIALER_ID,
            campaign_id: String(campaignId),
            contact_id: String(contactId),
        },
        body: {},
        collectionName: "CallLogs",
    });

    check("charge succeeds", () => assert.strictEqual(result.outcome, "deducted"));

    const txs = await db
        .collection("credittransactions")
        .find({ type: "call_deduction" })
        .toArray();
    check("exactly one transaction written", () => assert.strictEqual(txs.length, 1));
    check("billingKey uses the dialer call_unique_id, not call_control_id", () =>
        assert.strictEqual(txs[0]?.reference?.billingKey, `${campaignId}:call:${DIALER_ID}`)
    );
    check("reference.callId is the dialer id", () =>
        assert.strictEqual(txs[0]?.reference?.callId, DIALER_ID)
    );

    const worker = await db
        .collection("credittransactions")
        .findOne(
            workerPreBilledQuery({
                campaignId,
                dialerCallUniqueId: DIALER_ID,
                carrierIds: [CALL_CONTROL_ID],
            })
        );
    check("worker's pre-billed guard FINDS the webhook charge (no double charge)", () =>
        assert.ok(worker, "worker guard did not match — the call would be billed twice")
    );

    const rerun = await maybeDeductTelnyxCallCredits({
        callControlId: CALL_CONTROL_ID,
        durationSec: 42,
        telnyxMapping: {
            call_id: DIALER_ID,
            campaign_id: String(campaignId),
            contact_id: String(contactId),
        },
        body: {},
        collectionName: "CallLogs",
    });
    check("replayed webhook does not charge again", () =>
        assert.strictEqual(rerun.outcome, "already_billed")
    );

    const after = await db.collection("users").findOne({ _id: userId });
    check("user charged exactly once", () =>
        assert.ok(
            after.credits < START_CREDITS &&
                Math.abs(START_CREDITS - after.credits - txs[0].amount * -1) < 1e-6,
            `credits=${after.credits}, tx amount=${txs[0].amount}`
        )
    );
    const log = await db.collection("CallLogs").findOne({ call_id: DIALER_ID });
    check("CallLog flagged creditsDeducted=true", () =>
        assert.strictEqual(log?.creditsDeducted, true)
    );

    await db.collection("credittransactions").deleteMany({});
    await db.collection("CallLogs").deleteMany({});
    await db.collection("users").deleteMany({});
    await db.collection("campaigns").deleteMany({});
    await db.collection("analytics").deleteMany({});
}

async function scenarioTelnyxMappingLost(db) {
    console.log("\n--- Telnyx hangup, Redis mapping expired (fallback path) ---");
    const userId = new ObjectId();
    const campaignId = new ObjectId();
    const contactId = new ObjectId();
    await db.collection("users").insertOne({
        _id: userId,
        email: `e2e-probe-${userId}@example.invalid`,
        credits: START_CREDITS,
        creditPlan: { currentTier: "A" },
    });
    await db.collection("campaigns").insertOne({
        _id: campaignId,
        name: "E2E Probe Campaign",
        userId,
        selectedVoice: { tier: "standard" },
        selectedPhoneNumber: "+15551230000",
        companyCountryIso: "US",
    });
    // No dialer id anywhere: call_id echoes the carrier id, as the upsert does when unmapped.
    await db.collection("CallLogs").insertOne({
        lead_id: CALL_CONTROL_ID,
        call_id: CALL_CONTROL_ID,
        campaign_id: String(campaignId),
        contact_id: String(contactId),
        userId,
        from_number: "+15551230000",
        telnyx: { call_control_id: CALL_CONTROL_ID, status: "completed", duration: 30 },
        call_data: { events: [] },
        createdAt: new Date().toISOString(),
    });

    const result = await maybeDeductTelnyxCallCredits({
        callControlId: CALL_CONTROL_ID,
        durationSec: 30,
        telnyxMapping: null,
        body: { campaign_id: String(campaignId), contact_id: String(contactId) },
        collectionName: "CallLogs",
    });
    check("still charges when the mapping is gone", () =>
        assert.strictEqual(result.outcome, "deducted")
    );

    const tx = await db.collection("credittransactions").findOne({ type: "call_deduction" });
    check("falls back to call_control_id as billing identity", () =>
        assert.strictEqual(tx?.reference?.billingKey, `${campaignId}:call:${CALL_CONTROL_ID}`)
    );

    const worker = await db.collection("credittransactions").findOne(
        workerPreBilledQuery({
            campaignId,
            dialerCallUniqueId: DIALER_ID,
            carrierIds: [CALL_CONTROL_ID],
        })
    );
    check("worker guard still finds it via the carrier-id branch", () =>
        assert.ok(worker, "worker guard missed the fallback key — double charge")
    );

    await db.collection("credittransactions").deleteMany({});
    await db.collection("CallLogs").deleteMany({});
    await db.collection("users").deleteMany({});
    await db.collection("campaigns").deleteMany({});
    await db.collection("analytics").deleteMany({});
}

async function scenarioTwilio(db) {
    console.log("\n--- Twilio completed, worker mapping present ---");
    const userId = new ObjectId();
    const campaignId = new ObjectId();
    const contactId = new ObjectId();
    await db.collection("users").insertOne({
        _id: userId,
        email: `e2e-probe-${userId}@example.invalid`,
        credits: START_CREDITS,
        creditPlan: { currentTier: "A" },
    });
    await db.collection("campaigns").insertOne({
        _id: campaignId,
        name: "E2E Probe Campaign",
        userId,
        selectedVoice: { tier: "standard" },
        selectedPhoneNumber: "+15551230000",
        companyCountryIso: "US",
    });
    await db.collection("CallLogs").insertOne({
        lead_id: DIALER_ID,
        call_id: DIALER_ID,
        call_unique_id: DIALER_ID,
        campaign_id: String(campaignId),
        contact_id: String(contactId),
        userId,
        from_number: "+15551230000",
        twilio: { call_sid: CALL_SID, status: "completed", duration: 55 },
        call_data: { events: [] },
        createdAt: new Date().toISOString(),
    });

    const result = await maybeDeductTwilioCallCredits({
        callSid: CALL_SID,
        durationSec: 55,
        twilioMapping: {
            call_id: DIALER_ID,
            campaign_id: String(campaignId),
            contact_id: String(contactId),
        },
        body: {},
        collectionName: "CallLogs",
    });
    check("charge succeeds", () => assert.strictEqual(result.outcome, "deducted"));

    const tx = await db.collection("credittransactions").findOne({ type: "call_deduction" });
    check("billingKey uses the dialer call_unique_id, not the CallSid", () =>
        assert.strictEqual(tx?.reference?.billingKey, `${campaignId}:call:${DIALER_ID}`)
    );

    const worker = await db.collection("credittransactions").findOne(
        workerPreBilledQuery({
            campaignId,
            dialerCallUniqueId: DIALER_ID,
            carrierIds: [CALL_SID],
        })
    );
    check("worker's pre-billed guard FINDS the webhook charge (no double charge)", () =>
        assert.ok(worker, "worker guard did not match — the call would be billed twice")
    );

    // What the old code did: key on the CallSid while the worker keys on the dialer id.
    const oldKeyOnly = await db.collection("credittransactions").findOne({
        type: "call_deduction",
        "reference.billingKey": `${campaignId}:call:${DIALER_ID}`,
    });
    check("regression witness: dialer-keyed lookup matches (it would not have before)", () =>
        assert.ok(oldKeyOnly)
    );
}

(async () => {
    console.log(`--- Billing E2E against throwaway db "${PROBE_DB}" ---`);
    await connectDB();
    const client = new MongoClient(process.env.MONGODB_URI, { serverSelectionTimeoutMS: 15000 });
    await client.connect();
    const db = client.db();

    try {
        await db.collection("credittransactions").createIndex(
            { type: 1, "reference.billingKey": 1 },
            {
                unique: true,
                partialFilterExpression: {
                    type: "call_deduction",
                    "reference.billingKey": { $exists: true, $type: "string" },
                },
            }
        );
        await scenarioTelnyxWithMapping(db);
        await scenarioTelnyxMappingLost(db);
        await scenarioTwilio(db);
    } catch (err) {
        console.error("\nHARNESS ERROR:", err.stack || err.message);
        failed += 1;
    } finally {
        await db.dropDatabase().catch((e) => console.error("cleanup failed:", e.message));
        console.log(`\nDropped throwaway db "${PROBE_DB}".`);
        await client.close().catch(() => {});
    }

    console.log(`\n--- ${passed} passed, ${failed} failed ---`);
    process.exit(failed ? 1 : 0);
})();
