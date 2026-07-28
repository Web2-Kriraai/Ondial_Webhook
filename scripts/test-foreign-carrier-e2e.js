/**
 * E2E: foreign Twilio + Telnyx CallLogs — TEST and NORMAL (live) paths.
 *
 * Proves carrier-primary identity (one doc per CallSid / call_control_id):
 *  - status can arrive BEFORE mapping
 *  - optional UUID-only dialer shell is merged away (no Initial + fake Retry)
 *  - is_test_call true → isTestCall on doc; false → NORMAL
 *
 * Throwaway DB only. Run:
 *   node scripts/test-foreign-carrier-e2e.js
 */
require("dotenv").config({ quiet: true });

const assert = require("assert");
const crypto = require("crypto");
const { MongoClient, ObjectId } = require("mongodb");

const PROBE_DB = "ondial_e2e_foreign_carrier";
const PORT = process.env.E2E_FOREIGN_PORT || "9103";
const BASE = `http://127.0.0.1:${PORT}`;
const SECRET = "e2e-foreign-carrier-secret";

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
const probeUri = withDbName(realUri, PROBE_DB);

process.env.MONGODB_URI = probeUri;
process.env.PORT = PORT;
process.env.WEBHOOK_INTERNAL_SECRET = SECRET;
process.env.CALLLOGS_COLLECTION = "CallLogs";
process.env.ONDIAL_CREDIT_DEDUCTION_ENABLED = "0";
process.env.ANALYSIS_API_URL = "";
process.env.ONDIAL_SKIP_CREDIT_TEST_CALLS = "1";

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

async function postJson(path, body) {
    const res = await fetch(`${BASE}${path}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
    });
    const text = await res.text();
    let json = null;
    try {
        json = JSON.parse(text);
    } catch {
        /* ignore */
    }
    return { status: res.status, json, text };
}

async function postForm(path, fields) {
    const body = new URLSearchParams();
    for (const [k, v] of Object.entries(fields)) {
        if (v != null) body.set(k, String(v));
    }
    const res = await fetch(`${BASE}${path}`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: body.toString(),
    });
    const text = await res.text();
    let json = null;
    try {
        json = JSON.parse(text);
    } catch {
        /* ignore */
    }
    return { status: res.status, json, text };
}

function telnyxEvent(eventType, ccid, dialerId, extra = {}) {
    return {
        data: {
            record_type: "event",
            id: `${eventType}-${dialerId}-${Math.random().toString(16).slice(2, 8)}`,
            event_type: eventType,
            occurred_at: new Date().toISOString(),
            payload: {
                call_control_id: ccid,
                call_leg_id: `leg-${dialerId}`,
                call_session_id: `sess-${dialerId}`,
                connection_id: "e2e-connection",
                direction: "outgoing",
                from: "+15551230000",
                to: "+15557654321",
                state: "parked",
                ...extra,
            },
        },
        meta: { attempt: 1, delivered_to: `${BASE}/telnyx/webhooks` },
    };
}

async function waitFor(fn, { timeoutMs = 20000, everyMs = 400, label = "condition" } = {}) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const v = await fn();
        if (v) return v;
        await new Promise((r) => setTimeout(r, everyMs));
    }
    throw new Error(`timed out waiting for ${label}`);
}

async function countByCarrier(db, field, id) {
    return db.collection("CallLogs").countDocuments({ [field]: id });
}

(async () => {
    console.log(`--- Foreign carrier E2E (TEST + NORMAL) db=${PROBE_DB} port=${PORT} ---\n`);
    require("../index.js");

    const client = new MongoClient(probeUri, { serverSelectionTimeoutMS: 15000 });
    await client.connect();
    const db = client.db();

    try {
        await waitFor(
            async () => {
                try {
                    const r = await fetch(`${BASE}/health`);
                    return r.ok ? true : null;
                } catch {
                    return null;
                }
            },
            { label: "/health", timeoutMs: 45000 }
        );
        check("service up", () => assert.ok(true));

        // ─── Twilio TEST: status first + UUID shell race ─────────────────────
        console.log("\n=== Twilio TEST (status-first + UUID shell race) ===");
        {
            const dialerId = crypto.randomUUID();
            const sid = `CA${Date.now().toString(16)}test${Math.random().toString(16).slice(2, 10)}`;
            const campaignId = new ObjectId();
            const contactId = new ObjectId();

            // Simulate Ondial UUID-only shell racing ahead (old bug path).
            await db.collection("CallLogs").insertOne({
                lead_id: dialerId,
                call_id: dialerId,
                call_unique_id: dialerId,
                campaign_id: String(campaignId),
                contact_id: String(contactId),
                isTestCall: true,
                to_number: "+15557654321",
                call_data: { events: [] },
                createdAt: new Date().toISOString(),
            });

            const st1 = await postForm("/twilio/call-status", {
                CallSid: sid,
                CallStatus: "initiated",
                Timestamp: new Date().toISOString(),
                Direction: "outbound-api",
                From: "+15551230000",
                To: "+15557654321",
            });
            check("Twilio TEST status initiated 2xx", () =>
                assert.ok(st1.status >= 200 && st1.status < 300, `status=${st1.status}`)
            );

            await waitFor(
                () => db.collection("CallLogs").findOne({ "twilio.call_sid": sid }),
                { label: "Twilio CallSid doc after status" }
            );

            const map = await postJson("/api/twilio-mapping", {
                twilio_call_sid: sid,
                CallSid: sid,
                call_id: dialerId,
                lead_id: dialerId,
                campaign_id: String(campaignId),
                contact_id: String(contactId),
                is_test_call: true,
            });
            check("Twilio TEST mapping 200", () => assert.strictEqual(map.status, 200));
            await new Promise((r) => setTimeout(r, 800));

            const n = await countByCarrier(db, "twilio.call_sid", sid);
            const doc = await db.collection("CallLogs").findOne({ "twilio.call_sid": sid });
            const uuidShells = await db.collection("CallLogs").countDocuments({
                lead_id: dialerId,
                $or: [
                    { "twilio.call_sid": { $exists: false } },
                    { "twilio.call_sid": null },
                    { "twilio.call_sid": "" },
                ],
            });

            check("Twilio TEST → exactly 1 CallLog for CallSid", () => assert.strictEqual(n, 1));
            check("Twilio TEST → UUID-only shell gone (merged)", () => assert.strictEqual(uuidShells, 0));
            check("Twilio TEST → lead_id is dialer UUID", () =>
                assert.strictEqual(doc.lead_id, dialerId)
            );
            check("Twilio TEST → call_id is dialer UUID", () =>
                assert.strictEqual(doc.call_id, dialerId)
            );
            check("Twilio TEST → isTestCall true", () => assert.strictEqual(doc.isTestCall, true));

            await postForm("/twilio/call-status", {
                CallSid: sid,
                CallStatus: "completed",
                CallDuration: "12",
                Timestamp: new Date().toISOString(),
            });
            await new Promise((r) => setTimeout(r, 600));
            const n2 = await countByCarrier(db, "twilio.call_sid", sid);
            check("Twilio TEST completed still 1 CallLog", () => assert.strictEqual(n2, 1));
        }

        // ─── Twilio NORMAL (live) ────────────────────────────────────────────
        console.log("\n=== Twilio NORMAL / live ===");
        {
            const dialerId = crypto.randomUUID();
            const sid = `CA${Date.now().toString(16)}live${Math.random().toString(16).slice(2, 10)}`;
            const campaignId = new ObjectId();
            const contactId = new ObjectId();

            await postForm("/twilio/call-status", {
                CallSid: sid,
                CallStatus: "initiated",
                Timestamp: new Date().toISOString(),
                From: "+15551230000",
                To: "+15557654321",
            });
            await waitFor(
                () => db.collection("CallLogs").findOne({ "twilio.call_sid": sid }),
                { label: "Twilio live CallSid doc" }
            );

            await postJson("/api/twilio-mapping", {
                twilio_call_sid: sid,
                CallSid: sid,
                call_id: dialerId,
                lead_id: dialerId,
                campaign_id: String(campaignId),
                contact_id: String(contactId),
                is_test_call: false,
            });
            await new Promise((r) => setTimeout(r, 800));

            const doc = await db.collection("CallLogs").findOne({ "twilio.call_sid": sid });
            const n = await countByCarrier(db, "twilio.call_sid", sid);
            check("Twilio NORMAL → 1 CallLog", () => assert.strictEqual(n, 1));
            check("Twilio NORMAL → dialer UUID on lead_id", () =>
                assert.strictEqual(doc.lead_id, dialerId)
            );
            check("Twilio NORMAL → isTestCall not true", () =>
                assert.ok(doc.isTestCall !== true, `isTestCall=${doc.isTestCall}`)
            );
        }

        // ─── Telnyx TEST: status first + UUID shell ─────────────────────────
        console.log("\n=== Telnyx TEST (status-first + UUID shell race) ===");
        {
            const dialerId = crypto.randomUUID();
            const ccid = `v3:e2eTelnyxTest${Date.now()}`;
            const campaignId = new ObjectId();
            const contactId = new ObjectId();

            await db.collection("CallLogs").insertOne({
                lead_id: dialerId,
                call_id: dialerId,
                call_unique_id: dialerId,
                campaign_id: String(campaignId),
                contact_id: String(contactId),
                isTestCall: true,
                to_number: "+15557654321",
                call_data: { events: [] },
                createdAt: new Date().toISOString(),
            });

            const init = await postJson(
                "/telnyx/webhooks",
                telnyxEvent("call.initiated", ccid, dialerId)
            );
            check("Telnyx TEST initiated 2xx", () =>
                assert.ok(init.status >= 200 && init.status < 300, `status=${init.status}`)
            );
            await waitFor(
                () => db.collection("CallLogs").findOne({ "telnyx.call_control_id": ccid }),
                { label: "Telnyx ccid doc" }
            );

            await postJson("/api/telnyx-mapping", {
                call_control_id: ccid,
                call_id: dialerId,
                lead_id: dialerId,
                campaign_id: String(campaignId),
                contact_id: String(contactId),
                is_test_call: true,
            });
            await new Promise((r) => setTimeout(r, 800));

            const n = await countByCarrier(db, "telnyx.call_control_id", ccid);
            const doc = await db.collection("CallLogs").findOne({ "telnyx.call_control_id": ccid });
            const uuidShells = await db.collection("CallLogs").countDocuments({
                lead_id: dialerId,
                $or: [
                    { "telnyx.call_control_id": { $exists: false } },
                    { "telnyx.call_control_id": null },
                    { "telnyx.call_control_id": "" },
                ],
            });

            check("Telnyx TEST → 1 CallLog for call_control_id", () => assert.strictEqual(n, 1));
            check("Telnyx TEST → UUID-only shell gone", () => assert.strictEqual(uuidShells, 0));
            check("Telnyx TEST → lead_id dialer UUID", () => assert.strictEqual(doc.lead_id, dialerId));
            check("Telnyx TEST → isTestCall true", () => assert.strictEqual(doc.isTestCall, true));
        }

        // ─── Telnyx NORMAL ───────────────────────────────────────────────────
        console.log("\n=== Telnyx NORMAL / live ===");
        {
            const dialerId = crypto.randomUUID();
            const ccid = `v3:e2eTelnyxLive${Date.now()}`;
            const campaignId = new ObjectId();
            const contactId = new ObjectId();

            await postJson("/telnyx/webhooks", telnyxEvent("call.initiated", ccid, dialerId));
            await waitFor(
                () => db.collection("CallLogs").findOne({ "telnyx.call_control_id": ccid }),
                { label: "Telnyx live ccid doc" }
            );

            await postJson("/api/telnyx-mapping", {
                call_control_id: ccid,
                call_id: dialerId,
                lead_id: dialerId,
                campaign_id: String(campaignId),
                contact_id: String(contactId),
                is_test_call: false,
            });
            await new Promise((r) => setTimeout(r, 800));

            // Re-fire an event so upsert reapplies dialer id after mapping (mapping itself upserts too).
            await postJson(
                "/telnyx/webhooks",
                telnyxEvent("call.answered", ccid, dialerId, {
                    answer_time: new Date().toISOString(),
                    state: "answered",
                })
            );
            await new Promise((r) => setTimeout(r, 800));

            const doc = await db.collection("CallLogs").findOne({ "telnyx.call_control_id": ccid });
            const n = await countByCarrier(db, "telnyx.call_control_id", ccid);
            check("Telnyx NORMAL → 1 CallLog", () => assert.strictEqual(n, 1));
            check("Telnyx NORMAL → dialer UUID on lead_id", () =>
                assert.strictEqual(doc.lead_id, dialerId)
            );
            check("Telnyx NORMAL → isTestCall not true", () =>
                assert.ok(doc.isTestCall !== true, `isTestCall=${doc.isTestCall}`)
            );
        }
    } finally {
        try {
            await db.dropDatabase();
            console.log(`\nDropped throwaway db ${PROBE_DB}`);
        } catch (e) {
            console.warn("dropDatabase:", e.message);
        }
        await client.close();
        console.log(`\n--- ${passed} passed, ${failed} failed ---`);
        process.exit(failed ? 1 : 0);
    }
})().catch((err) => {
    console.error(err);
    process.exit(1);
});
