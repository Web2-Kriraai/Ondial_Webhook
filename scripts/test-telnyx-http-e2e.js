/**
 * HTTP-level E2E for the Telnyx Call Control pipeline.
 *
 * Boots the real service in-process against a THROWAWAY database on the configured Mongo
 * server, drives spec-shaped Telnyx webhooks (call.initiated -> call.answered -> call.hangup)
 * over HTTP, then asserts on what landed in Mongo: call_id parity with the dialer, status and
 * duration, a single credit charge, retry idempotency, and /hangup authorization.
 *
 * The throwaway database is dropped at the end. No live collection is touched and the
 * connection string is only ever held in memory.
 *
 * Run: node scripts/test-telnyx-http-e2e.js
 */
require("dotenv").config();

const assert = require("assert");
const { MongoClient, ObjectId } = require("mongodb");

const PROBE_DB = "ondial_e2e_probe_http";
const PORT = process.env.E2E_PORT || "9099";
const BASE = `http://127.0.0.1:${PORT}`;
const SECRET = "e2e-internal-secret-not-a-real-credential";

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

// Must be set before ../index.js is required — it reads these at module load.
process.env.MONGODB_URI = probeUri;
process.env.PORT = PORT;
process.env.WEBHOOK_INTERNAL_SECRET = SECRET;
process.env.CALLLOGS_COLLECTION = "CallLogs";
process.env.ONDIAL_CREDIT_DEDUCTION_ENABLED = "1";
// Keep the probe from reaching out to the analysis service.
process.env.ANALYSIS_API_URL = "";

const DIALER_ID = `e2e-dialer-${Date.now()}`;
const CCID = `v3:e2eHttpProbe${Date.now()}`;
const FROM = "+15551230000";
const TO = "+15557654321";
const START = new Date(Date.now() - 60000);
const ANSWER = new Date(START.getTime() + 5000);
const END = new Date(ANSWER.getTime() + 42000);

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

async function post(path, body, headers = {}) {
    const res = await fetch(`${BASE}${path}`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...headers },
        body: JSON.stringify(body),
    });
    const text = await res.text();
    let json = null;
    try {
        json = JSON.parse(text);
    } catch {
        /* non-json */
    }
    return { status: res.status, json, text };
}

function telnyxEvent(eventType, extraPayload = {}, eventIdSuffix = "") {
    return {
        data: {
            record_type: "event",
            id: `${eventType}-${DIALER_ID}${eventIdSuffix}`,
            event_type: eventType,
            occurred_at: new Date().toISOString(),
            payload: {
                call_control_id: CCID,
                call_leg_id: `leg-${DIALER_ID}`,
                call_session_id: `sess-${DIALER_ID}`,
                connection_id: "e2e-connection",
                direction: "outgoing",
                from: FROM,
                to: TO,
                state: "parked",
                start_time: START.toISOString(),
                ...extraPayload,
            },
        },
        meta: { attempt: 1, delivered_to: `${BASE}/telnyx/webhooks` },
    };
}

async function waitFor(fn, { timeoutMs = 25000, everyMs = 500, label = "condition" } = {}) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const v = await fn();
        if (v) return v;
        await new Promise((r) => setTimeout(r, everyMs));
    }
    throw new Error(`timed out waiting for ${label}`);
}

(async () => {
    console.log(`--- Telnyx HTTP E2E against throwaway db "${PROBE_DB}" on port ${PORT} ---\n`);
    require("../index.js");

    const client = new MongoClient(probeUri, { serverSelectionTimeoutMS: 15000 });
    await client.connect();
    const db = client.db();

    const userId = new ObjectId();
    const campaignId = new ObjectId();
    const contactId = new ObjectId();

    try {
        await waitFor(
            async () => {
                try {
                    const r = await fetch(`${BASE}/health`);
                    return r.ok ? await r.json() : null;
                } catch {
                    return null;
                }
            },
            { label: "service /health", timeoutMs: 40000 }
        );
        check("service is up", () => assert.ok(true));

        await db.collection("users").insertOne({
            _id: userId,
            email: `e2e-http-${userId}@example.invalid`,
            credits: 100,
            creditPlan: { currentTier: "A" },
        });
        await db.collection("campaigns").insertOne({
            _id: campaignId,
            name: "E2E HTTP Probe",
            userId,
            selectedVoice: { tier: "standard" },
            selectedPhoneNumber: FROM,
            companyCountryIso: "US",
        });

        // The worker posts this right after the dialer returns a call_control_id.
        const mapping = await post("/api/telnyx-mapping", {
            call_control_id: CCID,
            call_id: DIALER_ID,
            campaign_id: String(campaignId),
            contact_id: String(contactId),
        });
        check("worker mapping accepted", () => assert.strictEqual(mapping.status, 200));
        await new Promise((r) => setTimeout(r, 1000));

        console.log("\n--- call.initiated ---");
        const initiated = await post("/telnyx/webhooks", telnyxEvent("call.initiated"));
        check("call.initiated acked 2xx (Telnyx will not retry)", () =>
            assert.ok(initiated.status >= 200 && initiated.status < 300, `got ${initiated.status}`)
        );

        const afterInit = await waitFor(
            () => db.collection("CallLogs").findOne({ "telnyx.call_control_id": CCID }),
            { label: "CallLog after call.initiated" }
        );
        check("CallLog created", () => assert.ok(afterInit));
        // The doc is anchored on the carrier id exactly like the Twilio path anchors on CallSid;
        // the dialer's id is carried alongside it, which is what gives worker/analysis parity.
        check("call_id anchors on the call_control_id (same shape as Twilio's CallSid)", () =>
            assert.strictEqual(afterInit.call_id, CCID)
        );
        check("call_unique_id carries the dialer id (the parity field)", () =>
            assert.strictEqual(afterInit.call_unique_id, DIALER_ID)
        );
        check("telnyx.external_call_id carries the dialer id", () =>
            assert.strictEqual(afterInit.telnyx?.external_call_id, DIALER_ID)
        );
        check("lead_id is the synthetic telnyx anchor", () =>
            assert.strictEqual(afterInit.lead_id, `telnyx:${CCID}`)
        );
        check("campaign and contact carried from the mapping", () => {
            assert.strictEqual(String(afterInit.campaign_id), String(campaignId));
            assert.strictEqual(String(afterInit.contact_id), String(contactId));
        });

        console.log("\n--- call.answered ---");
        const answered = await post(
            "/telnyx/webhooks",
            telnyxEvent("call.answered", { answer_time: ANSWER.toISOString(), state: "answered" })
        );
        check("call.answered acked 2xx", () =>
            assert.ok(answered.status >= 200 && answered.status < 300, `got ${answered.status}`)
        );
        // Wait for an answered status specifically. Waiting for "anything but initiated" races
        // against the "mapped" placeholder the mapping endpoint writes.
        const ANSWERED_STATUSES = ["answered", "in-progress"];
        const afterAns = await waitFor(
            async () => {
                const d = await db.collection("CallLogs").findOne({ "telnyx.call_control_id": CCID });
                const s = String(d?.telnyx?.status || "").toLowerCase();
                return d && ANSWERED_STATUSES.includes(s) ? d : null;
            },
            { label: `telnyx.status to reach one of ${ANSWERED_STATUSES.join("/")}` }
        );
        check("status advanced to answered/in-progress", () =>
            assert.ok(
                ANSWERED_STATUSES.includes(String(afterAns.telnyx.status).toLowerCase()),
                `status=${afterAns.telnyx.status}`
            )
        );

        console.log("\n--- call.hangup ---");
        const hangup = await post(
            "/telnyx/webhooks",
            telnyxEvent("call.hangup", {
                answer_time: ANSWER.toISOString(),
                end_time: END.toISOString(),
                hangup_cause: "normal_clearing",
                hangup_source: "callee",
                state: "hangup",
            })
        );
        check("call.hangup acked 2xx", () =>
            assert.ok(hangup.status >= 200 && hangup.status < 300, `got ${hangup.status}`)
        );

        const billed = await waitFor(
            async () => {
                const tx = await db
                    .collection("credittransactions")
                    .find({ type: "call_deduction" })
                    .toArray();
                return tx.length ? tx : null;
            },
            { label: "credit transaction", timeoutMs: 30000 }
        );
        const finalLog = await db.collection("CallLogs").findOne({ "telnyx.call_control_id": CCID });

        check("final status is completed", () =>
            assert.strictEqual(String(finalLog.telnyx.status).toLowerCase(), "completed")
        );
        check("duration computed from answer_time to end_time (42s)", () =>
            assert.strictEqual(Number(finalLog.telnyx.duration), 42)
        );
        check("charged exactly once", () => assert.strictEqual(billed.length, 1));
        check("billingKey keyed on the dialer id (worker parity)", () =>
            assert.strictEqual(billed[0].reference.billingKey, `${campaignId}:call:${DIALER_ID}`)
        );
        check("CallLog flagged creditsDeducted", () =>
            assert.strictEqual(finalLog.creditsDeducted, true)
        );

        console.log("\n--- Telnyx retries the same hangup event ---");
        await post(
            "/telnyx/webhooks",
            telnyxEvent("call.hangup", {
                answer_time: ANSWER.toISOString(),
                end_time: END.toISOString(),
                hangup_cause: "normal_clearing",
                state: "hangup",
            })
        );
        await new Promise((r) => setTimeout(r, 5000));
        const afterReplay = await db
            .collection("credittransactions")
            .countDocuments({ type: "call_deduction" });
        check("retry does not create a second charge", () => assert.strictEqual(afterReplay, 1));

        console.log("\n--- /hangup authorization ---");
        const noAuth = await post("/hangup", { call_control_id: CCID });
        check("/hangup without a secret is rejected 401", () =>
            assert.strictEqual(noAuth.status, 401)
        );
        const withAuth = await post(
            "/hangup",
            { call_control_id: CCID },
            { "x-webhook-secret": SECRET }
        );
        check("/hangup with the secret passes auth (not 401)", () =>
            assert.notStrictEqual(withAuth.status, 401)
        );
        const noIds = await post("/hangup", {}, { "x-webhook-secret": SECRET });
        check("/hangup with no ids returns 400, not 401", () =>
            assert.strictEqual(noIds.status, 400)
        );
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
