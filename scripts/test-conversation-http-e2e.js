/**
 * HTTP E2E for POST /conversation (call_id-first + pool + Telnyx routing).
 *
 * Boots the real service in-process against a throwaway Mongo DB, then:
 *  A) India/pool: POST /conversation with call_id + campaign_id + contact_id + turns
 *  B) Telnyx: mapping → POST /conversation with call_id only (resolve via reverse map)
 *  C) Telnyx: POST /conversation with call_id + campaign_id + contact_id + turns
 *
 * Run: node scripts/test-conversation-http-e2e.js
 */
require("dotenv").config();

const assert = require("assert");
const { MongoClient, ObjectId } = require("mongodb");

const PROBE_DB = "ondial_e2e_probe_conversation";
const PORT = process.env.E2E_PORT || "9098";
const BASE = `http://127.0.0.1:${PORT}`;
const SECRET = "e2e-conversation-secret-not-real";

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

const POOL_CALL_ID = `e2e-pool-${Date.now()}`;
const TELNYX_CALL_ID = `e2e-telnyx-dialer-${Date.now()}`;
const CCID = `v3:e2eConv${Date.now()}`;

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

async function waitFor(fn, { timeoutMs = 25000, everyMs = 400, label = "condition" } = {}) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const v = await fn();
        if (v) return v;
        await new Promise((r) => setTimeout(r, everyMs));
    }
    throw new Error(`timed out waiting for ${label}`);
}

(async () => {
    console.log(`--- Conversation HTTP E2E against "${PROBE_DB}" on port ${PORT} ---\n`);
    require("../index.js");

    const client = new MongoClient(probeUri, { serverSelectionTimeoutMS: 15000 });
    await client.connect();
    const db = client.db();

    const campaignId = new ObjectId();
    const contactId = new ObjectId();
    const telnyxCampaignId = new ObjectId();
    const telnyxContactId = new ObjectId();

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

        // ── A) Pool / India via common /conversation ─────────────────────────
        console.log("\n--- A) POST /conversation (pool by call_id) ---");
        const poolBody = {
            call_id: POOL_CALL_ID,
            campaign_id: String(campaignId),
            contact_id: String(contactId),
            start_time: "2026-07-25T06:29:08.000Z",
            end_time: "2026-07-25T06:29:50.000Z",
            turns: [
                { role: "agent", text: "Hello, calling from Ondial." },
                { role: "user", text: "Hi, go ahead." },
                { role: "agent", text: "Great — noted." },
            ],
        };
        const poolRes = await post("/conversation", poolBody);
        check("pool conversation returns 200", () =>
            assert.strictEqual(poolRes.status, 200, `got ${poolRes.status}: ${poolRes.text}`)
        );
        check("pool response provider=pool", () =>
            assert.strictEqual(poolRes.json?.provider, "pool")
        );
        check("pool response received=true", () => assert.strictEqual(poolRes.json?.received, true));
        check("pool turnCount=3", () => assert.strictEqual(poolRes.json?.turnCount, 3));

        const poolDoc = await waitFor(
            () =>
                db.collection("CallLogs").findOne({
                    $or: [{ call_unique_id: POOL_CALL_ID }, { call_id: POOL_CALL_ID }],
                }),
            { label: "pool CallLog" }
        );
        check("pool CallLog has campaign_id + contact_id", () => {
            assert.strictEqual(String(poolDoc.campaign_id), String(campaignId));
            assert.strictEqual(String(poolDoc.contact_id), String(contactId));
        });
        check("pool CallLog has conversation turns", () => {
            const turns = poolDoc.conversation?.turns || poolDoc.pool?.conversation?.turns || [];
            assert.ok(turns.length >= 3, `turns=${turns.length}`);
        });

        // ── B) Telnyx mapping + call_id-only conversation ────────────────────
        console.log("\n--- B) Telnyx mapping → /conversation (call_id) ---");
        const mapping = await post("/api/telnyx-mapping", {
            call_control_id: CCID,
            call_id: TELNYX_CALL_ID,
            call_unique_id: TELNYX_CALL_ID,
            campaign_id: String(telnyxCampaignId),
            contact_id: String(telnyxContactId),
        });
        check("telnyx mapping accepted", () =>
            assert.strictEqual(mapping.status, 200, `got ${mapping.status}: ${mapping.text}`)
        );
        await new Promise((r) => setTimeout(r, 800));

        const telnyxRes = await post("/conversation", {
            call_id: TELNYX_CALL_ID,
            campaign_id: String(telnyxCampaignId),
            contact_id: String(telnyxContactId),
            start_time: "2026-07-25T06:29:08.000Z",
            end_time: "2026-07-25T06:29:50.000Z",
            turns: [
                { role: "agent", text: "Hello from Telnyx." },
                { role: "user", text: "Hi." },
                { role: "agent", text: "Noted." },
            ],
        });
        check("telnyx conversation returns 200", () =>
            assert.strictEqual(telnyxRes.status, 200, `got ${telnyxRes.status}: ${telnyxRes.text}`)
        );
        check("telnyx response provider=telnyx (routed via dialer map)", () =>
            assert.strictEqual(
                telnyxRes.json?.provider,
                "telnyx",
                `json=${JSON.stringify(telnyxRes.json)}`
            )
        );
        check("telnyx response received=true", () =>
            assert.strictEqual(telnyxRes.json?.received, true)
        );

        const telnyxDoc = await waitFor(
            () =>
                db.collection("CallLogs").findOne({
                    $or: [
                        { "telnyx.call_control_id": CCID },
                        { call_unique_id: TELNYX_CALL_ID },
                        { call_id: TELNYX_CALL_ID },
                    ],
                }),
            { label: "telnyx CallLog after conversation" }
        );
        check("telnyx CallLog has campaign_id + contact_id", () => {
            assert.strictEqual(String(telnyxDoc.campaign_id), String(telnyxCampaignId));
            assert.strictEqual(String(telnyxDoc.contact_id), String(telnyxContactId));
        });
        check("telnyx CallLog has conversation turns", () => {
            const turns =
                telnyxDoc.telnyx?.conversation?.turns ||
                telnyxDoc.conversation?.turns ||
                [];
            assert.ok(turns.length >= 2, `turns=${turns.length}`);
        });

        // ── C) Validation: missing call_id ───────────────────────────────────
        console.log("\n--- C) Validation ---");
        const bad = await post("/conversation", {
            turns: [{ role: "agent", text: "no id" }],
        });
        check("missing call_id returns 400", () => assert.strictEqual(bad.status, 400));
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
