/**
 * E2E preflight: verifies live Telnyx credentials, Mongo and Redis reachability.
 * Read-only — places no calls and buys nothing.
 * Run: node scripts/tmp-e2e-preflight.js
 */
require("dotenv").config();

const results = [];
function record(name, ok, detail) {
    results.push({ name, ok, detail });
    console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? ` — ${detail}` : ""}`);
}

async function telnyxGet(path) {
    const key = String(process.env.TELNYX_API_KEY || "").trim();
    const res = await fetch(`https://api.telnyx.com/v2${path}`, {
        headers: { Authorization: `Bearer ${key}`, Accept: "application/json" },
    });
    const text = await res.text();
    let json = null;
    try {
        json = JSON.parse(text);
    } catch {
        /* non-json error body */
    }
    return { status: res.status, json, text };
}

async function checkTelnyx() {
    const key = String(process.env.TELNYX_API_KEY || "").trim();
    const connId = String(process.env.TELNYX_CONNECTION_ID || "").trim();
    if (!key) return record("Telnyx API key present", false, "TELNYX_API_KEY not set");

    const whoami = await telnyxGet("/phone_numbers?page[size]=1");
    if (whoami.status === 401 || whoami.status === 403) {
        return record("Telnyx API key valid", false, `HTTP ${whoami.status} — key rejected`);
    }
    if (whoami.status >= 400) {
        return record("Telnyx API key valid", false, `HTTP ${whoami.status}: ${whoami.text.slice(0, 200)}`);
    }
    record("Telnyx API key valid", true, `HTTP ${whoami.status}`);

    if (!connId) {
        record("TELNYX_CONNECTION_ID set", false, "missing — purchase would be rejected");
    } else {
        const conn = await telnyxGet(`/connections/${encodeURIComponent(connId)}`);
        if (conn.status === 200) {
            const d = conn.json?.data || {};
            record(
                "TELNYX_CONNECTION_ID resolves",
                true,
                `type=${d.record_type || d.connection_name ? d.record_type : "?"} name=${d.connection_name || "?"} active=${d.active}`
            );
            const webhookUrl =
                d.webhook_event_url ||
                d.outbound?.webhook_event_url ||
                d.inbound?.webhook_event_url ||
                null;
            record(
                "Connection has a webhook_event_url",
                Boolean(webhookUrl),
                webhookUrl ? "set" : "NOT SET — no call events will ever reach this service"
            );
            // Telnyx posts JSON as { data: { event_type, payload } }. Any other route on this
            // service parses a different shape and will reject every event.
            const path = webhookUrl ? new URL(webhookUrl).pathname : "";
            record(
                "webhook_event_url points at the Telnyx route (/telnyx/webhooks)",
                path === "/telnyx/webhooks",
                path
                    ? `currently "${path}" — Telnyx payloads sent here are rejected, so no call is logged or billed`
                    : "no URL to check"
            );
        } else {
            record(
                "TELNYX_CONNECTION_ID resolves",
                false,
                `HTTP ${conn.status}: ${String(conn.text).slice(0, 200)}`
            );
        }
    }

    // Numbers bound to this connection — these are the only ones that can fire call events.
    const owned = await telnyxGet("/phone_numbers?page[size]=100");
    if (owned.status === 200) {
        const list = Array.isArray(owned.json?.data) ? owned.json.data : [];
        const bound = list.filter((n) => String(n.connection_id || "") === connId);
        const unbound = list.filter((n) => !String(n.connection_id || "").trim());
        record(
            "Telnyx owns at least one number",
            list.length > 0,
            `${list.length} number(s) on the account`
        );
        record(
            "Numbers bound to TELNYX_CONNECTION_ID",
            bound.length > 0,
            `${bound.length} bound, ${unbound.length} with NO connection (need backfill-telnyx-connection.js)`
        );
        const otherConn = list.length - bound.length - unbound.length;
        if (otherConn > 0) {
            record(
                "No numbers stranded on a different connection",
                false,
                `${otherConn} number(s) attached to some other connection — their events go elsewhere`
            );
        }
    } else {
        record("List Telnyx numbers", false, `HTTP ${owned.status}`);
    }
}

async function checkMongo() {
    const { MongoClient } = require("mongodb");
    const uri = String(process.env.MONGODB_URI || "").trim();
    if (!uri) return record("Mongo reachable", false, "MONGODB_URI not set");
    const client = new MongoClient(uri, { serverSelectionTimeoutMS: 12000 });
    try {
        await client.connect();
        const db = client.db();
        await db.command({ ping: 1 });
        record("Mongo reachable", true, `db=${db.databaseName}`);

        const idx = await db.collection("credittransactions").indexes();
        const unique = idx.find(
            (i) => i.key?.type === 1 && i.key?.["reference.billingKey"] === 1 && i.unique === true
        );
        record(
            "credittransactions billingKey unique index",
            Boolean(unique),
            unique ? unique.name : "MISSING — duplicate charges would not be blocked at the DB level"
        );
    } catch (err) {
        record("Mongo reachable", false, err.message);
    } finally {
        await client.close().catch(() => {});
    }
}

async function checkRedis() {
    const Redis = require("ioredis");
    const url = String(process.env.REDIS_URL || "").trim();
    const host = String(process.env.REDIS_HOST || "").trim();
    if (!url && !host) return record("Redis reachable", false, "REDIS_URL / REDIS_HOST not set");
    const client = url
        ? new Redis(url, { maxRetriesPerRequest: 1, connectTimeout: 8000, lazyConnect: true })
        : new Redis({
              host,
              port: Number(process.env.REDIS_PORT || 6379),
              password: process.env.REDIS_PASSWORD || undefined,
              maxRetriesPerRequest: 1,
              connectTimeout: 8000,
              lazyConnect: true,
          });
    try {
        await client.connect();
        const pong = await client.ping();
        record("Redis reachable", pong === "PONG", `ping=${pong}`);
    } catch (err) {
        record("Redis reachable", false, err.message);
    } finally {
        client.disconnect();
    }
}

function checkConfigConsistency() {
    const pub = String(process.env.TELNYX_PUBLIC_KEY || "").trim();
    const flag = String(process.env.TELNYX_WEBHOOK_VERIFY || "").trim().toLowerCase();
    const off = flag === "0" || flag === "false" || flag === "off";
    record(
        "Telnyx webhook signature verification",
        Boolean(pub) && !off,
        pub
            ? off
                ? "public key set but TELNYX_WEBHOOK_VERIFY disables it"
                : "enabled"
            : "TELNYX_PUBLIC_KEY missing — webhooks accepted UNVERIFIED (spoofable)"
    );
}

(async () => {
    console.log("--- Telnyx / infra preflight (read-only) ---\n");
    checkConfigConsistency();
    await checkTelnyx();
    await checkMongo();
    await checkRedis();

    const failed = results.filter((r) => !r.ok);
    console.log(
        `\n--- ${results.length - failed.length}/${results.length} passed ---`
    );
    if (failed.length) {
        console.log("Blocking/attention items:");
        for (const f of failed) console.log(`  - ${f.name}: ${f.detail}`);
    }
    process.exit(0);
})();
