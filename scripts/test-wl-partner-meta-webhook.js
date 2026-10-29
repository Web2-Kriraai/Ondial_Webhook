/**
 * Offline: white-label partner Meta webhooks (signature, verify token, routing).
 * No Mongo / Redis — an in-memory fake db.
 *
 *   node --test scripts/test-wl-partner-meta-webhook.js
 */
process.env.TOKEN_ENCRYPTION_KEY = process.env.TOKEN_ENCRYPTION_KEY || "x".repeat(32);

const test = require("node:test");
const assert = require("node:assert");
const crypto = require("crypto");
const { ObjectId } = require("mongodb");

const {
    authorizeMetaWebhookPost,
    authorizeMetaWebhookVerify,
    extractMetaPhoneNumberIds,
} = require("../lib/metaWebhookAuth");
const wl = require("../whatsapp/wlPartnerMeta");
const {
    findInboundCandidateContacts,
    pickFollowupContact,
    relayInboundWhatsAppMessage,
} = require("../whatsapp/whatsappAiRelay");

function encrypt(plain) {
    const key = Buffer.from(process.env.TOKEN_ENCRYPTION_KEY, "utf8");
    const iv = crypto.randomBytes(16);
    const c = crypto.createCipheriv("aes-256-cbc", key, iv);
    return `${iv.toString("hex")}:${Buffer.concat([c.update(plain, "utf8"), c.final()]).toString("hex")}`;
}

function sign(body, secret) {
    return `sha256=${crypto.createHmac("sha256", secret).update(body).digest("hex")}`;
}

// ---- tiny in-memory Mongo stand-in -------------------------------------------------
function get(doc, path) {
    return path.split(".").reduce((v, k) => (v == null ? undefined : v[k]), doc);
}
function eq(a, b) {
    return String(a) === String(b);
}
function matches(doc, query) {
    return Object.entries(query || {}).every(([key, cond]) => {
        if (key === "$or") return cond.some((q) => matches(doc, q));
        if (key === "$and") return cond.every((q) => matches(doc, q));
        const value = get(doc, key);
        if (cond && typeof cond === "object" && !(cond instanceof ObjectId)) {
            if ("$elemMatch" in cond) {
                return Array.isArray(value) && value.some((el) => matches(el, cond.$elemMatch));
            }
            if ("$in" in cond) return cond.$in.some((c) => eq(c, value));
            if ("$exists" in cond || "$nin" in cond) {
                if (cond.$exists === true && value === undefined) return false;
                if (cond.$nin && cond.$nin.some((c) => value === c)) return false;
                return true;
            }
        }
        return eq(value, cond);
    });
}
function fakeDb(data) {
    const calls = [];
    return {
        calls,
        collection(name) {
            const rows = data[name] || [];
            return {
                find(query) {
                    calls.push({ name, op: "find", query });
                    let out = rows.filter((r) => matches(r, query));
                    const cursor = {
                        sort(spec) {
                            const [[k, dir]] = Object.entries(spec);
                            out = [...out].sort(
                                (a, b) => dir * (new Date(get(a, k) || 0) - new Date(get(b, k) || 0))
                            );
                            return cursor;
                        },
                        limit(n) {
                            out = out.slice(0, n);
                            return cursor;
                        },
                        toArray: async () => out,
                    };
                    return cursor;
                },
                async findOne(query) {
                    calls.push({ name, op: "findOne", query });
                    return rows.find((r) => matches(r, query)) || null;
                },
            };
        },
    };
}

// ---- fixtures ------------------------------------------------------------------------
const ONDIAL_SECRET = "ondial-app-secret";
const PARTNER_SECRET = "partner-app-secret";
const PARTNER_PNID = "555000111";
const NATIVE_PNID = "1200331536506095";
const CUSTOMER_WA = "919811122233";

function wlProfile(extra = {}) {
    return {
        _id: new ObjectId(),
        provider: "meta",
        usesPlatformAccount: false,
        isDefault: true,
        verified: true,
        wabaId: "waba-partner",
        numberId: PARTNER_PNID,
        businessNumberId: PARTNER_PNID,
        accessToken: encrypt("partner-access-token"),
        wlWebhookAppSecret: encrypt(PARTNER_SECRET),
        wlWebhookVerifyToken: encrypt("partner-verify-token"),
        ...extra,
    };
}

const customerA = {
    _id: new ObjectId(),
    email: "a@customer.test",
    wlTenantId: "tenant-1",
    omniChannelSettings: { whatsappProfiles: [wlProfile()] },
};
const customerB = {
    _id: new ObjectId(),
    email: "b@customer.test",
    wlTenantId: "tenant-1",
    omniChannelSettings: { whatsappProfiles: [wlProfile({ wlWebhookVerifyToken: "" })] },
};
// Same number id but NOT a WL user: must never count as partner.
const nonWlUser = {
    _id: new ObjectId(),
    email: "own@waba.test",
    omniChannelSettings: {
        whatsappProfiles: [wlProfile({ wlWebhookAppSecret: encrypt("not-wl-secret") })],
    },
};

const campaignA = { _id: new ObjectId(), createdBy: String(customerA._id) };
const campaignB = { _id: new ObjectId(), createdBy: customerB.email };
const nativeCampaign = { _id: new ObjectId(), createdBy: "native@ondial.test" };

function contact(campaign, lastSent, extra = {}) {
    return {
        _id: new ObjectId(),
        campaignId: campaign._id,
        mobileNumber: CUSTOMER_WA,
        whatsappQueueStatus: "WAITING_REPLY",
        whatsappStatus: "sent",
        lastWhatsappSentAt: new Date(lastSent),
        updatedAt: new Date(lastSent),
        ...extra,
    };
}

function partnerPayload(pnid = PARTNER_PNID, extraValue = {}) {
    return {
        object: "whatsapp_business_account",
        entry: [
            {
                id: "waba",
                changes: [
                    {
                        field: "messages",
                        value: {
                            metadata: { phone_number_id: pnid },
                            messages: [{ from: CUSTOMER_WA, id: "wamid.in1", type: "text", text: { body: "hi" } }],
                            ...extraValue,
                        },
                    },
                ],
            },
        ],
    };
}

function loaders(db) {
    return {
        loadPartnerSecrets: (ids, opts) => wl.getPartnerAppSecretsForPhoneNumberIds(db, ids, opts),
        loadPartnerVerifyTokens: (opts) => wl.getPartnerVerifyTokens(db, opts),
    };
}

test.beforeEach(() => wl.resetWlPartnerMetaCaches());

// ---- POST signature -------------------------------------------------------------------
test("OnDial signature still accepted via env (no partner lookup)", async () => {
    const db = fakeDb({ users: [customerA] });
    const payload = partnerPayload(NATIVE_PNID);
    const body = JSON.stringify(payload);
    const r = await authorizeMetaWebhookPost({
        rawBody: Buffer.from(body),
        signature: sign(body, ONDIAL_SECRET),
        envSecret: ONDIAL_SECRET,
        payload,
        ...loaders(db),
    });
    assert.deepStrictEqual(r, { ok: true, via: "env" });
    assert.strictEqual(db.calls.length, 0, "native path must not touch the DB");
});

test("partner signature accepted only with the matching partner secret", async () => {
    const db = fakeDb({ users: [customerA, customerB, nonWlUser] });
    const payload = partnerPayload();
    const body = JSON.stringify(payload);
    const ok = await authorizeMetaWebhookPost({
        rawBody: Buffer.from(body),
        signature: sign(body, PARTNER_SECRET),
        envSecret: ONDIAL_SECRET,
        payload,
        ...loaders(db),
    });
    assert.deepStrictEqual(ok, { ok: true, via: "partner" });

    const nonWl = await authorizeMetaWebhookPost({
        rawBody: Buffer.from(body),
        signature: sign(body, "not-wl-secret"),
        envSecret: ONDIAL_SECRET,
        payload,
        ...loaders(db),
    });
    assert.strictEqual(nonWl.ok, false, "secret of a non-WL user is never a candidate");
    assert.strictEqual(nonWl.status, 401);
});

test("wrong secret rejected with 401; partner secret on another number rejected", async () => {
    const db = fakeDb({ users: [customerA] });
    const payload = partnerPayload();
    const body = JSON.stringify(payload);
    const bad = await authorizeMetaWebhookPost({
        rawBody: Buffer.from(body),
        signature: sign(body, "wrong"),
        envSecret: ONDIAL_SECRET,
        payload,
        ...loaders(db),
    });
    assert.deepStrictEqual(bad, { ok: false, status: 401, reason: "invalid_signature" });

    const otherPayload = partnerPayload("999999");
    const otherBody = JSON.stringify(otherPayload);
    const other = await authorizeMetaWebhookPost({
        rawBody: Buffer.from(otherBody),
        signature: sign(otherBody, PARTNER_SECRET),
        envSecret: ONDIAL_SECRET,
        payload: otherPayload,
        ...loaders(db),
    });
    assert.strictEqual(other.ok, false);

    const tampered = await authorizeMetaWebhookPost({
        rawBody: Buffer.from(body + " "),
        signature: sign(body, PARTNER_SECRET),
        envSecret: ONDIAL_SECRET,
        payload,
        ...loaders(db),
    });
    assert.strictEqual(tampered.ok, false, "HMAC is over the raw body bytes");
});

test("missing env secret: 503 unless a partner secret matches", async () => {
    const db = fakeDb({ users: [customerA] });
    const payload = partnerPayload();
    const body = JSON.stringify(payload);
    const none = await authorizeMetaWebhookPost({
        rawBody: Buffer.from(body),
        signature: sign(body, "wrong"),
        envSecret: "",
        payload,
        ...loaders(db),
    });
    assert.strictEqual(none.status, 503);
    const partner = await authorizeMetaWebhookPost({
        rawBody: Buffer.from(body),
        signature: sign(body, PARTNER_SECRET),
        envSecret: "",
        payload,
        ...loaders(db),
    });
    assert.strictEqual(partner.ok, true);
});

test("flood of random phone_number_ids with forged signatures: bounded DB calls", async () => {
    const db = fakeDb({ users: [customerA] });
    const realNow = Date.now;
    let clock = 5_000_000;
    Date.now = () => clock;
    try {
        for (let i = 0; i < 1000; i++) {
            clock += 60; // 1000 requests over 60s
            const payload = partnerPayload(String(100000000 + Math.floor(Math.random() * 1e9)));
            const body = JSON.stringify(payload);
            const r = await authorizeMetaWebhookPost({
                rawBody: Buffer.from(body),
                signature: sign(body, "forged"),
                envSecret: ONDIAL_SECRET,
                payload,
                ...loaders(db),
            });
            assert.strictEqual(r.status, 401);
        }
    } finally {
        Date.now = realNow;
    }
    const userQueries = db.calls.filter((c) => c.name === "users").length;
    // initial load + at most one miss-reload per 30s over 60s
    assert.ok(userQueries <= 3, `expected <=3 users queries, got ${userQueries}`);
});

test("new partner secret accepted within ~60s; reloads are global and rate-limited", async () => {
    const users = [];
    const db = fakeDb({ users });
    const t0 = 1_000_000;
    assert.deepStrictEqual(await wl.getPartnerAppSecretsForPhoneNumberIds(db, [PARTNER_PNID], { now: t0 }), []);
    users.push(customerA); // partner configured just now
    assert.deepStrictEqual(
        await wl.getPartnerAppSecretsForPhoneNumberIds(db, [PARTNER_PNID], { now: t0 + 1000 }),
        [],
        "snapshot still fresh"
    );
    assert.strictEqual(
        await wl.getPartnerAppSecretsForPhoneNumberIds(db, [PARTNER_PNID], { now: t0 + 1000, refresh: true }),
        null,
        "miss reload rate-limited (30s, global)"
    );
    assert.deepStrictEqual(
        await wl.getPartnerAppSecretsForPhoneNumberIds(db, [PARTNER_PNID], { now: t0 + 31_000, refresh: true }),
        [PARTNER_SECRET],
        "miss reload after 30s picks up the new secret"
    );
    const before = db.calls.length;
    await wl.getPartnerAppSecretsForPhoneNumberIds(db, ["123", PARTNER_PNID], { now: t0 + 60_000 });
    assert.strictEqual(db.calls.length, before, "served from snapshot; unknown id costs nothing");

    // Rotated secret, no miss-reload: periodic 60s refresh picks it up.
    wl.resetWlPartnerMetaCaches();
    users.length = 0;
    users.push(customerA);
    await wl.getPartnerAppSecretsForPhoneNumberIds(db, [PARTNER_PNID], { now: t0 });
    users[0] = {
        ...customerA,
        omniChannelSettings: { whatsappProfiles: [wlProfile({ wlWebhookAppSecret: encrypt("rotated") })] },
    };
    assert.deepStrictEqual(
        await wl.getPartnerAppSecretsForPhoneNumberIds(db, [PARTNER_PNID], { now: t0 + 61_000 }),
        ["rotated"]
    );
});

test("extractMetaPhoneNumberIds dedupes", () => {
    const p = partnerPayload();
    p.entry.push(partnerPayload("777").entry[0], partnerPayload().entry[0]);
    assert.deepStrictEqual(extractMetaPhoneNumberIds(p), [PARTNER_PNID, "777"]);
});

// ---- GET verify -----------------------------------------------------------------------
test("verify token: env token as today, partner token accepted, others 403", async () => {
    const db = fakeDb({ users: [customerA, customerB] });
    assert.deepStrictEqual(
        await authorizeMetaWebhookVerify({ token: "env-token", envToken: "env-token", ...loaders(db) }),
        { ok: true, via: "env" }
    );
    assert.strictEqual(db.calls.length, 0, "env token path does not touch the DB");
    assert.deepStrictEqual(
        await authorizeMetaWebhookVerify({ token: "partner-verify-token", envToken: "env-token", ...loaders(db) }),
        { ok: true, via: "partner" }
    );
    assert.deepStrictEqual(
        await authorizeMetaWebhookVerify({ token: "nope", envToken: "env-token", ...loaders(db) }),
        { ok: false, status: 403, reason: "token_mismatch" }
    );
    assert.strictEqual(
        (await authorizeMetaWebhookVerify({ token: "", envToken: "env-token", ...loaders(db) })).status,
        403
    );
});

// ---- routing --------------------------------------------------------------------------
test("WL routing picks the customer with the latest outbound to that wa_id", async () => {
    const cA = contact(campaignA, "2026-10-29T08:00:00Z");
    const cB = contact(campaignB, "2026-10-29T09:00:00Z");
    const cNative = contact(nativeCampaign, "2026-10-29T10:00:00Z"); // newest, but not this partner
    const db = fakeDb({
        users: [customerA, customerB, nonWlUser],
        campaigns: [campaignA, campaignB, nativeCampaign],
        contactprocessings: [cA, cB, cNative],
    });
    const r = await findInboundCandidateContacts(db, CUSTOMER_WA, PARTNER_PNID);
    assert.ok(r.wlScope);
    assert.strictEqual(r.wlUnattributed, false);
    const ids = r.contacts.map((c) => String(c._id)).sort();
    assert.deepStrictEqual(ids, [String(cA._id), String(cB._id)].sort(), "native contact excluded");
    const picked = pickFollowupContact(r.contacts, null, Date.parse("2026-10-29T10:30:00Z"));
    assert.strictEqual(String(picked._id), String(cB._id));

    // A sends later -> A wins.
    cA.lastWhatsappSentAt = new Date("2026-10-29T10:15:00Z");
    const r2 = await findInboundCandidateContacts(db, CUSTOMER_WA, PARTNER_PNID);
    assert.strictEqual(
        String(pickFollowupContact(r2.contacts, null, Date.parse("2026-10-29T10:30:00Z"))._id),
        String(cA._id)
    );
});

test("WL campaign sending from a different profile/number is not a candidate", async () => {
    const otherProfile = { _id: new ObjectId(), provider: "meta", usesPlatformAccount: true, numberId: "other" };
    const user = {
        ...customerA,
        omniChannelSettings: { whatsappProfiles: [wlProfile(), otherProfile] },
    };
    const camp = { _id: new ObjectId(), createdBy: String(user._id), followupWhatsappProfileId: String(otherProfile._id) };
    const db = fakeDb({ users: [user], campaigns: [camp], contactprocessings: [contact(camp, "2026-10-29T08:00:00Z")] });
    const r = await findInboundCandidateContacts(db, CUSTOMER_WA, PARTNER_PNID);
    assert.strictEqual(r.wlUnattributed, true);
});

test("WL number with no history for that wa_id: not delivered to anyone", async () => {
    const db = fakeDb({
        users: [customerA, customerB],
        campaigns: [campaignA, campaignB, nativeCampaign],
        contactprocessings: [contact(nativeCampaign, "2026-10-29T10:00:00Z")],
    });
    const r = await findInboundCandidateContacts(db, CUSTOMER_WA, PARTNER_PNID);
    assert.strictEqual(r.wlUnattributed, true);
    assert.deepStrictEqual(r.contacts, []);

    const relay = await relayInboundWhatsAppMessage(db, {
        phone: CUSTOMER_WA,
        text: "hello",
        messageId: "wamid.x",
        phoneNumberId: PARTNER_PNID,
    });
    assert.deepStrictEqual(relay, { handled: false, reason: "wl_unattributed" });
    assert.ok(
        !db.calls.some((c) => c.name === "whatsapp_ai_sessions" || c.name === "contactprocessings" && c.op !== "find"),
        "no session / contact writes or reads beyond the scoped lookup"
    );
});

test("native routing unchanged: phone-only lookup for non-WL / missing phone_number_id", async () => {
    const cNative = contact(nativeCampaign, "2026-10-29T10:00:00Z");
    const cA = contact(campaignA, "2026-10-29T08:00:00Z");
    const db = fakeDb({
        users: [customerA, nonWlUser],
        campaigns: [campaignA, nativeCampaign],
        contactprocessings: [cNative, cA],
    });
    for (const pnid of [NATIVE_PNID, ""]) {
        db.calls.length = 0;
        const r = await findInboundCandidateContacts(db, CUSTOMER_WA, pnid);
        assert.strictEqual(r.wlScope, null);
        assert.strictEqual(r.contacts.length, 2, "all contacts for the phone, as before");
        const contactQueries = db.calls.filter((c) => c.name === "contactprocessings");
        assert.strictEqual(contactQueries.length, 1);
        assert.deepStrictEqual(Object.keys(contactQueries[0].query), ["$or"], "same query shape as before");
        assert.ok(!db.calls.some((c) => c.name === "campaigns"));
    }
});

test("statuses are routed by wamid (unchanged, unambiguous)", async () => {
    const { parseMetaStatusUpdates } = require("../whatsapp/metaStatusUpdates");
    const p = partnerPayload(PARTNER_PNID, {
        messages: undefined,
        statuses: [{ id: "wamid.out1", status: "read", recipient_id: CUSTOMER_WA }],
    });
    assert.deepStrictEqual(parseMetaStatusUpdates(p), [
        { messageId: "wamid.out1", status: "read", errorCode: null, error: null },
    ]);
});
