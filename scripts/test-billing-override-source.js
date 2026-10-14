/**
 * Offline checks: per-minute rate override selection (points 13/14).
 *  - Org member's own billingOverride (enabled) prices the call; the admin wallet still pays.
 *  - Member override disabled → admin (wallet) override.
 *  - basic voice tier uses basicRatePerMinute; with only standardRatePerMinute set → normal pricing.
 * Run: node --test scripts/test-billing-override-source.js
 */
const assert = require("node:assert/strict");
const test = require("node:test");
const path = require("node:path");
const { ObjectId } = require("mongodb");

// ── Stub db + side-effect modules before campaignCreditDeduction is required ──
let fakeDb = null;
const notified = [];
function stubModule(rel, exportsObj) {
    const file = path.join(__dirname, "..", rel);
    require.cache[require.resolve(file)] = { id: file, filename: file, loaded: true, exports: exportsObj };
}
stubModule("db.js", {
    getDb: () => {
        if (!fakeDb) throw new Error("no db");
        return fakeDb;
    },
    connectDB: async () => fakeDb,
});
stubModule("lib/pricingShadow.js", {
    resolveForeignLiveRateSelection: async () => ({ applied: false }),
    applyPriceFloor: ({ customerChargeUsd }) => ({ charge: customerChargeUsd }),
    recordForeignPricingShadow: async () => {},
});
stubModule("lib/callEconomics.js", { maybeRecordCallEconomics: async () => {} });
stubModule("lib/notifyTenantCallStatus.js", {
    notifyTenantCallBilled: async (payload) => {
        notified.push(payload);
        return { ok: true };
    },
    notifyTenantCallStatus: async () => ({ ok: true }),
});

const { tryDeductCampaignCallCredits, pickRateOverride } = require("../lib/campaignCreditDeduction");

// ── Minimal in-memory Mongo ──
function getPath(doc, key) {
    return key.split(".").reduce((v, k) => (v == null ? undefined : v[k]), doc);
}
function same(a, b) {
    if (a instanceof ObjectId || b instanceof ObjectId) return String(a) === String(b);
    return a === b;
}
function matches(doc, query) {
    for (const [key, cond] of Object.entries(query || {})) {
        if (key === "$or") {
            if (!cond.some((q) => matches(doc, q))) return false;
            continue;
        }
        const v = getPath(doc, key);
        if (cond && typeof cond === "object" && !(cond instanceof ObjectId) && !(cond instanceof Date)) {
            if ("$in" in cond && !cond.$in.some((x) => same(v, x))) return false;
            if ("$ne" in cond && same(v, cond.$ne)) return false;
            if ("$gt" in cond && !(Number(v) > cond.$gt)) return false;
            if ("$gte" in cond && !(Number(v) >= cond.$gte)) return false;
            continue;
        }
        if (!same(v, cond)) return false;
    }
    return true;
}
function evalExpr(doc, expr) {
    if (typeof expr === "string" && expr.startsWith("$")) return getPath(doc, expr.slice(1));
    if (expr && typeof expr === "object" && !(expr instanceof Date)) {
        if (expr.$max) return Math.max(...expr.$max.map((e) => evalExpr(doc, e)));
        if (expr.$subtract) {
            const [a, b] = expr.$subtract.map((e) => evalExpr(doc, e));
            return a - b;
        }
    }
    return expr;
}
function applyUpdate(doc, update) {
    if (Array.isArray(update)) {
        for (const stage of update) {
            const computed = {};
            for (const [k, e] of Object.entries(stage.$set || {})) computed[k] = evalExpr(doc, e);
            Object.assign(doc, computed);
        }
        return;
    }
    if (update.$set) Object.assign(doc, update.$set);
    if (update.$inc) {
        for (const [k, n] of Object.entries(update.$inc)) doc[k] = Number(doc[k] || 0) + n;
    }
}

function createMemoryDb(seed = {}) {
    const store = {};
    const hooks = {};
    function rows(name) {
        if (!store[name]) store[name] = (seed[name] || []).map((d) => ({ ...d }));
        return store[name];
    }
    return {
        store,
        hooks,
        rows,
        collection(name) {
            return {
                async findOne(q) {
                    return rows(name).find((d) => matches(d, q)) || null;
                },
                find(q) {
                    const found = rows(name).filter((d) => matches(d, q));
                    const cursor = {
                        project: () => cursor,
                        toArray: async () => found.map((d) => ({ ...d })),
                    };
                    return cursor;
                },
                async insertOne(doc) {
                    if (hooks[`${name}.insertOne`]) await hooks[`${name}.insertOne`](doc);
                    const row = { _id: new ObjectId(), ...doc };
                    rows(name).push(row);
                    return { insertedId: row._id };
                },
                async updateOne(filter, update, opts = {}) {
                    const doc = rows(name).find((d) => matches(d, filter));
                    if (!doc) {
                        if (opts.upsert) {
                            const row = { _id: new ObjectId(), ...filter, ...(update.$setOnInsert || {}) };
                            applyUpdate(row, update);
                            rows(name).push(row);
                        }
                        return { matchedCount: 0, modifiedCount: 0 };
                    }
                    applyUpdate(doc, update);
                    return { matchedCount: 1, modifiedCount: 1 };
                },
                async updateMany(filter, update) {
                    const found = rows(name).filter((d) => matches(d, filter));
                    found.forEach((d) => applyUpdate(d, update));
                    return { matchedCount: found.length, modifiedCount: found.length };
                },
                // Driver v7 shape: returns the document (or null), no { value } wrapper.
                async findOneAndUpdate(filter, update, opts = {}) {
                    const doc = rows(name).find((d) => matches(d, filter));
                    if (!doc) return null;
                    const before = { ...doc };
                    applyUpdate(doc, update);
                    return opts.returnDocument === "after" ? { ...doc } : before;
                },
            };
        },
    };
}

// ── Fixture: admin (wallet) + org member (campaign creator) ──
// Normal pricing = starter package pricePerMinute 0.5 (no country config).
function fixture({ adminOverride = null, memberOverride = null, voiceTier = "standard" } = {}) {
    const adminId = new ObjectId();
    const orgUserId = new ObjectId();
    const campaignId = new ObjectId();
    const db = createMemoryDb({
        users: [
            { _id: adminId, email: "admin@org.test", role: "admin", credits: 100, billingOverride: adminOverride },
            {
                _id: orgUserId,
                email: "member@org.test",
                role: "user",
                createdBy: String(adminId),
                credits: 50,
                billingOverride: memberOverride,
            },
        ],
        campaigns: [
            {
                _id: campaignId,
                name: "Member camp",
                userId: orgUserId,
                createdBy: "member@org.test",
                status: "active",
                selectedVoice: { tier: voiceTier },
            },
        ],
        creditpackages: [{ packageId: "starter", pricePerMinute: 0.5 }],
        CallLogs: [{ call_id: "call-1", userId: orgUserId, campaign_id: String(campaignId) }],
        systemsettings: [],
    });
    fakeDb = db;
    notified.length = 0;
    const credits = (id) => db.rows("users").find((u) => String(u._id) === String(id)).credits;
    const txs = () => db.rows("credittransactions");
    const run = () =>
        tryDeductCampaignCallCredits({
            callUniqueId: "call-1",
            contactId: null,
            campaignId: String(campaignId),
            durationSec: 60,
        });
    return { adminId, orgUserId, credits, txs, run };
}

test("pickRateOverride: creator enabled wins, else wallet, same user counts as wallet", () => {
    const admin = { _id: "a", billingOverride: { enabled: true, flatRatePerMinute: 1 } };
    const member = { _id: "m", billingOverride: { enabled: true, flatRatePerMinute: 2 } };
    assert.deepEqual(pickRateOverride(member, admin), { override: member.billingOverride, source: "creator" });
    const off = { _id: "m", billingOverride: { enabled: false, flatRatePerMinute: 2 } };
    assert.deepEqual(pickRateOverride(off, admin), { override: admin.billingOverride, source: "wallet" });
    assert.deepEqual(pickRateOverride(admin, admin), { override: admin.billingOverride, source: "wallet" });
    assert.deepEqual(pickRateOverride(null, { _id: "x" }), { override: null, source: null });
});

test("creator override enabled → creator rate charged to admin wallet", async () => {
    const f = fixture({
        adminOverride: { enabled: true, flatRatePerMinute: 1 },
        memberOverride: { enabled: true, flatRatePerMinute: 2 },
    });
    const res = await f.run();
    assert.deepEqual(res, { outcome: "deducted", cost: 2 });
    assert.equal(f.credits(f.adminId), 98, "admin wallet pays");
    assert.equal(f.credits(f.orgUserId), 50, "member wallet untouched");
    const [tx] = f.txs();
    assert.equal(String(tx.userId), String(f.adminId));
    assert.equal(tx.reference.planRatePerMinute, 2);
    assert.equal(tx.reference.billingOverrideSource, "creator");
});

test("creator override disabled → admin override", async () => {
    const f = fixture({
        adminOverride: { enabled: true, flatRatePerMinute: 1 },
        memberOverride: { enabled: false, flatRatePerMinute: 2 },
    });
    const res = await f.run();
    assert.deepEqual(res, { outcome: "deducted", cost: 1 });
    assert.equal(f.credits(f.adminId), 99);
    assert.equal(f.txs()[0].reference.billingOverrideSource, "wallet");
});

test("creator multiplier applies to creator tier rate", async () => {
    const f = fixture({
        memberOverride: { enabled: true, standardRatePerMinute: 1, chargeMultiplier: 1.5 },
    });
    const res = await f.run();
    assert.deepEqual(res, { outcome: "deducted", cost: 1.5 });
    assert.equal(f.credits(f.adminId), 98.5);
});

test("basic voice tier uses basicRatePerMinute", async () => {
    const f = fixture({
        voiceTier: "basic",
        adminOverride: { enabled: true, basicRatePerMinute: 0.25, standardRatePerMinute: 1 },
    });
    const res = await f.run();
    assert.deepEqual(res, { outcome: "deducted", cost: 0.25 });
    assert.equal(f.txs()[0].reference.billingOverrideSource, "wallet");
});

test("basic voice tier with only standardRatePerMinute → normal pricing", async () => {
    const f = fixture({
        voiceTier: "basic",
        memberOverride: { enabled: true, standardRatePerMinute: 1 },
    });
    const res = await f.run();
    assert.deepEqual(res, { outcome: "deducted", cost: 0.5 });
    assert.equal(f.credits(f.adminId), 99.5);
    assert.equal(f.txs()[0].reference.billingOverrideSource, null);
});

test("flat rate wins over basic tier rate", async () => {
    const f = fixture({
        voiceTier: "basic",
        adminOverride: { enabled: true, flatRatePerMinute: 0.75, basicRatePerMinute: 0.25 },
    });
    const res = await f.run();
    assert.deepEqual(res, { outcome: "deducted", cost: 0.75 });
});

// Point 12: multiplier scales whatever the call costs; 0 / empty = no multiplier.
test("multiplier alone scales the normal price (discount)", async () => {
    // Basic voice → package price 0.5 (no country price for basic).
    const f = fixture({ voiceTier: "basic", adminOverride: { enabled: true, chargeMultiplier: 0.8 } });
    const res = await f.run();
    assert.deepEqual(res, { outcome: "deducted", cost: 0.4 });
    assert.equal(f.txs()[0].reference.billingOverrideSource, "wallet");
});

test("multiplier with a rate for another voice tier scales the normal price", async () => {
    const f = fixture({
        voiceTier: "basic",
        adminOverride: { enabled: true, standardRatePerMinute: 1, chargeMultiplier: 1.2 },
    });
    const res = await f.run();
    assert.deepEqual(res, { outcome: "deducted", cost: 0.6 });
});

test("multiplier 0 is ignored (never a free call)", async () => {
    const f = fixture({ voiceTier: "basic", adminOverride: { enabled: true, chargeMultiplier: 0 } });
    const res = await f.run();
    assert.deepEqual(res, { outcome: "deducted", cost: 0.5 });
    assert.equal(f.txs()[0].reference.billingOverrideSource, null);
});
