/**
 * Offline checks: tryDeductCampaignCallCredits partial charge (clamp to 0), org wallet billing,
 * org-scoped pause, and E11000 refund of the amount actually charged.
 * Run: node --test scripts/test-call-partial-charge.js
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

const { tryDeductCampaignCallCredits } = require("../lib/campaignCreditDeduction");

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

// ── Fixture: admin (wallet) + org user (campaign creator) + an unrelated org ──
function fixture({ adminCredits, orgUserCredits = 50, creatorMode = false } = {}) {
    const adminId = new ObjectId();
    const orgUserId = new ObjectId();
    const otherId = new ObjectId();
    const campaignId = new ObjectId();
    const adminCampaignId = new ObjectId();
    const otherCampaignId = new ObjectId();
    const override = { enabled: true, flatRatePerMinute: 1 }; // 60s call → cost 1
    const db = createMemoryDb({
        users: [
            { _id: adminId, email: "admin@org.test", role: "admin", credits: adminCredits, billingOverride: override },
            {
                _id: orgUserId,
                email: "member@org.test",
                role: "user",
                createdBy: String(adminId),
                credits: orgUserCredits,
                billingOverride: override,
            },
            { _id: otherId, email: "other@else.test", role: "admin", credits: 0 },
        ],
        campaigns: [
            { _id: campaignId, name: "Member camp", userId: orgUserId, createdBy: "member@org.test", status: "active", concurrentCalls: 2 },
            { _id: adminCampaignId, name: "Admin camp", userId: String(adminId), createdBy: "admin@org.test", status: "active", concurrentCalls: 3 },
            { _id: otherCampaignId, name: "Other camp", userId: otherId, createdBy: "other@else.test", status: "active", concurrentCalls: 1 },
        ],
        CallLogs: [{ call_id: "call-1", userId: orgUserId, campaign_id: String(campaignId) }],
        systemsettings: creatorMode
            ? [{ key: "billingWalletV1", value: { orgUserId: String(adminId), mode: "creator" } }]
            : [],
    });
    fakeDb = db;
    notified.length = 0;
    const credits = (id) => db.rows("users").find((u) => String(u._id) === String(id)).credits;
    const campaignStatus = (id) => db.rows("campaigns").find((c) => String(c._id) === String(id)).status;
    const callLog = () => db.rows("CallLogs")[0];
    const txs = () => db.rows("credittransactions");
    const run = () =>
        tryDeductCampaignCallCredits({
            callUniqueId: "call-1",
            contactId: null,
            campaignId: String(campaignId),
            durationSec: 60,
        });
    return { db, adminId, orgUserId, campaignId, adminCampaignId, otherCampaignId, credits, campaignStatus, callLog, txs, run };
}

test("full charge: org user call billed to admin wallet, no pause", async () => {
    const f = fixture({ adminCredits: 5 });
    const res = await f.run();
    assert.deepEqual(res, { outcome: "deducted", cost: 1 });
    assert.equal(f.credits(f.adminId), 4);
    assert.equal(f.credits(f.orgUserId), 50, "org user wallet untouched");
    const [tx] = f.txs();
    assert.equal(tx.amount, -1);
    assert.equal(String(tx.userId), String(f.adminId));
    assert.equal(tx.balanceAfter, 4);
    assert.equal(tx.reference.fullCost, 1);
    assert.equal(tx.reference.unpaidAmount, 0);
    assert.equal(tx.reference.partial, false);
    assert.equal(f.callLog().creditsDeducted, true);
    assert.equal(f.callLog().creditsDeductedAmount, 1);
    assert.equal(f.callLog().creditsUnpaidAmount, 0);
    assert.equal(f.callLog().creditDeductionError, null);
    assert.equal(f.campaignStatus(f.campaignId), "active");
    assert.equal(f.campaignStatus(f.adminCampaignId), "active");
});

test("full charge leaving < 0.065 pauses the org", async () => {
    const f = fixture({ adminCredits: 1.05 });
    const res = await f.run();
    assert.equal(res.outcome, "deducted");
    assert.ok(Math.abs(f.credits(f.adminId) - 0.05) < 1e-9);
    assert.equal(f.txs()[0].balanceAfter, 0.05);
    assert.equal(f.campaignStatus(f.campaignId), "paused");
    assert.equal(f.campaignStatus(f.adminCampaignId), "paused");
});

test("partial charge: takes what is left (balance → 0), records unpaid, pauses org", async () => {
    const f = fixture({ adminCredits: 0.4 });
    const res = await f.run();
    assert.deepEqual(res, { outcome: "partial_insufficient", cost: 0.4, unpaid: 0.6 });
    assert.equal(f.credits(f.adminId), 0);
    const [tx] = f.txs();
    assert.equal(tx.amount, -0.4);
    assert.equal(tx.balanceAfter, 0);
    assert.equal(tx.reference.fullCost, 1);
    assert.equal(tx.reference.unpaidAmount, 0.6);
    assert.equal(tx.reference.partial, true);
    assert.ok(tx.reference.billingKey.endsWith(":call:call-1"));
    const log = f.callLog();
    assert.equal(log.creditsDeducted, true);
    assert.equal(log.creditsDeductedAmount, 0.4);
    assert.equal(log.creditsUnpaidAmount, 0.6);
    assert.equal(log.creditDeductionError, "insufficient_credits_partial");
    assert.equal(notified.at(-1).creditsCharged, 0.4);
    assert.equal(f.campaignStatus(f.campaignId), "paused", "org user campaign paused");
    assert.equal(f.campaignStatus(f.adminCampaignId), "paused", "admin campaign paused");
    assert.equal(f.campaignStatus(f.otherCampaignId), "active", "other org untouched");

    // Re-delivery is idempotent and reports the charged amount.
    const again = await f.run();
    assert.deepEqual(again, { outcome: "already_billed", cost: 0.4 });
    assert.equal(f.txs().length, 1);
});

test("zero balance: nothing charged, insufficient_credits, org paused", async () => {
    const f = fixture({ adminCredits: 0 });
    const res = await f.run();
    assert.deepEqual(res, { outcome: "insufficient_credits", cost: 1 });
    assert.equal(f.credits(f.adminId), 0);
    assert.equal(f.txs().length, 0, "no zero-amount ledger row");
    assert.equal(f.callLog().creditsDeducted, false);
    assert.equal(f.callLog().creditDeductionError, "insufficient_credits");
    assert.equal(f.campaignStatus(f.campaignId), "paused");
    assert.equal(f.campaignStatus(f.adminCampaignId), "paused");
    assert.equal(f.campaignStatus(f.otherCampaignId), "active");
});

test("negative balance is left untouched (not reset to 0)", async () => {
    const f = fixture({ adminCredits: -2 });
    const res = await f.run();
    assert.equal(res.outcome, "insufficient_credits");
    assert.equal(f.credits(f.adminId), -2);
});

test("E11000 duplicate billingKey refunds only the amount this attempt charged", async () => {
    const f = fixture({ adminCredits: 0.4 });
    f.db.hooks["credittransactions.insertOne"] = async (doc) => {
        // The racing path already recorded the winning row for this billingKey.
        f.db.rows("credittransactions").push({
            _id: new ObjectId(),
            userId: f.adminId,
            type: "call_deduction",
            amount: -0.4,
            reference: { billingKey: doc.reference.billingKey },
        });
        const err = new Error("E11000 duplicate key error");
        err.code = 11000;
        throw err;
    };
    const res = await f.run();
    assert.equal(res.outcome, "already_billed");
    // Debited 0.4 (clamped), refunded 0.4 — NOT the full cost of 1.
    assert.equal(f.credits(f.adminId), 0.4);
});

test("creator mode: org user pays own wallet and only their campaigns pause", async () => {
    const f = fixture({ adminCredits: 100, orgUserCredits: 0.25, creatorMode: true });
    const res = await f.run();
    assert.deepEqual(res, { outcome: "partial_insufficient", cost: 0.25, unpaid: 0.75 });
    assert.equal(f.credits(f.orgUserId), 0);
    assert.equal(f.credits(f.adminId), 100);
    assert.equal(f.campaignStatus(f.campaignId), "paused");
    assert.equal(f.campaignStatus(f.adminCampaignId), "active");
});
