/**
 * Offline checks: follow-up cap, WhatsApp callback scheduling / conditional arm,
 * late carrier events, atomic completed attempts, analysis dedupe, replay nonce lifecycle.
 * Run: node --test scripts/test-followup-guards.js
 */
const assert = require("node:assert/strict");
const test = require("node:test");
const path = require("node:path");
const { ObjectId } = require("mongodb");

// ── Stub db / redis before anything requires them (no live services needed) ──
let fakeDb = null;
let fakeRedis = null;
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
stubModule("redis.js", {
    getRedis: () => {
        if (!fakeRedis) throw new Error("Redis not connected");
        return fakeRedis;
    },
    getBullRedis: () => fakeRedis,
    connectRedis: async () => ({}),
});

function makeFakeRedis() {
    const store = new Map();
    return {
        store,
        async set(key, value, ...args) {
            const nx = args.includes("NX");
            if (nx && store.has(key)) return null;
            store.set(key, String(value));
            return "OK";
        },
        async get(key) {
            return store.has(key) ? store.get(key) : null;
        },
        async del(key) {
            return store.delete(key) ? 1 : 0;
        },
        multi() {
            const ops = [];
            const chain = {
                set: (...a) => (ops.push(() => this.set(...a)), chain),
                del: (...a) => (ops.push(() => this.del(...a)), chain),
                exec: async () => {
                    for (const op of ops) await op();
                    return [];
                },
            };
            return chain;
        },
    };
}

const {
    resolveMaxFollowUpCallsPerContact,
    followUpArmCapFilter,
} = require("../lib/followUpArmCap");
const {
    parseScheduledAt,
    resolveScheduledAt,
    buildWhatsappCallbackPatches,
    applyWhatsappCallbackUpdate,
} = require("../whatsapp/applyWhatsappCallbackUpdate");
const { isStaleCallEventForContact } = require("../lib/callReceiveStatusPolicy");
const {
    buildCompletedAttemptFilter,
    syncTwilioContactFromCall,
} = require("../lib/twilioContactSync");
const { collectAnalysisCallIdAliases } = require("../lib/resolveLegacyAnalysisCallId");
const {
    mergeAnalysisDataForRerun,
    persistCallAnalysisDoc,
} = require("../lib/analysisPersistGuard");

const NOW = new Date("2026-10-13T06:00:00.000Z"); // 11:30 IST

// ── W4: scheduled_at parsing ─────────────────────────────────────────────────
test("naive scheduled_at is wall-clock in campaign timezone (fallback Asia/Kolkata)", () => {
    assert.equal(parseScheduledAt("2026-10-14 15:00", {}).toISOString(), "2026-10-14T09:30:00.000Z");
    assert.equal(
        parseScheduledAt("2026-10-14T15:00:00", { timezone: "America/New_York" }).toISOString(),
        "2026-10-14T19:00:00.000Z"
    );
    assert.equal(
        parseScheduledAt("2026-10-14T15:00", {
            timezone: "(UTC+5:30) Chennai, Kolkata, Mumbai, New Delhi (IST)",
        }).toISOString(),
        "2026-10-14T09:30:00.000Z"
    );
});

test("scheduled_at with explicit offset / Z is honoured as-is", () => {
    assert.equal(
        parseScheduledAt("2026-10-14T15:00:00+05:30", { timezone: "America/New_York" }).toISOString(),
        "2026-10-14T09:30:00.000Z"
    );
    assert.equal(parseScheduledAt("2026-10-14T15:00:00Z", {}).toISOString(), "2026-10-14T15:00:00.000Z");
    assert.equal(parseScheduledAt("not a date", {}), null);
});

test("resolveScheduledAt never returns a past or present time", () => {
    const campaign = { retryDelayMinutes: 30 };
    const past = resolveScheduledAt("2026-10-13T05:00:00Z", campaign, NOW);
    assert.equal(past.toISOString(), "2026-10-13T06:30:00.000Z");
    const exactlyNow = resolveScheduledAt(NOW.toISOString(), campaign, NOW);
    assert.equal(exactlyNow.toISOString(), "2026-10-13T06:30:00.000Z");
    // Naive "10:00" IST today is already past at 11:30 IST.
    assert.equal(resolveScheduledAt("2026-10-13 10:00", campaign, NOW).toISOString(), "2026-10-13T06:30:00.000Z");
    // Future stays.
    assert.equal(resolveScheduledAt("2026-10-13 18:00", campaign, NOW).toISOString(), "2026-10-13T12:30:00.000Z");
    // No retryDelayMinutes → existing default fallback (5 min).
    assert.equal(resolveScheduledAt("2026-10-12 10:00", {}, NOW).toISOString(), "2026-10-13T06:05:00.000Z");
});

// ── Cap convention ───────────────────────────────────────────────────────────
test("cap = campaign.maxRetryAttempts (default 3, minimum 1)", () => {
    assert.equal(resolveMaxFollowUpCallsPerContact({}), 3);
    assert.equal(resolveMaxFollowUpCallsPerContact(null), 3);
    assert.equal(resolveMaxFollowUpCallsPerContact({ maxRetryAttempts: null }), 3);
    assert.equal(resolveMaxFollowUpCallsPerContact({ maxRetryAttempts: "abc" }), 3);
    assert.equal(resolveMaxFollowUpCallsPerContact({ maxRetryAttempts: 3.9 }), 3);
    assert.equal(resolveMaxFollowUpCallsPerContact({ maxRetryAttempts: -4 }), 1);
    assert.equal(resolveMaxFollowUpCallsPerContact({ maxRetryAttempts: 0 }), 1);
    assert.deepEqual(followUpArmCapFilter(2), { $or: [{ followUpArmCount: { $exists: false } }, { followUpArmCount: { $lt: 2 } }] });
});

// ── W5: WhatsApp arm ─────────────────────────────────────────────────────────
const cid = new ObjectId();
test("callback status:true arms with conditional filter + followUpArmCount $inc", () => {
    const p = buildWhatsappCallbackPatches({
        contact: { _id: cid, status: "completed", callReceiveStatus: 3 },
        campaign: { retryDelayMinutes: 15, maxRetryAttempts: 2 },
        callbackUpdate: { status: true, scheduled_at: "2026-10-13 18:00" },
        now: NOW,
    });
    assert.equal(p.action, "schedule");
    assert.equal(p.contactSet.status, "pending");
    assert.equal(p.contactSet.isFollowUp, true);
    assert.equal(p.scheduledAt.toISOString(), "2026-10-13T12:30:00.000Z");
    assert.deepEqual(p.contactInc, { followUpArmCount: 1 });
    assert.deepEqual(p.contactFilter.status, { $nin: ["processing", "enqueued"] });
    assert.deepEqual(p.contactFilter.callReceiveStatus, { $nin: [2] });
    assert.deepEqual(p.contactFilter.$or, [{ followUpArmCount: { $exists: false } }, { followUpArmCount: { $lt: 2 } }]);
});

test("callback status:true at cap → no arm, follow-up-cap-reached history", () => {
    const p = buildWhatsappCallbackPatches({
        contact: { _id: cid, status: "completed", followUpArmCount: 2 },
        analysis: { _id: "a1" },
        campaign: { maxRetryAttempts: 2 },
        callbackUpdate: { status: true },
        now: NOW,
    });
    assert.equal(p.action, "cap_reached");
    assert.equal(p.contactSet.status, undefined);
    const h = p.contactPush.statusHistory;
    assert.equal(h.reason, "follow-up-cap-reached");
    assert.equal(h.fromStatus, "completed");
    assert.equal(h.toStatus, "completed");
    assert.equal(h.cap, 2);
    assert.equal(h.count, 2);
    assert.equal(h.analysisId, "a1");
    assert.equal(h.source, "whatsapp_ai");
    assert.ok(h.timestamp instanceof Date);
    assert.equal(p.analysisSet["analysis_data.callback_requested.status"], true);
});

test("maxRetryAttempts 0 still allows one arm; cap 3 allows the 3rd", () => {
    const blocked = buildWhatsappCallbackPatches({
        contact: { status: "completed", followUpArmCount: 1 },
        campaign: { maxRetryAttempts: 0 },
        callbackUpdate: { status: true },
        now: NOW,
    });
    assert.equal(blocked.action, "cap_reached");
    const allowed = buildWhatsappCallbackPatches({
        contact: { status: "completed", followUpArmCount: 2 },
        campaign: { maxRetryAttempts: 3 },
        callbackUpdate: { status: true },
        now: NOW,
    });
    assert.equal(allowed.action, "schedule");
});

test("F9: pending follow-up keepExisting → no contact write, no duplicate history", () => {
    const future = new Date(NOW.getTime() + 3600_000);
    const p = buildWhatsappCallbackPatches({
        contact: { status: "pending", isFollowUp: true, scheduledAt: future, followUpArmCount: 2 },
        campaign: {},
        callbackUpdate: { status: true },
        now: NOW,
    });
    assert.equal(p.action, "acknowledge");
    assert.equal(p.contactSet, null);
    assert.equal(p.contactPush, undefined);
    assert.equal(p.analysisSet["analysis_data.callback_requested"].scheduled_at, future.toISOString());
});

test("reschedule of a pending follow-up is not capped and does not $inc", () => {
    const p = buildWhatsappCallbackPatches({
        contact: { status: "pending", isFollowUp: true, followUpArmCount: 2 },
        campaign: {},
        callbackUpdate: { status: true, scheduled_at: "2026-10-14T10:00:00+05:30" },
        now: NOW,
    });
    assert.equal(p.action, "reschedule");
    assert.equal(p.contactInc, null);
    assert.equal(p.contactFilter.status, "pending");
});

test("busy contact (in call) → acknowledge only", () => {
    const p = buildWhatsappCallbackPatches({
        contact: { status: "processing" },
        callbackUpdate: { status: true },
        now: NOW,
    });
    assert.equal(p.action, "acknowledge_only");
    const ringing = buildWhatsappCallbackPatches({
        contact: { status: "retry", callReceiveStatus: 2 },
        callbackUpdate: { status: true },
        now: NOW,
    });
    assert.equal(ringing.action, "acknowledge_only");
    // CRS 1 (missed call, finished) must stay armable.
    const missed = buildWhatsappCallbackPatches({
        contact: { status: "retry", callReceiveStatus: 1 },
        callbackUpdate: { status: true },
        now: NOW,
    });
    assert.equal(missed.action, "schedule");
});

function makeContactsColl({ matchFirst = true, latest = null } = {}) {
    const calls = [];
    return {
        calls,
        async updateOne(filter, update) {
            calls.push({ filter, update });
            const matched = calls.length === 1 ? (matchFirst ? 1 : 0) : 1;
            return { matchedCount: matched, modifiedCount: matched };
        },
        async findOne() {
            return latest;
        },
    };
}

test("TOCTOU: conditional arm matching nothing falls back to acknowledge-only", async () => {
    const contacts = makeContactsColl({
        matchFirst: false,
        latest: { _id: cid, status: "processing", followUpArmCount: 0 },
    });
    const analysisWrites = [];
    const db = {
        collection: (n) =>
            n === "contactprocessings"
                ? contacts
                : { updateOne: async (f, u) => (analysisWrites.push(u), { matchedCount: 1 }) },
    };
    const res = await applyWhatsappCallbackUpdate(db, {
        contact: { _id: cid, status: "completed" },
        analysis: { _id: "a1" },
        campaign: {},
        callbackUpdate: { status: true },
        now: NOW,
    });
    assert.equal(res.action, "acknowledge_only");
    assert.equal(contacts.calls.length, 2);
    assert.equal(contacts.calls[0].filter.status.$nin[0], "processing");
    assert.equal(contacts.calls[1].update.$set.status, undefined);
    assert.equal(analysisWrites[0].$set["analysis_data.callback_requested.status"], true);
});

test("TOCTOU: concurrent arm hit the cap → cap history recorded, not armed", async () => {
    const contacts = makeContactsColl({
        matchFirst: false,
        latest: { _id: cid, status: "pending", isFollowUp: true, followUpArmCount: 2 },
    });
    const db = { collection: () => contacts };
    const res = await applyWhatsappCallbackUpdate(db, {
        contact: { _id: cid, status: "completed", followUpArmCount: 1 },
        campaign: { maxRetryAttempts: 2 },
        callbackUpdate: { status: true },
        now: NOW,
    });
    assert.equal(res.action, "cap_reached");
    assert.equal(contacts.calls[1].update.$push.statusHistory.reason, "follow-up-cap-reached");
});

// ── W3: late carrier events ──────────────────────────────────────────────────
test("isStaleCallEventForContact", () => {
    const start = new Date("2026-10-13T05:00:00Z");
    assert.equal(isStaleCallEventForContact({ contact: {}, callStartedAt: null }).stale, false);
    assert.equal(
        isStaleCallEventForContact({
            contact: { lastFollowUpAt: new Date("2026-10-13T05:10:00Z") },
            callStartedAt: start,
        }).reason,
        "contact_rearmed_after_call"
    );
    assert.equal(
        isStaleCallEventForContact({
            contact: { isFollowUp: true, status: "pending", scheduledAt: "2026-10-13T07:00:00Z" },
            callStartedAt: start,
        }).reason,
        "pending_follow_up_after_call"
    );
    assert.equal(
        isStaleCallEventForContact({
            contact: { status: "processing" },
            callSid: "CAold",
            callStartedAt: start,
            latestCall: { carrier_call_id: "CAnew", registeredAt: start.getTime() + 60_000 },
        }).reason,
        "superseded_by_newer_call"
    );
    // Current follow-up call (dialed after the arm) is not stale.
    assert.equal(
        isStaleCallEventForContact({
            contact: { lastFollowUpAt: new Date("2026-10-13T04:00:00Z"), isFollowUp: true, status: "processing" },
            callSid: "CAnew",
            callStartedAt: start,
            latestCall: { carrier_call_id: "CAnew", registeredAt: start.getTime() },
        }).stale,
        false
    );
});

function makeSyncDb(contact, { matched = 1 } = {}) {
    const updates = [];
    return {
        updates,
        collection: () => ({
            findOne: async () => contact,
            updateOne: async (filter, update) => {
                updates.push({ filter, update });
                return { matchedCount: matched, modifiedCount: matched };
            },
        }),
    };
}

test("late completed for an old call on a re-armed contact is skipped", async () => {
    const oid = new ObjectId();
    fakeDb = makeSyncDb({
        _id: oid,
        status: "pending",
        isFollowUp: true,
        callReceiveStatus: 0,
        lastFollowUpAt: new Date("2026-10-13T05:30:00Z"),
        scheduledAt: new Date("2026-10-14T05:30:00Z"),
        callAttempts: [{ attemptNumber: 1, status: "completed" }],
    });
    fakeRedis = null; // Redis down → Mongo signals still work
    const r = await syncTwilioContactFromCall({
        contactIdRaw: String(oid),
        twilioStatus: "completed",
        callSid: "CAold",
        callStartedAt: new Date("2026-10-13T05:00:00Z"),
    });
    assert.equal(r.outcome, "skipped_stale_call");
    assert.equal(fakeDb.updates.length, 0);
});

test("completed transition is guarded, records CallSid and clears isFollowUp", async () => {
    const oid = new ObjectId();
    fakeDb = makeSyncDb({ _id: oid, status: "processing", isFollowUp: true, callReceiveStatus: 2, callAttempts: [] });
    const r = await syncTwilioContactFromCall({
        contactIdRaw: String(oid),
        twilioStatus: "completed",
        callSid: "CA1",
    });
    assert.equal(r.outcome, "updated");
    const { filter, update } = fakeDb.updates[0];
    assert.deepEqual(filter.callReceiveStatus, { $ne: 3 });
    assert.deepEqual(filter["callAttempts.CallSid"], { $ne: "CA1" });
    assert.equal(update.$set.isFollowUp, false);
    assert.equal(update.$push.callAttempts.CallSid, "CA1");
    assert.equal(update.$push.callAttempts.attemptNumber, 1);
});

test("second concurrent completion loses the race and does not push", async () => {
    const oid = new ObjectId();
    fakeDb = makeSyncDb(
        { _id: oid, status: "processing", callReceiveStatus: 2, callAttempts: [] },
        { matched: 0 }
    );
    const r = await syncTwilioContactFromCall({
        contactIdRaw: String(oid),
        twilioStatus: "completed",
        callSid: "CA1",
    });
    assert.equal(r.outcome, "already_completed");
    assert.equal(fakeDb.updates.length, 2); // original + one guarded re-run, both no-op
});

test("retry attempts carry attemptNumber / attemptedAt / CallSid and keep isFollowUp", async () => {
    const oid = new ObjectId();
    fakeDb = makeSyncDb({ _id: oid, status: "processing", isFollowUp: true, callReceiveStatus: 2, callAttempts: [] });
    await syncTwilioContactFromCall({ contactIdRaw: String(oid), twilioStatus: "no-answer", callSid: "CA2" });
    const { filter, update } = fakeDb.updates[0];
    const a = update.$push.callAttempts;
    assert.equal(a.attemptNumber, 1);
    assert.ok(a.attemptedAt instanceof Date);
    assert.equal(a.CallSid, "CA2");
    assert.equal(update.$set.isFollowUp, undefined);
    assert.deepEqual(filter.callReceiveStatus, { $ne: 3 });
    assert.deepEqual(filter["callAttempts.CallSid"], { $ne: "CA2" });
});

test("buildCompletedAttemptFilter backfill path guards on attempts length", () => {
    const oid = new ObjectId();
    const f = buildCompletedAttemptFilter({ oid, contact: { callAttempts: [] }, prevReceive: 3, callSid: null });
    assert.equal(f.callReceiveStatus, 3);
    assert.ok(Array.isArray(f.$or));
});

// ── W7: analysis dedupe ──────────────────────────────────────────────────────
test("aliases cover Telnyx UUID + v3 control id", () => {
    const UUID = "04b7df0f-10e4-476d-babe-c0b8e9aedd6b";
    const V3 = "v3:abc";
    const aliases = collectAnalysisCallIdAliases(
        { call_unique_id: UUID, telnyx: { call_control_id: V3 } },
        V3
    );
    assert.equal(aliases[0], UUID);
    assert.ok(aliases.includes(V3));
});

test("re-run preserves WhatsApp-edited callback_requested and never touches followUpProcessed", async () => {
    const existing = {
        _id: "doc1",
        call_id: "v3:abc",
        followUpProcessed: true,
        analysis_data: { callback_requested: { status: true, source: "whatsapp_ai" } },
    };
    assert.deepEqual(
        mergeAnalysisDataForRerun(existing, { summary: "x", callback_requested: { status: false } }),
        { summary: "x", callback_requested: { status: true, source: "whatsapp_ai" } }
    );
    const writes = [];
    const coll = {
        findOne: async (q) => (q.call_id ? existing : null),
        updateOne: async (filter, update, opts) => (writes.push({ filter, update, opts }), { matchedCount: 1 }),
    };
    const res = await persistCallAnalysisDoc(
        { collection: () => coll },
        {
            persistCallId: "04b7df0f-10e4-476d-babe-c0b8e9aedd6b",
            aliases: ["v3:abc"],
            updateObj: {
                call_id: "04b7df0f-10e4-476d-babe-c0b8e9aedd6b",
                analysis_data: { callback_requested: { status: false } },
            },
        }
    );
    assert.equal(res.action, "updated");
    assert.equal(res.callId, "v3:abc");
    const w = writes[0];
    assert.equal(w.filter._id, "doc1");
    assert.equal(w.opts, undefined); // no upsert → no second doc
    assert.equal(w.update.$set.call_id, undefined);
    assert.equal(w.update.$set.followUpProcessed, undefined);
    assert.equal(w.update.$setOnInsert, undefined);
    assert.equal(w.update.$set.analysis_data.callback_requested.source, "whatsapp_ai");
});

test("new analysis inserts under canonical id with $setOnInsert followUpProcessed:false", async () => {
    const writes = [];
    const coll = {
        findOne: async () => null,
        updateOne: async (filter, update, opts) => (writes.push({ filter, update, opts }), { matchedCount: 0 }),
    };
    const res = await persistCallAnalysisDoc(
        { collection: () => coll },
        { persistCallId: "U1", aliases: ["v3:x"], updateObj: { call_id: "U1", analysis_data: {} } }
    );
    assert.equal(res.action, "inserted");
    assert.deepEqual(writes[0].filter, { call_id: "U1" });
    assert.equal(writes[0].opts.upsert, true);
    assert.equal(writes[0].update.$setOnInsert.followUpProcessed, false);
});

// ── Analysis payload: the analysis API does not accept extra time-zone fields ──
test("analysis payload sends current_time_ist only (no campaign time-zone fields)", () => {
    const { buildBaseEnrichedFields } = require("../lib/analysis_services/shared");
    const out = buildBaseEnrichedFields({}, { timezone: "America/New_York" }, {}, {});
    assert.match(out.current_time_ist, /\+05:30$/);
    assert.equal("campaign_timezone" in out, false);
    assert.equal("current_time_campaign" in out, false);
});

// ── W8: replay nonce lifecycle ───────────────────────────────────────────────
test("replay nonce: released on failure (retry re-processes), done on success", async () => {
    fakeRedis = makeFakeRedis();
    const { handleWebhook } = require("../webhookHandler");
    const meta = { eventId: "evt-1", jobId: "job-1" };
    const body = { unknown: true }; // unknown shape → no DB work

    // Fail markDedupeProcessed once → handleWebhook throws → nonce must be released.
    const origMulti = fakeRedis.multi.bind(fakeRedis);
    fakeRedis.multi = () => ({ set() { return this; }, del() { return this; }, exec: async () => { throw new Error("boom"); } });
    await assert.rejects(handleWebhook(body, meta), /boom/);
    assert.equal(fakeRedis.store.has("replay:event:evt-1"), false);

    fakeRedis.multi = origMulti;
    await handleWebhook(body, meta);
    assert.equal(fakeRedis.store.get("replay:event:evt-1"), "done");

    // A later delivery with the same event id is blocked.
    fakeRedis.store.delete(`dedupe:done:payload:${require("crypto").createHash("sha256").update(JSON.stringify(body)).digest("hex")}`);
    await handleWebhook(body, { eventId: "evt-1", jobId: "job-2" });
    assert.equal(fakeRedis.store.get("replay:event:evt-1"), "done");
});

test("replay nonce: same job retrying after a crash takes over its own reservation", async () => {
    fakeRedis = makeFakeRedis();
    const { handleWebhook } = require("../webhookHandler");
    fakeRedis.store.set("replay:event:evt-2", "processing:job-9");
    await handleWebhook({ other: 1 }, { eventId: "evt-2", jobId: "job-9" });
    assert.equal(fakeRedis.store.get("replay:event:evt-2"), "done");
    // A different job seeing an in-flight reservation does not process.
    fakeRedis.store.set("replay:event:evt-3", "processing:job-1");
    await handleWebhook({ other: 2 }, { eventId: "evt-3", jobId: "job-2" });
    assert.equal(fakeRedis.store.get("replay:event:evt-3"), "processing:job-1");
});

// ── Plan B: missed follow-up gets its own retries per follow-up cycle ─────────
test("Plan B: follow-up retries are counted per cycle, capped by max", () => {
    const { resolveUnansweredRetryDecision, followUpAttemptSetFields, followUpCycleKey } =
        require("../lib/followUpArmCap");
    const arm = { isFollowUp: true, lastFollowUpAt: "2026-10-13T05:45:50Z", scheduledAt: "2026-10-13T05:50:50Z" };
    // Lifetime attempt 3 of max 3 would fail under the old rule; follow-up try 1 is retryable.
    let d = resolveUnansweredRetryDecision(arm, 3, 3);
    assert.equal(d.isRetryable, true);
    assert.equal(d.followUp.attemptNumber, 1);
    let c = { ...arm, ...followUpAttemptSetFields(d.followUp) };
    d = resolveUnansweredRetryDecision(c, 3, 4);
    assert.equal(d.followUp.attemptNumber, 2);
    assert.equal(d.isRetryable, true);
    c = { ...c, ...followUpAttemptSetFields(d.followUp) };
    d = resolveUnansweredRetryDecision(c, 3, 5);
    assert.equal(d.followUp.attemptNumber, 3);
    assert.equal(d.isRetryable, false);
    // New arm → fresh cycle.
    const rearmed = { ...c, lastFollowUpAt: "2026-10-13T07:00:00Z" };
    assert.equal(resolveUnansweredRetryDecision(rearmed, 3, 6).followUp.attemptNumber, 1);
    // No cycle marker → lifetime rule.
    const bare = resolveUnansweredRetryDecision({ isFollowUp: true }, 3, 3);
    assert.equal(bare.followUp, null);
    assert.equal(bare.isRetryable, false);
    // Non follow-up → lifetime rule.
    assert.equal(resolveUnansweredRetryDecision({}, 3, 2).isRetryable, true);
    assert.equal(followUpCycleKey({}), "");
});
