/**
 * Wave 5 — foreign-only credit hold (CREDIT_HOLD_ENABLED=0 default).
 * Hold at dial; settle on hangup; TTL release for orphans.
 */
const { ObjectId } = require("mongodb");
const logger = require("../logger");

function envOn(name, def = false) {
    const raw = process.env[name];
    if (raw == null || String(raw).trim() === "") return def;
    const v = String(raw).trim().toLowerCase();
    return v !== "0" && v !== "false" && v !== "off" && v !== "no";
}

function providersAllow(provider) {
    const list = String(process.env.CREDIT_HOLD_PROVIDERS || "twilio,telnyx")
        .split(",")
        .map((x) => x.trim().toLowerCase())
        .filter((p) => p && p !== "pool");
    return list.includes(String(provider || "").toLowerCase());
}

async function acquireCreditHold(db, { userId, callId, provider, amount, campaignId }) {
    if (!envOn("CREDIT_HOLD_ENABLED", false)) return { skipped: true, reason: "disabled" };
    if (!providersAllow(provider)) return { skipped: true, reason: "provider" };
    const ttl = Math.max(60, Number(process.env.CREDIT_HOLD_TTL_SEC || 7200) || 7200);
    const minDial = Math.max(0, Number(process.env.MIN_CREDITS_FOR_DIAL || 0) || 0);
    const holdAmount = Math.max(minDial, Number(amount) || 0);
    if (holdAmount <= 0 || !userId || !callId) return { skipped: true, reason: "invalid" };

    const oid = userId instanceof ObjectId ? userId : new ObjectId(String(userId));
    const updated = await db.collection("users").findOneAndUpdate(
        { _id: oid, credits: { $gte: holdAmount } },
        { $inc: { credits: -holdAmount }, $set: { updatedAt: new Date() } },
        { returnDocument: "after" }
    );
    if (!updated?.value && !updated?.credits && updated == null) {
        // driver version differences
    }
    const okUser = updated?.value || updated;
    if (!okUser) {
        return { ok: false, error: "insufficient_for_hold" };
    }

    const expiresAt = new Date(Date.now() + ttl * 1000);
    await db.collection("creditholds").updateOne(
        { callId: String(callId) },
        {
            $set: {
                callId: String(callId),
                userId: oid,
                campaignId: campaignId || null,
                provider: String(provider || "").toLowerCase(),
                amount: holdAmount,
                status: "held",
                expiresAt,
                updatedAt: new Date(),
            },
            $setOnInsert: { createdAt: new Date() },
        },
        { upsert: true }
    );
    return { ok: true, holdAmount, expiresAt };
}

async function settleCreditHold(db, { callId, finalCharge }) {
    if (!envOn("CREDIT_HOLD_ENABLED", false)) return { skipped: true };
    const hold = await db.collection("creditholds").findOne({ callId: String(callId), status: "held" });
    if (!hold) return { skipped: true, reason: "no_hold" };

    const held = Number(hold.amount) || 0;
    const charge = Number(finalCharge) || 0;
    const delta = held - charge; // positive => refund remainder
    if (delta !== 0) {
        await db.collection("users").updateOne(
            { _id: hold.userId },
            { $inc: { credits: delta }, $set: { updatedAt: new Date() } }
        );
    }
    await db.collection("creditholds").updateOne(
        { _id: hold._id },
        { $set: { status: "settled", settledCharge: charge, updatedAt: new Date() } }
    );
    return { ok: true, held, charge, refunded: Math.max(0, delta) };
}

async function releaseExpiredHolds(db, { limit = 100 } = {}) {
    if (!envOn("CREDIT_HOLD_ENABLED", false)) return { skipped: true };
    const now = new Date();
    const expired = await db
        .collection("creditholds")
        .find({ status: "held", expiresAt: { $lte: now } })
        .limit(limit)
        .toArray();
    let released = 0;
    for (const h of expired) {
        try {
            await db.collection("users").updateOne(
                { _id: h.userId },
                { $inc: { credits: Number(h.amount) || 0 }, $set: { updatedAt: new Date() } }
            );
            await db.collection("creditholds").updateOne(
                { _id: h._id },
                { $set: { status: "released_ttl", updatedAt: new Date() } }
            );
            released++;
        } catch (err) {
            logger.warn("[CreditHold] TTL release failed", { error: err?.message, callId: h.callId });
        }
    }
    return { ok: true, released };
}

async function ensureCreditHoldIndexes(database) {
    try {
        await database.collection("creditholds").createIndex(
            { callId: 1 },
            { unique: true, name: "creditholds_callId" }
        );
        await database.collection("creditholds").createIndex(
            { status: 1, expiresAt: 1 },
            { name: "creditholds_ttl_scan" }
        );
        await database.collection("billing_wallet_shadow_log").createIndex(
            { createdAt: -1 },
            { name: "billing_wallet_shadow_createdAt", expireAfterSeconds: 90 * 24 * 3600 }
        );
    } catch (err) {
        logger.warn("[DB] credit hold indexes partial", { error: err?.message });
    }
}

module.exports = {
    acquireCreditHold,
    settleCreditHold,
    releaseExpiredHolds,
    ensureCreditHoldIndexes,
};
