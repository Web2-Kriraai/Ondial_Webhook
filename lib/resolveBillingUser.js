/**
 * Wave 5 — shared billing wallet resolution (W1 parent default).
 * BILLING_WALLET_SHADOW=1 logs both parent and creator candidates without changing debit.
 */
const { ObjectId } = require("mongodb");
const logger = require("../logger");

function envOn(name, def = false) {
    const raw = process.env[name];
    if (raw == null || String(raw).trim() === "") return def;
    const v = String(raw).trim().toLowerCase();
    return v !== "0" && v !== "false" && v !== "off" && v !== "no";
}

function toObjectId(id) {
    if (!id) return null;
    if (id instanceof ObjectId) return id;
    const s = String(id);
    if (!ObjectId.isValid(s)) return null;
    try {
        return new ObjectId(s);
    } catch {
        return null;
    }
}

/**
 * @returns {Promise<{ user: object|null, policy: string, parentUserId: string|null, creatorUserId: string|null }>}
 */
async function resolveBillingUser(db, { campaign, callLogUserId, explicitUserId } = {}) {
    let user = null;
    const tryIds = [explicitUserId, callLogUserId, campaign?.userId].filter(Boolean);
    for (const id of tryIds) {
        const oid = toObjectId(id);
        if (!oid) continue;
        user = await db.collection("users").findOne({ _id: oid });
        if (user) break;
    }
    if (!user && campaign?.createdBy) {
        user = await db.collection("users").findOne({ email: campaign.createdBy });
    }
    if (!user) {
        return { user: null, policy: "none", parentUserId: null, creatorUserId: null };
    }

    const creatorUserId = user._id ? String(user._id) : null;
    let parentUser = null;
    const role = String(user.role || "").toLowerCase();
    if (role === "user" && user.createdBy) {
        const parentOid = toObjectId(user.createdBy);
        if (parentOid) {
            parentUser = await db.collection("users").findOne({ _id: parentOid });
        }
    }

    // W3: per-org setting billingWallet parent|creator (default parent = W1)
    let policy = "parent";
    try {
        const orgId = parentUser?._id || (role !== "user" ? user._id : null);
        if (orgId) {
            const setting = await db.collection("systemsettings").findOne({
                key: "billingWalletV1",
                "value.orgUserId": String(orgId),
            });
            const mode = String(setting?.value?.mode || setting?.value || "")
                .trim()
                .toLowerCase();
            if (mode === "creator" || mode === "parent") policy = mode;
        }
    } catch {
        /* default parent */
    }

    const chosen = policy === "creator" || !parentUser ? user : parentUser;

    if (envOn("BILLING_WALLET_SHADOW", true)) {
        try {
            await db.collection("billing_wallet_shadow_log").insertOne({
                campaignId: campaign?._id || null,
                callLogUserId: callLogUserId ? String(callLogUserId) : null,
                creatorUserId,
                parentUserId: parentUser?._id ? String(parentUser._id) : null,
                chosenUserId: chosen?._id ? String(chosen._id) : null,
                policy,
                createdAt: new Date(),
            });
        } catch (err) {
            logger.warn("[BillingWallet] shadow log failed (ignored)", {
                error: err?.message || String(err),
            });
        }
    }

    return {
        user: chosen,
        policy,
        parentUserId: parentUser?._id ? String(parentUser._id) : null,
        creatorUserId,
    };
}

/**
 * On E11000, refund the userId recorded on the winning transaction (not the loser path).
 */
async function refundExistingBillingKeyWinner(db, { billingKey, cost, callId }) {
    const existing = await db.collection("credittransactions").findOne({
        type: "call_deduction",
        "reference.billingKey": String(billingKey),
    });
    if (!existing?.userId) {
        logger.error("[Credit] E11000 but no existing tx for billingKey — cannot refund safely", {
            callId,
            billingKey,
        });
        return { refunded: false };
    }
    const oid = toObjectId(existing.userId);
    if (!oid) return { refunded: false };
    await db.collection("users").updateOne(
        { _id: oid },
        { $inc: { credits: Number(cost) || 0 }, $set: { updatedAt: new Date() } }
    );
    logger.info("[Credit] Refunded winner wallet after E11000 duplicate billingKey", {
        callId,
        billingKey,
        refundedUserId: String(oid),
        refundedAmount: cost,
    });
    return { refunded: true, userId: oid };
}

module.exports = {
    resolveBillingUser,
    refundExistingBillingKeyWinner,
    envOn,
};
