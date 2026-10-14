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
 * W3: per-org systemsettings billingWalletV1 { value: { orgUserId, mode: parent|creator } }.
 * @returns {Promise<"parent"|"creator">} default "parent" (W1) when unset or on error.
 */
async function readBillingWalletMode(db, orgId) {
    if (!orgId) return "parent";
    try {
        const setting = await db.collection("systemsettings").findOne({
            key: "billingWalletV1",
            "value.orgUserId": String(orgId),
        });
        const mode = String(setting?.value?.mode || setting?.value || "")
            .trim()
            .toLowerCase();
        if (mode === "creator" || mode === "parent") return mode;
    } catch {
        /* default parent */
    }
    return "parent";
}

/**
 * Apply the org wallet rule to an already-loaded user (no shadow log):
 * org user (role "user" + createdBy) → parent admin, unless the org is in creator mode.
 * Individuals / admins pay themselves.
 */
async function applyBillingWalletPolicy(db, user) {
    if (!user) return null;
    const role = String(user.role || "").toLowerCase();
    if (role !== "user" || !user.createdBy) return user;
    const parentOid = toObjectId(user.createdBy);
    if (!parentOid) return user;
    const parentUser = await db.collection("users").findOne({ _id: parentOid });
    if (!parentUser) return user;
    const policy = await readBillingWalletMode(db, parentUser._id);
    return policy === "creator" ? user : parentUser;
}

/**
 * @returns {Promise<{ user: object|null, policy: string, parentUserId: string|null, creatorUserId: string|null, creatorUser: object|null }>}
 * `creatorUser` is the user resolution started from (the org member), before the wallet rule.
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
        return { user: null, policy: "none", parentUserId: null, creatorUserId: null, creatorUser: null };
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
    const orgId = parentUser?._id || (role !== "user" ? user._id : null);
    const policy = await readBillingWalletMode(db, orgId);

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
        creatorUser: user,
    };
}

/**
 * On E11000, refund the userId recorded on the winning transaction (not the loser path).
 * `cost` must be the amount THIS (losing) attempt actually debited. With partial charges
 * that can be less than the full call cost; 0 means nothing was debited, so no refund.
 */
async function refundExistingBillingKeyWinner(db, { billingKey, cost, callId }) {
    const refundAmount = Number(cost) || 0;
    if (refundAmount <= 0) return { refunded: false, reason: "nothing_charged" };
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
        { $inc: { credits: refundAmount }, $set: { updatedAt: new Date() } }
    );
    logger.info("[Credit] Refunded winner wallet after E11000 duplicate billingKey", {
        callId,
        billingKey,
        refundedUserId: String(oid),
        refundedAmount: refundAmount,
    });
    return { refunded: true, userId: oid, amount: refundAmount };
}

module.exports = {
    resolveBillingUser,
    readBillingWalletMode,
    applyBillingWalletPolicy,
    refundExistingBillingKeyWinner,
    envOn,
};
