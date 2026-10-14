/**
 * Pause active outbound campaigns when post-call billing hits insufficient credits.
 * Mongo-native; mirrors Calling_system1 shared-lib/campaignCreditPause.js field writes.
 *
 * Scope is ORG-wide for the billed wallet: when an admin's wallet is exhausted, every
 * campaign billed to that wallet (admin + org users createdBy the admin) is paused.
 * Orgs in billingWalletV1 "creator" mode bill each user separately → only that user.
 */
const { ObjectId } = require("mongodb");
const logger = require("../logger");
const { readBillingWalletMode } = require("./resolveBillingUser");

function slotsHeldByCampaign(campaign) {
    const v = campaign?.concurrentCalls;
    if (v === 0 || v === "0") return 0;
    const n = Number(v);
    if (Number.isFinite(n) && n >= 1) return Math.min(Math.floor(n), 1_000_000);
    return 0;
}

function idVariants(id) {
    if (!id) return [];
    const s = String(id);
    const out = [s];
    if (id instanceof ObjectId) out.unshift(id);
    else if (ObjectId.isValid(s)) {
        try {
            out.unshift(new ObjectId(s));
        } catch {
            /* string only */
        }
    }
    return out;
}

/**
 * Users whose campaigns spend the billed user's wallet.
 * Billed user is an admin/individual in parent (default) mode → itself + its org users.
 * Billed user is an org user (creator mode / orphan) or org is in creator mode → itself only.
 */
async function resolveWalletMembers(db, user) {
    const members = [user];
    const role = String(user?.role || "").toLowerCase();
    if (!user?._id || (role === "user" && user.createdBy)) return members;
    try {
        const mode = await readBillingWalletMode(db, user._id);
        if (mode === "creator") return members;
        const children = await db
            .collection("users")
            .find({ role: "user", createdBy: { $in: idVariants(user._id) } })
            .project({ _id: 1, email: 1 })
            .toArray();
        for (const child of children || []) {
            if (child && String(child._id) !== String(user._id)) members.push(child);
        }
    } catch (err) {
        logger.warn("[Credit] Org member lookup for pause failed; pausing billed user only", {
            userId: String(user._id),
            error: err?.message || String(err),
        });
    }
    return members;
}

function buildOwnerMatch(members) {
    const emails = [];
    const ids = [];
    for (const m of members || []) {
        if (m?.email) emails.push(m.email);
        if (m?._id) ids.push(...idVariants(m._id));
    }
    const or = [];
    if (emails.length) or.push({ createdBy: { $in: emails } });
    if (ids.length) or.push({ userId: { $in: ids } });
    return or.length ? { $or: or } : null;
}

/**
 * @returns {Promise<{ pausedCount: number, campaignIds: string[] }>}
 */
async function pauseActiveCampaignsForInsufficientCredits(db, { user } = {}) {
    if (!db || !user) return { pausedCount: 0, campaignIds: [] };

    const members = await resolveWalletMembers(db, user);
    const ownerMatch = buildOwnerMatch(members);
    if (!ownerMatch) return { pausedCount: 0, campaignIds: [] };

    const campaigns = await db
        .collection("campaigns")
        .find({
            status: "active",
            archive: { $ne: true },
            ...ownerMatch,
        })
        .project({
            _id: 1,
            concurrentCalls: 1,
            concurrentCallsBeforePause: 1,
        })
        .toArray();

    if (!campaigns.length) return { pausedCount: 0, campaignIds: [] };

    const now = new Date();
    const campaignIds = [];

    for (const row of campaigns) {
        const prevSlots = slotsHeldByCampaign(row);
        const set = {
            status: "paused",
            isPaused: true,
            pausedForInsufficientCredits: true,
            pausedForScriptGeneration: false,
            concurrentCalls: 0,
            updatedAt: now,
        };
        // Preserve concurrentCallsBeforePause: use current live slots if positive,
        // otherwise keep the previously stored value (e.g. from an earlier pause cycle).
        if (prevSlots > 0) {
            set.concurrentCallsBeforePause = prevSlots;
        } else if (row.concurrentCallsBeforePause > 0) {
            // Already stored from a prior pause — do not overwrite with 0.
            // (no-op: omit from $set so the existing value is preserved)
        }

        const result = await db.collection("campaigns").updateOne(
            { _id: row._id, status: "active" },
            { $set: set }
        );
        if (result.matchedCount > 0) {
            campaignIds.push(String(row._id));
        }
    }

    if (campaignIds.length) {
        logger.warn("[Credit] Paused active campaign(s) after insufficient_credits", {
            pausedCount: campaignIds.length,
            billingUserId: user._id ? String(user._id) : null,
            billingUserEmail: user.email || null,
            orgMemberCount: members.length,
            campaignIds,
        });
    }

    return { pausedCount: campaignIds.length, campaignIds };
}

module.exports = { pauseActiveCampaignsForInsufficientCredits, buildOwnerMatch, resolveWalletMembers };
