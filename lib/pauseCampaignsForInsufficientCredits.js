/**
 * Pause active outbound campaigns when post-call billing hits insufficient credits.
 * Mongo-native; mirrors Calling_system1 shared-lib/campaignCreditPause.js field writes.
 */

function slotsHeldByCampaign(campaign) {
    const v = campaign?.concurrentCalls;
    if (v === 0 || v === "0") return 0;
    const n = Number(v);
    if (Number.isFinite(n) && n >= 1) return Math.min(Math.floor(n), 1_000_000);
    return 0;
}

function buildOwnerMatch(user) {
    const or = [];
    if (user?.email) or.push({ createdBy: user.email });
    if (user?._id) {
        or.push({ userId: user._id });
        or.push({ userId: String(user._id) });
    }
    return or.length ? { $or: or } : null;
}

/**
 * @returns {Promise<{ pausedCount: number, campaignIds: string[] }>}
 */
async function pauseActiveCampaignsForInsufficientCredits(db, { user } = {}) {
    if (!db || !user) return { pausedCount: 0, campaignIds: [] };

    const ownerMatch = buildOwnerMatch(user);
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
        if (prevSlots > 0) {
            set.concurrentCallsBeforePause = prevSlots;
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
        console.warn(
            `[Credit] Paused ${campaignIds.length} active campaign(s) for ${user.email || user._id} after insufficient_credits`
        );
    }

    return { pausedCount: campaignIds.length, campaignIds };
}

module.exports = { pauseActiveCampaignsForInsufficientCredits };
