/**
 * One-shot verification for pending inbound/outbound credit cases.
 * Creates temporary docs, restores credits, cleans up.
 *
 * Run: node scripts/verify-inbound-credit-cases.js
 */
require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });

const { ObjectId } = require("mongodb");
const { connectDB, getDb } = require("../db");
const { tryDeductCampaignCallCredits } = require("../lib/campaignCreditDeduction");
const {
    callSidLookupVariants,
    syncInboundCompletionFields,
} = require("../lib/inboundDocAnchor");
const { resolveInboundBillingContext } = require("../lib/resolveInboundBillingContext");

const USER_ID = "68d7749bc50f55ec1dfa4198"; // niya@gmail.com
const INBOUND_CFG = "6a60a31ade716bfd02071920"; // Shreya
const DID = "+919484957197";

const results = [];

function pass(name, detail) {
    results.push({ name, ok: true, detail: detail || "" });
    console.log(`PASS  ${name}${detail ? ` — ${detail}` : ""}`);
}

function fail(name, detail) {
    results.push({ name, ok: false, detail: detail || "" });
    console.error(`FAIL  ${name}${detail ? ` — ${detail}` : ""}`);
}

function assert(name, cond, detail) {
    if (cond) pass(name, detail);
    else fail(name, detail);
}

(async () => {
    await connectDB();
    const db = getDb();
    const uid = new ObjectId(USER_ID);
    const userBefore = await db.collection("users").findOne({ _id: uid }, { projection: { credits: 1, email: 1 } });
    if (!userBefore) {
        console.error("User not found");
        process.exit(1);
    }
    const creditsStart = Number(userBefore.credits) || 0;
    console.log(`User ${userBefore.email} creditsStart=${creditsStart}`);

    const tag = `verify_${Date.now()}`;
    const providerUuid = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0001";
    const callSidHex = callSidLookupVariants(providerUuid).find((v) => v.startsWith("call_"));
    const validateCallId = new ObjectId().toString();
    const createdIds = [];

    try {
        // ── 1) Zero / missing duration skip ──────────────────────────────
        {
            const r0 = await tryDeductCampaignCallCredits({
                callUniqueId: `${tag}_dur0`,
                inboundConfigId: INBOUND_CFG,
                durationSec: 0,
                callLogCollectionName: "InboundConversation",
                userId: USER_ID,
            });
            assert("zero duration → skip_no_duration", r0.outcome === "skip_no_duration", r0.outcome);

            const rNeg = await tryDeductCampaignCallCredits({
                callUniqueId: `${tag}_durnull`,
                inboundConfigId: INBOUND_CFG,
                durationSec: null,
                callLogCollectionName: "InboundConversation",
                userId: USER_ID,
            });
            assert("null duration → skip_no_duration", rNeg.outcome === "skip_no_duration", rNeg.outcome);

            const creditsAfterSkip = (
                await db.collection("users").findOne({ _id: uid }, { projection: { credits: 1 } })
            )?.credits;
            assert(
                "duration skip does not change balance",
                Number(creditsAfterSkip) === creditsStart,
                `credits=${creditsAfterSkip}`
            );
        }

        // ── 2) Billing context resolves inbound config (not fake campaign) ─
        {
            const validateDoc = {
                _id: new ObjectId(validateCallId),
                call_id: validateCallId,
                call_sid: providerUuid, // UUID form (UI sometimes stores this)
                config_id: new ObjectId(INBOUND_CFG),
                userId: USER_ID,
                source: "validate",
                to_number: DID,
                from_number: "+919999999999",
                status: "completed",
                duration: 45,
                creditsDeducted: false,
                creditsDeductedAmount: 0,
                creditDeductionError: null,
                createdAt: new Date(),
                updatedAt: new Date(),
                startedAt: new Date(Date.now() - 45000),
                endedAt: new Date(),
            };
            await db.collection("InboundConversation").insertOne(validateDoc);
            createdIds.push(validateDoc._id);

            const stubDoc = {
                call_id: providerUuid,
                call_sid: callSidHex,
                config_id: new ObjectId(INBOUND_CFG),
                userId: USER_ID,
                to_number: DID,
                from_number: "+919999999999",
                status: "completed",
                duration: 45,
                creditsDeducted: false,
                createdAt: new Date(),
                updatedAt: new Date(),
            };
            const stubIns = await db.collection("InboundConversation").insertOne(stubDoc);
            createdIds.push(stubIns.insertedId);

            const billing = await resolveInboundBillingContext({
                anchor: { doc: validateDoc, callSid: callSidHex, userId: USER_ID },
                toPhone: DID,
            });
            assert(
                "billing context has inboundConfigId",
                billing.inboundConfigId === INBOUND_CFG,
                JSON.stringify(billing)
            );
            assert(
                "billing context does NOT treat config as campaignId",
                !billing.campaignId || billing.campaignId !== INBOUND_CFG,
                `campaignId=${billing.campaignId}`
            );
        }

        // ── 3) Full inbound deduct + UI sync (UUID + call_hex) ────────────
        {
            const first = await tryDeductCampaignCallCredits({
                callUniqueId: providerUuid,
                campaignId: null,
                inboundConfigId: INBOUND_CFG,
                durationSec: 45,
                callLogCollectionName: "InboundConversation",
                callLogLookupFilter: { call_sid: callSidHex },
                userId: USER_ID,
            });
            assert(
                "inbound config deduct succeeds",
                first.outcome === "deducted" && Number(first.cost) > 0,
                JSON.stringify(first)
            );

            await syncInboundCompletionFields({
                callSid: callSidHex,
                toPhone: DID,
                fromPhone: "+919999999999",
                providerCallId: providerUuid,
                durationSec: 45,
                creditFields: {
                    creditsDeducted: true,
                    creditDeductionError: null,
                    creditsDeductedAmount: first.cost,
                    status: "completed",
                },
                maxAttempts: 2,
                retryMs: 50,
            });

            const validateAfter = await db.collection("InboundConversation").findOne({
                call_id: validateCallId,
            });
            const stubAfter = await db.collection("InboundConversation").findOne({
                call_sid: callSidHex,
                call_id: providerUuid,
            });

            assert(
                "validate/UI row creditsDeducted after sync",
                validateAfter?.creditsDeducted === true && Number(validateAfter?.creditsDeductedAmount) > 0,
                JSON.stringify({
                    creditsDeducted: validateAfter?.creditsDeducted,
                    amount: validateAfter?.creditsDeductedAmount,
                    error: validateAfter?.creditDeductionError,
                })
            );
            assert(
                "stub row creditsDeducted",
                stubAfter?.creditsDeducted === true,
                `stub=${stubAfter?.creditsDeducted}`
            );

            const midCredits = Number(
                (await db.collection("users").findOne({ _id: uid }, { projection: { credits: 1 } }))
                    ?.credits
            );
            assert(
                "balance dropped after inbound deduct",
                midCredits < creditsStart && Math.abs(creditsStart - midCredits - first.cost) < 1e-6,
                `start=${creditsStart} mid=${midCredits} cost=${first.cost}`
            );

            // ── 4) Idempotency (2nd hangup) ───────────────────────────────
            const second = await tryDeductCampaignCallCredits({
                callUniqueId: providerUuid,
                inboundConfigId: INBOUND_CFG,
                durationSec: 45,
                callLogCollectionName: "InboundConversation",
                callLogLookupFilter: { call_sid: callSidHex },
                userId: USER_ID,
            });
            assert(
                "second hangup → already_billed",
                second.outcome === "already_billed",
                second.outcome
            );

            const afterIdem = Number(
                (await db.collection("users").findOne({ _id: uid }, { projection: { credits: 1 } }))
                    ?.credits
            );
            assert(
                "idempotent hangup does not double-charge",
                Math.abs(afterIdem - midCredits) < 1e-9,
                `mid=${midCredits} after=${afterIdem}`
            );

            // restore credits from this successful deduct so later tests start clean
            await db.collection("users").updateOne(
                { _id: uid },
                { $inc: { credits: first.cost }, $set: { updatedAt: new Date() } }
            );
        }

        // ── 5) Insufficient credits ──────────────────────────────────────
        {
            const insuffCallId = `insuff_${tag}`;
            const insuffSid = `call_insuff${Date.now().toString(16).padStart(24, "0").slice(0, 24)}`;
            // pad to valid call_ hex length
            const hex = Date.now().toString(16).padStart(32, "0").slice(-32);
            const sid = `call_${hex}`;
            const insuffIns = await db.collection("InboundConversation").insertOne({
                call_id: insuffCallId,
                call_sid: sid,
                config_id: new ObjectId(INBOUND_CFG),
                userId: USER_ID,
                to_number: DID,
                source: "validate",
                duration: 120,
                creditsDeducted: false,
                createdAt: new Date(),
                updatedAt: new Date(),
            });
            createdIds.push(insuffIns.insertedId);

            const snap = await db.collection("users").findOne({ _id: uid }, { projection: { credits: 1 } });
            await db.collection("users").updateOne({ _id: uid }, { $set: { credits: 0.000001 } });

            const insuff = await tryDeductCampaignCallCredits({
                callUniqueId: insuffCallId,
                inboundConfigId: INBOUND_CFG,
                durationSec: 120,
                callLogCollectionName: "InboundConversation",
                callLogLookupFilter: { call_sid: sid },
                userId: USER_ID,
            });
            assert(
                "insufficient_credits outcome",
                insuff.outcome === "insufficient_credits",
                JSON.stringify(insuff)
            );

            const insuffDoc = await db.collection("InboundConversation").findOne({ _id: insuffIns.insertedId });
            assert(
                "insufficient_credits marked on doc",
                insuffDoc?.creditDeductionError === "insufficient_credits" &&
                    insuffDoc?.creditsDeducted !== true,
                JSON.stringify({
                    err: insuffDoc?.creditDeductionError,
                    deducted: insuffDoc?.creditsDeducted,
                })
            );

            const creditsDuring = Number(
                (await db.collection("users").findOne({ _id: uid }, { projection: { credits: 1 } }))
                    ?.credits
            );
            assert(
                "insufficient path does not deduct",
                creditsDuring <= 0.000001 + 1e-9,
                `credits=${creditsDuring}`
            );

            // restore original snap credits (approx — we restored first.cost already above)
            await db.collection("users").updateOne(
                { _id: uid },
                { $set: { credits: Number(snap.credits), updatedAt: new Date() } }
            );
        }

        // ── 6) Outbound campaign deduct regression ───────────────────────
        {
            const recentCamp = await db.collection("campaigns").findOne(
                { createdBy: "niya@gmail.com", selectedPhoneNumber: { $exists: true, $ne: null } },
                { sort: { updatedAt: -1, createdAt: -1 } }
            );
            if (!recentCamp) {
                fail("outbound campaign found", "no campaign for niya");
            } else {
                const outCallId = `out_${tag}`;
                const outIns = await db.collection("CallLogs").insertOne({
                    lead_id: outCallId,
                    call_id: outCallId,
                    campaign_id: String(recentCamp._id),
                    contact_id: "",
                    call_data: { events: [] },
                    isTestCall: false,
                    creditsDeducted: false,
                    createdAt: new Date().toISOString(),
                    updatedAt: new Date(),
                    to_number: recentCamp.selectedPhoneNumber,
                });
                createdIds.push({ coll: "CallLogs", _id: outIns.insertedId });

                const balBeforeOut = Number(
                    (await db.collection("users").findOne({ _id: uid }, { projection: { credits: 1 } }))
                        ?.credits
                );

                const out = await tryDeductCampaignCallCredits({
                    callUniqueId: outCallId,
                    campaignId: String(recentCamp._id),
                    durationSec: 30,
                    callLogCollectionName: "CallLogs",
                    userId: USER_ID,
                });
                assert(
                    "outbound campaign deduct succeeds",
                    out.outcome === "deducted" && Number(out.cost) > 0,
                    JSON.stringify({ ...out, campaign: String(recentCamp._id) })
                );

                const out2 = await tryDeductCampaignCallCredits({
                    callUniqueId: outCallId,
                    campaignId: String(recentCamp._id),
                    durationSec: 30,
                    callLogCollectionName: "CallLogs",
                    userId: USER_ID,
                });
                assert(
                    "outbound second deduct → already_billed",
                    out2.outcome === "already_billed",
                    out2.outcome
                );

                const balAfterOut = Number(
                    (await db.collection("users").findOne({ _id: uid }, { projection: { credits: 1 } }))
                        ?.credits
                );
                assert(
                    "outbound charged once only",
                    Math.abs(balBeforeOut - balAfterOut - out.cost) < 1e-6,
                    `before=${balBeforeOut} after=${balAfterOut} cost=${out.cost}`
                );

                // restore outbound charge
                await db.collection("users").updateOne(
                    { _id: uid },
                    { $inc: { credits: out.cost }, $set: { updatedAt: new Date() } }
                );

                // cleanup outbound tx so billingKey doesn't linger confusingly (optional)
                await db.collection("credittransactions").deleteMany({
                    "reference.billingKey": `${recentCamp._id}:call:${outCallId}`,
                });
            }
        }

        // ── 7) Purani failed calls inventory (no backfill — report only) ──
        {
            const failedCount = await db.collection("InboundConversation").countDocuments({
                creditDeductionError: "campaign_not_found",
                creditsDeducted: { $ne: true },
                duration: { $gt: 0 },
            });
            const niyaFailed = await db.collection("InboundConversation").countDocuments({
                $or: [{ userId: uid }, { userId: USER_ID }],
                creditDeductionError: "campaign_not_found",
                creditsDeducted: { $ne: true },
                duration: { $gt: 0 },
            });
            pass(
                "purani failed inventory (report only, not backfilled)",
                `all=${failedCount}, niya=${niyaFailed}`
            );
        }
    } finally {
        // cleanup temp inbound docs
        for (const id of createdIds) {
            if (id && id.coll === "CallLogs") {
                await db.collection("CallLogs").deleteOne({ _id: id._id });
            } else if (id) {
                await db.collection("InboundConversation").deleteOne({ _id: id });
            }
        }
        // cleanup verify credit txs for inbound provider uuid
        await db.collection("credittransactions").deleteMany({
            "reference.callId": providerUuid,
        });
        await db.collection("credittransactions").deleteMany({
            "reference.callId": { $regex: `^insuff_verify_` },
        });

        const endCredits = Number(
            (await db.collection("users").findOne({ _id: uid }, { projection: { credits: 1 } }))
                ?.credits
        );
        // best-effort restore to start if drift
        if (Math.abs(endCredits - creditsStart) > 0.0001) {
            await db.collection("users").updateOne(
                { _id: uid },
                { $set: { credits: creditsStart, updatedAt: new Date() } }
            );
            console.log(`Restored user credits to ${creditsStart} (was ${endCredits})`);
        } else {
            console.log(`Credits unchanged net: ${endCredits}`);
        }
    }

    console.log("\n======== SUMMARY ========");
    const failed = results.filter((r) => !r.ok);
    for (const r of results) {
        console.log(`${r.ok ? "OK" : "XX"}  ${r.name}${r.detail ? ` | ${r.detail}` : ""}`);
    }
    console.log(
        failed.length
            ? `\n${failed.length} FAILED / ${results.length} total`
            : `\nAll ${results.length} checks passed`
    );
    process.exit(failed.length ? 1 : 0);
})().catch((e) => {
    console.error(e);
    process.exit(1);
});
