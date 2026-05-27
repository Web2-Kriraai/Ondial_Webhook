/**
 * Wizard test calls: Python/API call_id often differs from telephony provider call.id.
 * Match recent TestCall by to_number + campaign shell, then link provider id for billing.
 */
const { getDb } = require("../db");
const { pickNonEmpty } = require("./customParameters");
const { registerCallMapping, normalizePhone } = require("../callMapping");
const logger = require("../logger");

const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";
const CALLLOGS_COLLECTION = process.env.CALLLOGS_COLLECTION || "CallLogs";
const LOOKBACK_MS = Number(process.env.TEST_CALL_PHONE_LINK_LOOKBACK_MS || 15 * 60 * 1000);

function phoneVariants(raw) {
    const n = normalizePhone(raw);
    if (!n) return [];
    const out = new Set();
    if (raw != null && String(raw).trim()) out.add(String(raw).trim());
    out.add(n);
    if (n.length === 10) out.add(`91${n}`);
    if (n.startsWith("91") && n.length === 12) out.add(n.slice(2));
    return [...out];
}

/**
 * @returns {Promise<{ campaign_id: string, contact_id: string, lead_id: string } | null>}
 */
async function linkRecentTestCallByPhone({ providerCallId, toPhone }) {
    const providerId = String(providerCallId || "").trim();
    const variants = phoneVariants(toPhone);
    if (!providerId || variants.length === 0) return null;

    try {
        const db = getDb();
        const since = new Date(Date.now() - LOOKBACK_MS);
        const testDoc = await db.collection(TESTCALL_COLLECTION).findOne(
            {
                to_number: { $in: variants },
                campaign_id: { $exists: true, $ne: "" },
                createdAt: { $gte: since },
            },
            { sort: { createdAt: -1 } }
        );

        if (!testDoc?.campaign_id) return null;

        const campaign_id = String(testDoc.campaign_id);
        const contact_id = testDoc.contact_id != null ? String(testDoc.contact_id) : "";
        const canonicalLead = pickNonEmpty(testDoc.lead_id, testDoc.call_id) || providerId;

        await db.collection(TESTCALL_COLLECTION).updateOne(
            { _id: testDoc._id },
            {
                $set: {
                    provider_call_id: providerId,
                    updatedAt: new Date(),
                },
            }
        );

        await registerCallMapping({
            call_id: providerId,
            lead_id: canonicalLead,
            campaign_id,
            contact_id,
            phone: normalizePhone(toPhone) || "",
            collectionName: CALLLOGS_COLLECTION,
        });

        const identitySet = {
            campaign_id,
            contact_id,
            isTestCall: true,
            call_id: providerId,
            provider_call_id: providerId,
            updatedAt: new Date(),
        };

        await db.collection(CALLLOGS_COLLECTION).updateOne(
            { $or: [{ lead_id: providerId }, { call_id: providerId }] },
            {
                $set: identitySet,
                $setOnInsert: {
                    lead_id: providerId,
                    call_data: { events: [] },
                    createdAt: new Date().toISOString(),
                    recordingUrl: "",
                },
            },
            { upsert: true }
        );

        if (canonicalLead !== providerId) {
            await db.collection(CALLLOGS_COLLECTION).updateOne(
                { lead_id: String(canonicalLead) },
                {
                    $set: {
                        ...identitySet,
                        provider_call_id: providerId,
                    },
                }
            );
        }

        logger.info("[Webhook] Linked provider call to recent TestCall by phone", {
            provider_call_id: providerId,
            test_call_id: testDoc.call_id,
            canonical_lead_id: canonicalLead,
            campaign_id,
            contact_id,
            to: variants[0],
        });

        return { campaign_id, contact_id, lead_id: canonicalLead };
    } catch (err) {
        logger.warn("[Webhook] linkRecentTestCallByPhone failed", { error: err.message, providerCallId });
        return null;
    }
}

module.exports = { linkRecentTestCallByPhone };
