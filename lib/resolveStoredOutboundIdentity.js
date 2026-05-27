/**
 * Recover campaign_id / contact_id when telephony only sends overrideWsUrl / overrideWebhookUrl.
 * Merges CallLogs + TestCall (wizard shell) and event history — never prefers empty CallLogs over TestCall.
 */
const { getDb } = require("../db");
const { pickNonEmpty, parseJsonObject } = require("./customParameters");
const { resolveCampaignIdFromContact } = require("./resolveCampaignId");
const { linkRecentTestCallByPhone } = require("./linkRecentTestCallByPhone");
const { lookupMapping } = require("../callMapping");
const logger = require("../logger");

const CALLLOGS_COLLECTION = process.env.CALLLOGS_COLLECTION || "CallLogs";
const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";

function callKeyFilter(key) {
    const k = String(key).trim();
    if (!k) return null;
    return { $or: [{ lead_id: k }, { call_id: k }] };
}

function identityFromEvents(events) {
    let contact_id = null;
    let campaign_id = null;
    if (!Array.isArray(events)) return { contact_id, campaign_id };

    for (const ev of events) {
        const data = ev?.data;
        if (!data || typeof data !== "object") continue;

        const cpRaw = data.customParameters || data.custom_parameters;
        const cp =
            (typeof cpRaw === "string" ? parseJsonObject(cpRaw) : null) ||
            (cpRaw && typeof cpRaw === "object" ? cpRaw : null);
        const fromCp =
            cp && typeof cp === "object"
                ? {
                      contact_id: pickNonEmpty(cp.contact_id, cp.contactId),
                      campaign_id: pickNonEmpty(cp.campaign_id, cp.campaignId),
                  }
                : null;

        contact_id = pickNonEmpty(contact_id, data.contact_id, data.contactId, fromCp?.contact_id);
        campaign_id = pickNonEmpty(campaign_id, data.campaign_id, data.campaignId, fromCp?.campaign_id);

        const raw = data._raw?.call?.customParameters || data.call?.customParameters;
        if (raw && typeof raw === "object") {
            contact_id = pickNonEmpty(contact_id, raw.contact_id, raw.contactId);
            campaign_id = pickNonEmpty(campaign_id, raw.campaign_id, raw.campaignId);
        }
    }

    return { contact_id, campaign_id };
}

function mergeDocFields(identity, doc) {
    if (!doc) return identity;
    return {
        ...identity,
        contact_id: pickNonEmpty(identity.contact_id, doc.contact_id),
        campaign_id: pickNonEmpty(identity.campaign_id, doc.campaign_id),
    };
}

/**
 * @param {object} identity
 * @param {{ backfillCallLog?: boolean }} [options]
 */
async function resolveStoredOutboundIdentity(identity, options = {}) {
    if (identity?.contact_id && identity?.campaign_id) return identity;

    const key = pickNonEmpty(identity?.lead_id, identity?.normalizedCallId, identity?.call_unique_id);
    if (!key) return identity;

    try {
        const db = getDb();
        const filter = callKeyFilter(key);
        if (!filter) return identity;

        const projection = { contact_id: 1, campaign_id: 1, "call_data.events": 1 };
        const [callLogDoc, testCallDoc] = await Promise.all([
            db.collection(CALLLOGS_COLLECTION).findOne(filter, { projection }),
            db.collection(TESTCALL_COLLECTION).findOne(filter, { projection }),
        ]);

        let merged = { ...identity };
        merged = mergeDocFields(merged, callLogDoc);
        merged = mergeDocFields(merged, testCallDoc);

        const fromCallLogEvents = identityFromEvents(callLogDoc?.call_data?.events);
        const fromTestEvents = identityFromEvents(testCallDoc?.call_data?.events);
        merged.contact_id = pickNonEmpty(
            merged.contact_id,
            fromCallLogEvents.contact_id,
            fromTestEvents.contact_id
        );
        merged.campaign_id = pickNonEmpty(
            merged.campaign_id,
            fromCallLogEvents.campaign_id,
            fromTestEvents.campaign_id
        );

        if (merged.contact_id && !merged.campaign_id) {
            merged.campaign_id = await resolveCampaignIdFromContact(merged.contact_id);
        }

        let mapping = await lookupMapping(key);
        if (!mapping?.campaign_id && identity?.providerCallId) {
            mapping = await lookupMapping(identity.providerCallId);
        }
        if (mapping) {
            merged.campaign_id = pickNonEmpty(merged.campaign_id, mapping.campaign_id);
            merged.contact_id = pickNonEmpty(merged.contact_id, mapping.contact_id);
        }

        if (!merged.campaign_id && merged.contact_id) {
            const byContact = await db.collection(TESTCALL_COLLECTION).findOne(
                { contact_id: String(merged.contact_id) },
                { sort: { createdAt: -1 }, projection: { campaign_id: 1, contact_id: 1 } }
            );
            if (byContact?.campaign_id) {
                merged.campaign_id = String(byContact.campaign_id);
            }
        }

        if (!merged.campaign_id && options.toPhone) {
            const linked = await linkRecentTestCallByPhone({
                providerCallId: key,
                toPhone: options.toPhone,
            });
            if (linked) {
                merged = {
                    ...merged,
                    campaign_id: pickNonEmpty(merged.campaign_id, linked.campaign_id),
                    contact_id: pickNonEmpty(merged.contact_id, linked.contact_id),
                    lead_id: pickNonEmpty(merged.lead_id, linked.lead_id),
                };
            }
        }

        if (!merged.contact_id && !merged.campaign_id) return identity;

        if (options.backfillCallLog !== false && merged.campaign_id) {
            const set = { updatedAt: new Date() };
            if (merged.campaign_id) set.campaign_id = String(merged.campaign_id);
            if (merged.contact_id) set.contact_id = String(merged.contact_id);
            await db.collection(CALLLOGS_COLLECTION).updateOne(filter, { $set: set });
        }

        logger.info("[Webhook] Recovered identity from stored docs", {
            call_key: key,
            contact_id: merged.contact_id || null,
            campaign_id: merged.campaign_id || null,
            sources: {
                callLogs: !!(callLogDoc?.campaign_id || callLogDoc?.contact_id),
                testCall: !!(testCallDoc?.campaign_id || testCallDoc?.contact_id),
            },
        });

        return merged;
    } catch (err) {
        logger.warn("[Webhook] resolveStoredOutboundIdentity failed", { error: err.message, key });
        return identity;
    }
}

module.exports = { resolveStoredOutboundIdentity };
