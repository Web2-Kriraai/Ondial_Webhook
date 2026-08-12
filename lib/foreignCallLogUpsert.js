/**
 * Foreign (Twilio / Telnyx) CallLog identity: one document per carrier id.
 *
 * Why: carrier status webhooks arrive with CallSid / call_control_id before the dialer
 * UUID is mapped. India can key only on call_unique_id because every event echoes it;
 * foreign status callbacks do not. Carrier-id primary avoids UUID-shell + synthetic races.
 *
 * Dialer UUID (when known via mapping) is stored as lead_id / call_id / call_unique_id
 * on the same document for billing + UI parity with India.
 */
const logger = require("../logger");
const { normalizeCallId } = require("../callMapping");
const { findCallLogsByIdentity } = require("./findCallLogsByIdentity");

const UUID_RE =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function isDuplicateKeyError(err) {
    const msg = String(err?.message || "");
    return err?.code === 11000 || /E11000|duplicate key/i.test(msg);
}

function isUuidCallKey(value) {
    return typeof value === "string" && UUID_RE.test(value.trim());
}

/** Dialer call_unique_id (UUID) or other non-carrier keys from mapping — never CallSid / v3:… */
function isDialerCallKey(value, carrierId) {
    if (value == null) return false;
    const v = String(value).trim();
    if (!v || (carrierId && v === String(carrierId).trim())) return false;
    if (v.startsWith("twilio:") || v.startsWith("telnyx:")) return false;
    if (/^CA[0-9a-f]{32}$/i.test(v)) return false;
    if (v.startsWith("v3:")) return false;
    if (isUuidCallKey(v)) return true;
    // Worker/Ondial always send dialer UUID in prod; allow any non-carrier mapping id in tests.
    return v.length >= 8;
}

function resolveDialerId({ externalCall, externalLead, existing, carrierId }) {
    if (isDialerCallKey(externalCall, carrierId)) return String(externalCall).trim();
    if (isDialerCallKey(externalLead, carrierId)) return String(externalLead).trim();
    if (existing && isDialerCallKey(existing.lead_id, carrierId)) return String(existing.lead_id).trim();
    if (existing && isDialerCallKey(existing.call_id, carrierId)) return String(existing.call_id).trim();
    if (existing && isDialerCallKey(existing.call_unique_id, carrierId)) {
        return String(existing.call_unique_id).trim();
    }
    return "";
}

async function mergeForeignOrphanIntoKeeper(coll, { keeperId, orphan, carrierNs, now, logMeta }) {
    if (!keeperId || !orphan?._id) return;
    if (String(keeperId) === String(orphan._id)) return;

    const orphanEvents = Array.isArray(orphan.call_data?.events) ? orphan.call_data.events : [];
    if (orphanEvents.length) {
        await coll.updateOne(
            { _id: keeperId },
            { $push: { "call_data.events": { $each: orphanEvents } } }
        );
    }

    const mergeSet = { updatedAt: now };
    if (orphan.recordingUrl) mergeSet.recordingUrl = orphan.recordingUrl;
    if (orphan.conversation) mergeSet.conversation = orphan.conversation;
    if (orphan.to_number) mergeSet.to_number = orphan.to_number;
    if (orphan.contactName) mergeSet.contactName = orphan.contactName;
    if (orphan.isTestCall === true) mergeSet.isTestCall = true;
    if (orphan.creditsDeducted === true) mergeSet.creditsDeducted = true;
    if (orphan.creditsDeductedAmount != null) {
        mergeSet.creditsDeductedAmount = orphan.creditsDeductedAmount;
    }

    const nested = orphan[carrierNs];
    if (nested && typeof nested === "object") {
        const skipKey = carrierNs === "twilio" ? "call_sid" : "call_control_id";
        for (const [k, v] of Object.entries(nested)) {
            if (k === skipKey) continue;
            if (v != null && v !== "") mergeSet[`${carrierNs}.${k}`] = v;
        }
    }

    if (Object.keys(mergeSet).length > 1) {
        await coll.updateOne({ _id: keeperId }, { $set: mergeSet });
    }
    await coll.deleteOne({ _id: orphan._id });
    logger.info("[CallLog] Merged foreign orphan into carrier CallLog", {
        ...logMeta,
        keeperId: String(keeperId),
        orphanId: String(orphan._id),
    });
}

/**
 * @param {object} opts
 * @param {import("mongodb").Collection} opts.coll
 * @param {"twilio"|"telnyx"} opts.carrierNs
 * @param {string} opts.carrierField  e.g. "twilio.call_sid"
 * @param {string} opts.carrierId
 * @param {string} opts.syntheticLeadId
 * @param {object} opts.setFields
 * @param {object} opts.eventDoc
 * @param {object} opts.rootFromMapping
 */
async function upsertCarrierAnchoredCallLog({
    coll,
    carrierNs,
    carrierField,
    carrierId,
    syntheticLeadId,
    setFields,
    eventDoc,
    rootFromMapping,
}) {
    const campaignId = rootFromMapping.campaign_id != null ? String(rootFromMapping.campaign_id) : "";
    const contactId = rootFromMapping.contact_id != null ? String(rootFromMapping.contact_id) : "";
    const externalLead = rootFromMapping.lead_id != null ? String(rootFromMapping.lead_id).trim() : "";
    const externalCallRaw = rootFromMapping.call_id != null ? String(rootFromMapping.call_id).trim() : "";
    const externalCall = externalCallRaw ? normalizeCallId(externalCallRaw) || externalCallRaw : "";
    const now = new Date();

    // Carrier doc is always the keeper (unique sparse index on carrier id).
    let sidOwner = (await coll.findOne({ [carrierField]: carrierId })) || null;

    const dialerId = resolveDialerId({
        externalCall,
        externalLead,
        existing: sidOwner,
        carrierId,
    });

    // Phase 1 — ensure a carrier-anchored doc exists WITHOUT taking dialer lead_id yet
    // (partial unique index on lead_id would conflict with an existing UUID shell).
    // Concurrent upserts on the same carrier id hit unique sparse index → E11000;
    // loser reloads the winner and continues (never a second CallLog).
    if (!sidOwner) {
        for (let attempt = 1; attempt <= 4; attempt += 1) {
            try {
                await coll.updateOne(
                    { [carrierField]: carrierId },
                    {
                        $set: {
                            [carrierField]: carrierId,
                            lead_id: syntheticLeadId,
                            call_id: carrierId,
                            call_direction: "outbound",
                            provider_call_id: carrierId,
                            updatedAt: now,
                        },
                        $setOnInsert: {
                            createdAt: now,
                            recordingUrl: "",
                            call_data: { events: [] },
                        },
                    },
                    { upsert: true }
                );
                break;
            } catch (err) {
                if (!isDuplicateKeyError(err) || attempt === 4) throw err;
                logger.warn("[CallLog] Carrier upsert race; reloading keeper", {
                    attempt,
                    [carrierField]: carrierId,
                });
            }
        }
        sidOwner = await coll.findOne({ [carrierField]: carrierId });
    }

    const keeperId = sidOwner?._id;
    if (!keeperId) {
        throw new Error(`Failed to create carrier CallLog for ${carrierField}=${carrierId}`);
    }

    async function mergeOrphansIntoKeeper() {
        // Carrier field + synthetic lead are indexed (twilio/telnyx sparse + lead_id).
        // Dialer UUID uses sequential indexed finds — avoid packing call_unique_id into one $or COLLSCAN.
        const orphanOr = [{ [carrierField]: carrierId }, { lead_id: syntheticLeadId }, { call_id: carrierId }];
        if (externalLead && externalLead !== dialerId) {
            orphanOr.push({ lead_id: externalLead }, { call_id: externalLead });
        }

        const [carrierOrphans, dialerOrphans] = await Promise.all([
            coll
                .find({ _id: { $ne: keeperId }, $or: orphanOr })
                .project({
                    _id: 1,
                    call_data: 1,
                    recordingUrl: 1,
                    conversation: 1,
                    twilio: 1,
                    telnyx: 1,
                    creditsDeducted: 1,
                    creditsDeductedAmount: 1,
                    isTestCall: 1,
                    to_number: 1,
                    contactName: 1,
                })
                .toArray(),
            dialerId
                ? findCallLogsByIdentity(
                      coll,
                      { callId: dialerId, callUniqueId: dialerId, leadId: dialerId, includeCarrier: false },
                      { limitPerQuery: 20, includeCarrier: false }
                  )
                : Promise.resolve([]),
        ]);

        const byId = new Map();
        for (const orphan of [...carrierOrphans, ...dialerOrphans]) {
            if (!orphan?._id || String(orphan._id) === String(keeperId)) continue;
            byId.set(String(orphan._id), orphan);
        }
        const orphans = [...byId.values()];

        for (const orphan of orphans) {
            await mergeForeignOrphanIntoKeeper(coll, {
                keeperId,
                orphan,
                carrierNs,
                now,
                logMeta: { [carrierField]: carrierId, dialerId: dialerId || null },
            });
        }
    }

    // Phase 2 — free unique lead_id by merging UUID-only shells into the keeper first.
    await mergeOrphansIntoKeeper();

    // Phase 3 — attach dialer UUID + mapping fields + this event onto the single keeper.
    const $set = {
        ...setFields,
        [carrierField]: carrierId,
        call_direction: "outbound",
        updatedAt: now,
        provider_call_id: carrierId,
    };

    if (dialerId) {
        $set.lead_id = dialerId;
        $set.call_id = dialerId;
        $set.call_unique_id = dialerId;
        $set[`${carrierNs}.external_call_id`] = dialerId;
    } else {
        $set.lead_id = syntheticLeadId;
        $set.call_id = carrierId;
        if (externalCall) {
            $set[`${carrierNs}.external_call_id`] = externalCall;
            $set.call_unique_id = externalCall;
        }
    }

    if (campaignId) $set.campaign_id = campaignId;
    if (contactId) $set.contact_id = contactId;
    if (externalLead) $set[`${carrierNs}.external_lead_id`] = externalLead;

    let result = null;
    for (let attempt = 1; attempt <= 4; attempt += 1) {
        try {
            // Identity first (no $push) so a lead_id unique retry cannot duplicate the event.
            result = await coll.updateOne({ _id: keeperId }, { $set });
            break;
        } catch (err) {
            // lead_id unique: another shell appeared between merge and $set — merge again.
            if (!isDuplicateKeyError(err) || attempt === 4) throw err;
            logger.warn("[CallLog] lead_id claim race; re-merging orphans", {
                attempt,
                [carrierField]: carrierId,
                dialerId: dialerId || null,
            });
            await mergeOrphansIntoKeeper();
        }
    }

    await coll.updateOne({ _id: keeperId }, { $push: { "call_data.events": eventDoc } });

    return result.upsertedCount > 0 || result.modifiedCount > 0 || result.matchedCount > 0;
}

module.exports = {
    isUuidCallKey,
    isDialerCallKey,
    upsertCarrierAnchoredCallLog,
    resolveDialerId,
};
