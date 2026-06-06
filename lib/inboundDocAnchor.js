/**
 * Link telephony provider call.id (UUID) to Ondial InboundConversation.call_sid (call_<hex>).
 */
const logger = require("../logger");
const { INBOUNDCALLLOG_COLLECTION } = require("./inboundCall");
const {
    extractConfigIdFromDoc,
    isMongoObjectIdString,
    objectIdToString,
} = require("./mongoObjectId");
const { phoneVariants } = require("./resolveInboundBillingContext");

function isProviderUuidCallId(callId) {
    return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
        String(callId || "").trim()
    );
}

function isLikelyValidateUiDoc(doc) {
    if (!doc) return false;
    if (doc.source === "validate") return true;
    if (objectIdToString(doc.userId) && isMongoObjectIdString(String(doc.call_id))) return true;
    if (Array.isArray(doc.conversation?.transcript) && doc.conversation.transcript.length) return true;
    if (Array.isArray(doc.transcript) && doc.transcript.length) return true;
    return false;
}

function isWebhookStubDoc(doc) {
    if (!doc || isLikelyValidateUiDoc(doc)) return false;
    return isProviderUuidCallId(doc.call_id);
}

function scoreInboundDoc(doc) {
    if (!doc) return -1;
    let score = 0;
    if (doc.source === "validate") score += 200;
    if (objectIdToString(doc.userId) && isMongoObjectIdString(String(doc.call_id))) score += 180;
    if (extractConfigIdFromDoc(doc)) score += 50;
    if (objectIdToString(doc.userId)) score += 40;
    if (doc.call_id && isMongoObjectIdString(String(doc.call_id))) score += 30;
    if (doc.conversation?.transcript || doc.transcript) score += 20;
    if (doc.to_number || doc.phone_number) score += 10;
    if (isProviderUuidCallId(doc.call_id)) score -= 20;
    return score;
}

function pickBestInboundDoc(docs) {
    if (!Array.isArray(docs) || docs.length === 0) return null;
    const uiDocs = docs.filter(isLikelyValidateUiDoc);
    const pool = uiDocs.length ? uiDocs : docs.filter((d) => !isWebhookStubDoc(d));
    const finalPool = pool.length ? pool : docs;
    return finalPool.reduce((best, doc) =>
        scoreInboundDoc(doc) > scoreInboundDoc(best) ? doc : best
    );
}

/** Unique filter — validate/internal call_id wins; stubs share updates via call_sid. */
function inboundDocFilterFor(doc) {
    if (!doc) return null;
    if (isLikelyValidateUiDoc(doc) && isMongoObjectIdString(String(doc.call_id))) {
        return { call_id: String(doc.call_id) };
    }
    if (doc.call_sid) return { call_sid: String(doc.call_sid) };
    if (doc._id) return { _id: doc._id };
    return { call_id: String(doc.call_id) };
}

/** 92bd645e-e976-4a14-969a-5176d7fed547 → call_92bd645ee9764a14969a5176d7fed547 */
function providerCallIdToCallSid(providerCallId) {
    const raw = String(providerCallId || "").trim();
    if (!raw) return null;
    if (/^call_[a-f0-9]{32}$/i.test(raw)) return raw.toLowerCase();
    const hex = raw.replace(/-/g, "").toLowerCase();
    if (!/^[a-f0-9]{32}$/.test(hex)) return null;
    return `call_${hex}`;
}

/** call_92bd645ee9764a14969a5176d7fed547 → 92bd645e-e976-4a14-969a-5176d7fed547 */
function callSidToProviderCallId(callSid) {
    const m = String(callSid || "")
        .trim()
        .match(/^call_([a-f0-9]{32})$/i);
    if (!m) return null;
    const h = m[1];
    return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

function recentInboundTimeFilter(minutes = 30) {
    const cutoff = new Date(Date.now() - minutes * 60 * 1000);
    const iso = cutoff.toISOString();
    return {
        $or: [
            { updatedAt: { $gte: cutoff } },
            { startedAt: { $gte: cutoff } },
            { createdAt: { $gte: iso } },
            { createdAt: { $gte: cutoff } },
        ],
    };
}

function inboundPhoneOrFilter(toPhone) {
    const toVars = phoneVariants(toPhone);
    if (!toVars.length) return null;
    return {
        $or: [
            { to_number: { $in: toVars } },
            { phone_number: { $in: toVars } },
            { to: { $in: toVars } },
        ],
    };
}

async function findValidateCompanionDoc(db, { callSid, toPhone, fromPhone }) {
    const candidates = [];

    if (callSid) {
        const bySid = await db
            .collection(INBOUNDCALLLOG_COLLECTION)
            .find({ call_sid: callSid })
            .toArray();
        candidates.push(...bySid.filter(isLikelyValidateUiDoc));
    }

    const phoneOr = inboundPhoneOrFilter(toPhone);
    if (phoneOr) {
        const toVars = phoneVariants(toPhone);
        const fromVars = fromPhone ? phoneVariants(fromPhone) : [];
        const baseQuery = {
            $and: [phoneOr, recentInboundTimeFilter(30)],
        };

        let byPhone = await db
            .collection(INBOUNDCALLLOG_COLLECTION)
            .find(baseQuery)
            .sort({ updatedAt: -1, createdAt: -1, startedAt: -1 })
            .limit(10)
            .toArray();
        byPhone = byPhone.filter(isLikelyValidateUiDoc);
        if (fromVars.length) {
            const narrowed = byPhone.filter((doc) => {
                const fromVal = doc.from_number || doc.from || doc.phone_from;
                if (!fromVal) return true;
                const docFromVars = phoneVariants(fromVal);
                return docFromVars.some((v) => fromVars.includes(v));
            });
            if (narrowed.length) byPhone = narrowed;
        }
        candidates.push(...byPhone);
    }

    return pickBestInboundDoc(candidates);
}

function preferredPhoneForDoc(raw) {
    const variants = phoneVariants(raw);
    return variants.find((v) => v.startsWith("+")) || variants[0] || null;
}

async function ensureInboundDocMetadata(db, doc, { callSid, toPhone, fromPhone, campaignId, userId }) {
    if (!doc || isLikelyValidateUiDoc(doc)) return;
    const sid = String(callSid || doc.call_sid || "").trim();
    if (!sid) return;

    const set = {};
    const to = preferredPhoneForDoc(toPhone);
    const from = preferredPhoneForDoc(fromPhone);
    if (to && !doc.to_number && !doc.phone_number) set.to_number = to;
    if (from && !doc.from_number) set.from_number = from;
    if (campaignId && !extractConfigIdFromDoc(doc)) set.config_id = String(campaignId);
    if (userId && !objectIdToString(doc.userId)) set.userId = String(userId);

    if (!Object.keys(set).length) return;
    set.updatedAt = new Date();
    await db.collection(INBOUNDCALLLOG_COLLECTION).updateMany({ call_sid: sid }, { $set: set });
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function mergeInboundDocs(primary, extra) {
    const out = [...(primary || [])];
    for (const doc of extra || []) {
        if (!doc?._id) continue;
        if (out.some((d) => String(d._id) === String(doc._id))) continue;
        out.push(doc);
    }
    return out;
}

/**
 * Find the Ondial InboundConversation row (validate / UI) without /api/inbound-mapping.
 * @returns {Promise<{ doc, filter, syncFilter, providerCallId, callSid, campaignId, contactId, userId }>}
 */
async function resolveInboundConversationAnchor(providerCallId, context = {}) {
    const providerId = String(providerCallId || "").trim();
    const callSid = providerCallIdToCallSid(providerId);
    const hyphenated = callSidToProviderCallId(callSid) || providerId;
    const { toPhone, fromPhone } = context;

    const empty = {
        doc: null,
        filter: callSid ? { call_sid: callSid } : { call_id: hyphenated },
        syncFilter: callSid ? { call_sid: callSid } : null,
        providerCallId: hyphenated,
        callSid,
        campaignId: null,
        contactId: null,
        userId: null,
    };

    if (!providerId && !callSid) return empty;

    try {
        const { getDb } = require("../db");
        const db = getDb();
        const or = [{ call_id: providerId }, { call_id: hyphenated }];
        if (callSid) or.push({ call_sid: callSid });

        let docs = await db.collection(INBOUNDCALLLOG_COLLECTION).find({ $or: or }).toArray();
        const validateDoc = await findValidateCompanionDoc(db, { callSid, toPhone, fromPhone });
        if (validateDoc) {
            docs = mergeInboundDocs(docs, [validateDoc]);
        }

        // Ondial validate row always wins over webhook stub (same call_sid race).
        const doc = validateDoc || pickBestInboundDoc(docs);

        if (!doc) {
            return empty;
        }

        const filter = inboundDocFilterFor(doc);
        const syncFilter = doc.call_sid || callSid ? { call_sid: String(doc.call_sid || callSid) } : filter;

        if (doc.source === "validate" && callSid && String(doc.call_sid || "") !== callSid) {
            await db.collection(INBOUNDCALLLOG_COLLECTION).updateOne(filter, {
                $set: {
                    call_sid: callSid,
                    provider_call_id: hyphenated,
                    updatedAt: new Date(),
                },
            });
        }

        if (doc && !isLikelyValidateUiDoc(doc)) {
            await ensureInboundDocMetadata(db, doc, {
                callSid,
                toPhone,
                fromPhone,
                campaignId: context.campaignId,
                userId: context.userId,
            });
        }

        return {
            doc,
            filter,
            syncFilter,
            providerCallId: hyphenated,
            callSid: doc.call_sid ? String(doc.call_sid) : callSid,
            campaignId: extractConfigIdFromDoc(doc),
            contactId: objectIdToString(doc.contact_id),
            userId: objectIdToString(doc.userId),
        };
    } catch (err) {
        logger.warn("[InboundAnchor] lookup failed", {
            providerCallId: providerId,
            error: err.message,
        });
        return empty;
    }
}

/** Push hangup fields onto validate row even when call_sid is linked after webhook billing. */
async function syncInboundCompletionFields({
    callSid,
    toPhone,
    fromPhone,
    providerCallId,
    durationSec,
    recordingUrl,
    creditFields,
    maxAttempts = 16,
    retryMs = 500,
    quiet = false,
}) {
    const { getDb } = require("../db");
    const db = getDb();
    const set = { updatedAt: new Date() };

    const dur = Math.max(0, Math.floor(Number(durationSec) || 0));
    if (dur > 0) {
        set.duration = dur;
        set.duration_ms = dur * 1000;
    }
    const rec = recordingUrl != null ? String(recordingUrl).trim() : "";
    if (rec) set.recordingUrl = rec;
    if (creditFields && typeof creditFields === "object") {
        Object.assign(set, creditFields);
    }

    const hyphenated = String(providerCallId || "").trim();
    if (callSid) {
        set.call_sid = String(callSid);
    }
    if (hyphenated) {
        set.provider_call_id = hyphenated;
    }

    let syncedValidate = false;
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
        const validateDoc = await findValidateCompanionDoc(db, { callSid, toPhone, fromPhone });
        if (validateDoc) {
            const filter = inboundDocFilterFor(validateDoc);
            await db.collection(INBOUNDCALLLOG_COLLECTION).updateOne(filter, { $set: set });
            if (!quiet) {
                logger.info("[InboundAnchor] Synced completion to validate doc", {
                    call_id: validateDoc.call_id,
                    call_sid: callSid || null,
                    attempt,
                });
            }
            syncedValidate = true;
            break;
        }
        if (attempt < maxAttempts - 1) {
            await sleep(retryMs);
        }
    }

    if (callSid) {
        await db.collection(INBOUNDCALLLOG_COLLECTION).updateMany(
            { call_sid: String(callSid) },
            { $set: set }
        );
    }

    if (!syncedValidate && toPhone && !quiet) {
        logger.warn("[InboundAnchor] Validate doc not found for completion sync", {
            call_sid: callSid || null,
            to: toPhone,
            note: "billing fields were synced to call_sid-linked docs only",
        });
    }

    return { syncedValidate };
}

const deferredSyncKeys = new Set();

/** Retry validate sync when Ondial writes the UI row after telephony webhooks. */
function scheduleDeferredInboundCompletionSync(params, delaysMs = [8000, 20000, 45000]) {
    const key = String(params.callSid || params.providerCallId || "").trim();
    if (!key || deferredSyncKeys.has(key)) return;
    deferredSyncKeys.add(key);
    for (const delay of delaysMs) {
        setTimeout(async () => {
            try {
                const result = await syncInboundCompletionFields({ ...params, quiet: true });
                if (result.syncedValidate) {
                    logger.info("[InboundAnchor] Deferred validate sync succeeded", {
                        call_sid: params.callSid || null,
                        delay_ms: delay,
                    });
                    deferredSyncKeys.delete(key);
                }
            } catch (err) {
                logger.warn("[InboundAnchor] Deferred validate sync failed", {
                    call_sid: params.callSid || null,
                    error: err.message,
                });
            }
        }, delay);
    }
    setTimeout(() => deferredSyncKeys.delete(key), Math.max(...delaysMs) + 5000);
}

module.exports = {
    providerCallIdToCallSid,
    callSidToProviderCallId,
    resolveInboundConversationAnchor,
    inboundDocFilterFor,
    pickBestInboundDoc,
    findValidateCompanionDoc,
    syncInboundCompletionFields,
    scheduleDeferredInboundCompletionSync,
    isLikelyValidateUiDoc,
};
