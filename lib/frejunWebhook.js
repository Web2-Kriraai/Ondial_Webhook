/**
 * Frejun Voice App webhooks (api_version 2026-06-01).
 * Events carry call_id (cs_…) only — no custom_parameters.
 * Correlation: Redis map:frejun:id:{cs_} seeded by POST /api/frejun-mapping after initiate.
 */
const { getDb } = require("../db");
const { getRedis } = require("../redis");
const logger = require("../logger");
const {
    normalizeFrejunCallId,
    lookupFrejunCallIdMapping,
    markCallAnswered,
    hasAnsweredFlag,
    normalizeCallId,
    normalizePhone,
} = require("../callMapping");
const {
    upsertFrejunAnchoredCallLog,
    buildFrejunStatusEvent,
    mergeFrejunStatusIntoCallLog,
    resolveOutboundCollection,
} = require("../callLogs");
const { emitCallUpdateSse } = require("../events");

/** Lazy require — avoid partial exports if this module loads during webhookHandler init. */
function getWebhookHelpers() {
    return require("../webhookHandler");
}

const FREJUN_MAPPING_RETRY_MS = Math.max(
    50,
    Number(process.env.FREJUN_MAPPING_RETRY_MS || 150)
);
const FREJUN_MAPPING_RETRY_ATTEMPTS = Math.max(
    1,
    Number(process.env.FREJUN_MAPPING_RETRY_ATTEMPTS || 4)
);

function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
}

function extractFrejunCallIdFromBody(body) {
    if (!body || typeof body !== "object") return null;
    return (
        normalizeFrejunCallId(body.call_id) ||
        normalizeFrejunCallId(body.data?.call_id) ||
        normalizeFrejunCallId(body.data?.id) ||
        null
    );
}

function extractFrejunEventType(body) {
    return String(body?.type || body?.event || "").trim().toLowerCase();
}

/**
 * @returns {Promise<boolean>} true if first time seeing this event id
 */
async function claimFrejunWebhookEventId(eventId) {
    const id = String(eventId || "").trim();
    if (!id) return true;
    try {
        const redis = getRedis();
        const key = `frejun:webhook:event:${id}`;
        const ttlSec = Math.max(
            3600,
            Number(process.env.FREJUN_WEBHOOK_EVENT_TTL_SEC || 86400) || 86400
        );
        const set = await redis.set(key, "1", "EX", ttlSec, "NX");
        return set === "OK";
    } catch (err) {
        logger.warn("[Frejun] Event dedupe Redis failed — processing anyway", {
            event_id: id,
            error: err.message,
        });
        return true;
    }
}

async function lookupFrejunMappingWithRetry(frejunCallId) {
    let mapping = await lookupFrejunCallIdMapping(frejunCallId);
    if (mapping?.contact_id) return mapping;
    for (let i = 1; i < FREJUN_MAPPING_RETRY_ATTEMPTS; i += 1) {
        await sleep(FREJUN_MAPPING_RETRY_MS * i);
        mapping = await lookupFrejunCallIdMapping(frejunCallId);
        if (mapping?.contact_id) return mapping;
    }
    return mapping;
}

async function frejunCallWasAnswered({ frejunCallId, dialerCallId, toPhone, collectionName }) {
    const keys = [frejunCallId, dialerCallId].filter(Boolean);
    for (const key of keys) {
        try {
            if (await hasAnsweredFlag(key, toPhone)) return true;
        } catch (_) {
            /* ignore */
        }
    }
    try {
        const db = getDb();
        const coll = db.collection(collectionName || (await resolveOutboundCollection()));
        const doc =
            (await coll.findOne({ "frejun.call_id": frejunCallId })) ||
            (dialerCallId
                ? await coll.findOne({
                      $or: [
                          { call_unique_id: dialerCallId },
                          { call_id: dialerCallId },
                          { lead_id: dialerCallId },
                      ],
                  })
                : null);
        if (doc?.frejun?.answeredAt) return true;
        const events = Array.isArray(doc?.call_data?.events) ? doc.call_data.events : [];
        return events.some((e) => {
            const ty = String(e?.event_type || e?.data?.event_type || "").toLowerCase();
            return ty === "call.answered" || ty === "call_answered" || ty.includes("answered");
        });
    } catch (err) {
        logger.warn("[Frejun] answered-history lookup failed", {
            frejun_call_id: frejunCallId,
            error: err.message,
        });
        return false;
    }
}

async function appendFrejunEvent({
    frejunCallId,
    mapping,
    eventType,
    eventId,
    occurredAt,
    data,
    frejunSetFields,
    durationSec,
}) {
    const collectionName =
        (mapping?.collectionName && String(mapping.collectionName).trim()) ||
        (await resolveOutboundCollection());
    const dialerCallId = normalizeCallId(mapping?.call_id) || String(mapping?.call_id || "").trim();
    const leadId = String(mapping?.lead_id || dialerCallId || "").trim();
    const eventDoc = buildFrejunStatusEvent({
        frejunCallId,
        eventType,
        eventId,
        durationSec: durationSec == null ? null : durationSec,
        timestampIso: occurredAt || new Date().toISOString(),
        data: {
            ...(data && typeof data === "object" ? data : {}),
            call_id: dialerCallId || null,
            lead_id: leadId || null,
            campaign_id: mapping?.campaign_id || null,
            contact_id: mapping?.contact_id || null,
        },
    });

    const setPayload = {
        "frejun.call_id": frejunCallId,
        "frejun.last_event_type": eventType,
        "frejun.last_event_id": eventId || null,
        "frejun.updatedAt": new Date().toISOString(),
        ...(frejunSetFields || {}),
    };
    if (dialerCallId) {
        setPayload.call_unique_id = dialerCallId;
        setPayload["frejun.external_call_id"] = dialerCallId;
    }

    try {
        await upsertFrejunAnchoredCallLog({
            collectionName,
            frejunCallId,
            frejunSetFields: setPayload,
            eventDoc,
            rootFromMapping: {
                campaign_id: mapping?.campaign_id || "",
                contact_id: mapping?.contact_id || "",
                lead_id: leadId,
                call_id: dialerCallId || "",
            },
        });
    } catch (err) {
        logger.warn("[Frejun] upsertFrejunAnchoredCallLog failed; merge fallback", {
            frejun_call_id: frejunCallId,
            error: err.message,
        });
        await mergeFrejunStatusIntoCallLog(
            collectionName,
            { "frejun.call_id": frejunCallId },
            setPayload,
            eventDoc
        );
    }

    return { collectionName, dialerCallId, leadId };
}

/**
 * Process one Frejun webhook body after HTTP ack.
 */
async function processFrejunWebhook(body) {
    const eventType = extractFrejunEventType(body);
    const frejunCallId = extractFrejunCallIdFromBody(body);
    const eventId = body?.id != null ? String(body.id).trim() : "";
    const occurredAt = body?.occurred_at || body?.data?.occurred_at || new Date().toISOString();
    const data = body?.data && typeof body.data === "object" ? body.data : {};
    const toPhone = data.to || body.to || null;
    const fromPhone = data.from || body.from || null;

    if (!frejunCallId) {
        logger.info("[Frejun] Ignoring webhook without cs_ call_id", {
            event_type: eventType || null,
            event_id: eventId || null,
        });
        return { outcome: "skip_no_call_id" };
    }

    if (eventId) {
        const claimed = await claimFrejunWebhookEventId(eventId);
        if (!claimed) {
            logger.info("[Frejun] Duplicate event id skipped", { event_id: eventId });
            return { outcome: "duplicate_event" };
        }
    }

    const mapping = await lookupFrejunMappingWithRetry(frejunCallId);
    if (!mapping?.contact_id && !mapping?.campaign_id) {
        logger.warn("[Frejun] No mapping for cs_ — skipping contact update", {
            frejun_call_id: frejunCallId,
            event_type: eventType,
            event_id: eventId || null,
        });
        // Still append a minimal CallLog event when possible so ops can diagnose.
        try {
            await appendFrejunEvent({
                frejunCallId,
                mapping: { call_id: "", lead_id: "", campaign_id: "", contact_id: "" },
                eventType: eventType || "unknown",
                eventId,
                occurredAt,
                data,
                frejunSetFields: {
                    "frejun.status": "unmapped",
                    ...(fromPhone ? { "frejun.from": String(fromPhone) } : {}),
                    ...(toPhone ? { "frejun.to": String(toPhone) } : {}),
                },
            });
        } catch (_) {
            /* ignore */
        }
        return { outcome: "skip_no_mapping" };
    }

    const contactId = mapping.contact_id != null ? String(mapping.contact_id).trim() : "";
    const campaignId = mapping.campaign_id != null ? String(mapping.campaign_id).trim() : "";
    const dialerCallId =
        normalizeCallId(mapping.call_id) || String(mapping.call_id || "").trim() || "";
    const durationSec =
        data.duration_seconds != null && Number.isFinite(Number(data.duration_seconds))
            ? Math.max(0, Math.floor(Number(data.duration_seconds)))
            : null;

    const baseSet = {
        ...(fromPhone ? { "frejun.from": String(fromPhone) } : {}),
        ...(toPhone ? { "frejun.to": String(toPhone) } : {}),
        ...(data.direction ? { "frejun.direction": String(data.direction) } : {}),
        ...(body.leg_id ? { "frejun.leg_id": String(body.leg_id) } : {}),
    };

    const emitSse = (status, eventLabel) => {
        emitCallUpdateSse({
            campaign_id: campaignId || null,
            call_id: dialerCallId || frejunCallId,
            contact_id: contactId || null,
            status: status != null ? status : null,
            event: eventLabel || eventType,
            provider: "frejun",
        });
    };

    switch (eventType) {
        case "call.initiated": {
            await appendFrejunEvent({
                frejunCallId,
                mapping,
                eventType,
                eventId,
                occurredAt,
                data,
                frejunSetFields: {
                    ...baseSet,
                    "frejun.status": "ringing",
                    ...(data.start_time ? { "frejun.start_time": String(data.start_time) } : {}),
                },
            });
            // Same as receiver call_initiated: worker already set CRS=1 — SSE only.
            emitSse(1, "call_initiated");
            return { outcome: "initiated" };
        }

        case "call.answered": {
            const answeredAt = data.answered_at || occurredAt;
            try {
                await markCallAnswered(dialerCallId || frejunCallId, toPhone);
                if (dialerCallId && dialerCallId !== frejunCallId) {
                    await markCallAnswered(frejunCallId, toPhone);
                }
            } catch (err) {
                logger.warn("[Frejun] markCallAnswered failed", { error: err.message });
            }
            await appendFrejunEvent({
                frejunCallId,
                mapping,
                eventType,
                eventId,
                occurredAt,
                data,
                frejunSetFields: {
                    ...baseSet,
                    "frejun.status": "in-progress",
                    "frejun.answeredAt": answeredAt,
                },
            });
            if (contactId) {
                const { updateByContactId } = getWebhookHelpers();
                await updateByContactId(contactId, 2, "frejun_call_answered");
            }
            emitSse(2, "call_answered");
            return { outcome: "answered" };
        }

        case "call.completed": {
            const { updateByContactId, processOutboundHangupBilling } = getWebhookHelpers();
            const collectionName =
                (mapping.collectionName && String(mapping.collectionName).trim()) ||
                (await resolveOutboundCollection());
            const answered =
                (await frejunCallWasAnswered({
                    frejunCallId,
                    dialerCallId,
                    toPhone,
                    collectionName,
                })) ||
                (durationSec != null && durationSec > 0);
            const hangupStatus = answered ? 3 : 1;
            const dur = durationSec != null ? durationSec : 0;

            await appendFrejunEvent({
                frejunCallId,
                mapping,
                eventType,
                eventId,
                occurredAt,
                data,
                durationSec: dur,
                frejunSetFields: {
                    ...baseSet,
                    "frejun.status": hangupStatus === 3 ? "completed" : "no-answer",
                    "frejun.completedAt": data.ended_at || occurredAt,
                    "frejun.duration": dur,
                    ...(data.reason ? { "frejun.reason": String(data.reason) } : {}),
                    ...(data.ended_by ? { "frejun.ended_by": String(data.ended_by) } : {}),
                },
            });

            if (contactId) {
                await updateByContactId(
                    contactId,
                    hangupStatus,
                    `frejun_call_completed duration=${dur}s answered=${answered ? "yes" : "no"}`
                );
            }
            emitSse(hangupStatus, "call_hangup");

            if (hangupStatus === 3 && dur > 0) {
                const callUniqueForFinalize = dialerCallId || frejunCallId;
                await processOutboundHangupBilling({
                    identity: {
                        campaign_id: campaignId || null,
                        contact_id: contactId || null,
                        lead_id: mapping.lead_id || dialerCallId || null,
                        normalizedCallId: callUniqueForFinalize,
                        call_unique_id: callUniqueForFinalize,
                        collectionName,
                    },
                    contact_id: contactId || null,
                    lead_id: mapping.lead_id || dialerCallId || null,
                    callUniqueForFinalize,
                    durationSec: dur,
                    recordingUrl: null,
                    callStatus: hangupStatus === 3 ? "ANSWER" : null,
                    toPhone: toPhone ? normalizePhone(toPhone) || toPhone : null,
                });
            }
            return { outcome: hangupStatus === 3 ? "completed" : "no_answer", durationSec: dur };
        }

        case "call.failed": {
            const { updateByContactId } = getWebhookHelpers();
            const hasFailure = data.failure && typeof data.failure === "object";
            const reason = String(data.reason || data.failure?.reason || "").toLowerCase();
            const softFail =
                !hasFailure &&
                (reason.includes("no_answer") ||
                    reason.includes("busy") ||
                    reason.includes("no-answer") ||
                    reason === "busy");
            const failStatus = softFail ? 1 : 0;
            const dur = durationSec != null ? durationSec : 0;

            await appendFrejunEvent({
                frejunCallId,
                mapping,
                eventType,
                eventId,
                occurredAt,
                data,
                durationSec: dur,
                frejunSetFields: {
                    ...baseSet,
                    "frejun.status": "failed",
                    "frejun.completedAt": data.ended_at || occurredAt,
                    "frejun.duration": dur,
                    ...(data.reason ? { "frejun.reason": String(data.reason) } : {}),
                    ...(hasFailure ? { "frejun.failure": data.failure } : {}),
                },
            });

            if (contactId) {
                await updateByContactId(contactId, failStatus, "frejun_call_failed");
            }
            emitSse(failStatus, "call_failed");
            return { outcome: "failed", status: failStatus };
        }

        case "call.transfer.initiated":
        case "call.transfer.bridged":
        case "call.transfer.failed": {
            const { updateByContactId } = getWebhookHelpers();
            await appendFrejunEvent({
                frejunCallId,
                mapping,
                eventType,
                eventId,
                occurredAt,
                data,
                frejunSetFields: {
                    ...baseSet,
                    "frejun.status": "in-progress",
                    "frejun.transfer": {
                        transfer_id: data.transfer_id || null,
                        mode: data.mode || null,
                        reason: data.reason || null,
                        initiated_by: data.initiated_by || null,
                        bridged_at: data.bridged_at || null,
                        on_failure_executed: data.on_failure_executed ?? null,
                    },
                },
            });
            if (contactId) {
                await updateByContactId(contactId, 2, `frejun_${eventType}`);
            }
            emitSse(2, "call_transfer");
            return { outcome: "transfer" };
        }

        case "stream.initiated":
        case "stream.completed": {
            await appendFrejunEvent({
                frejunCallId,
                mapping,
                eventType,
                eventId,
                occurredAt,
                data,
                durationSec:
                    data.duration_seconds != null ? Number(data.duration_seconds) : null,
                frejunSetFields: {
                    ...baseSet,
                    ...(data.stream_id ? { "frejun.stream_id": String(data.stream_id) } : {}),
                    ...(data.ws_url ? { "frejun.ws_url": String(data.ws_url) } : {}),
                },
            });
            emitSse(null, eventType);
            return { outcome: "stream" };
        }

        case "recording.completed": {
            const recordingUrl =
                data.recording_url != null ? String(data.recording_url).trim() : "";
            const recordingId =
                data.recording_id != null ? String(data.recording_id).trim() : "";
            const frejunSetFields = {
                ...baseSet,
                ...(recordingId ? { "frejun.recording_id": recordingId } : {}),
                ...(recordingUrl ? { "frejun.recording_url": recordingUrl, recordingUrl } : {}),
            };
            await appendFrejunEvent({
                frejunCallId,
                mapping,
                eventType,
                eventId,
                occurredAt,
                data,
                frejunSetFields,
            });
            if (recordingUrl) {
                try {
                    const db = getDb();
                    const coll = db.collection(await resolveOutboundCollection());
                    await coll.updateOne(
                        { "frejun.call_id": frejunCallId },
                        {
                            $set: {
                                recordingUrl,
                                "frejun.recording_url": recordingUrl,
                                ...(recordingId ? { "frejun.recording_id": recordingId } : {}),
                                updatedAt: new Date(),
                            },
                        }
                    );
                } catch (err) {
                    logger.warn("[Frejun] recordingUrl patch failed", {
                        frejun_call_id: frejunCallId,
                        error: err.message,
                    });
                }
            }
            emitSse(null, "recording.completed");
            return { outcome: "recording_completed" };
        }

        case "recording.failed": {
            await appendFrejunEvent({
                frejunCallId,
                mapping,
                eventType,
                eventId,
                occurredAt,
                data,
                frejunSetFields: {
                    ...baseSet,
                    ...(data.recording_id
                        ? { "frejun.recording_id": String(data.recording_id) }
                        : {}),
                    ...(data.failure ? { "frejun.recording_failure": data.failure } : {}),
                },
            });
            emitSse(null, "recording.failed");
            return { outcome: "recording_failed" };
        }

        default: {
            await appendFrejunEvent({
                frejunCallId,
                mapping,
                eventType: eventType || "unknown",
                eventId,
                occurredAt,
                data,
                frejunSetFields: baseSet,
            });
            logger.info("[Frejun] Unhandled event type", {
                event_type: eventType || null,
                frejun_call_id: frejunCallId,
            });
            return { outcome: "unhandled", event_type: eventType };
        }
    }
}

module.exports = {
    processFrejunWebhook,
    extractFrejunCallIdFromBody,
    extractFrejunEventType,
    claimFrejunWebhookEventId,
    normalizeFrejunCallId,
};
