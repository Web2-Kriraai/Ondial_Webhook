/**
 * Bill wizard TestCall rows via Ondial call-completed (TestCall collection, not CallLogs).
 */
const { ObjectId } = require("mongodb");
const { getDb } = require("../db");
const logger = require("../logger");
const { getOndialWebhookFetchHeaders } = require("./ondialWebhookFetch");

const TESTCALL_COLLECTION = process.env.TESTCALL_COLLECTION || "TestCall";

function testCallMatchFilter(callId, leadId) {
    const ids = [callId, leadId]
        .filter((x) => typeof x === "string" && x.trim())
        .map((x) => x.trim());
    const unique = [...new Set(ids)];
    if (unique.length === 0) return null;
    const or = [];
    for (const id of unique) {
        or.push({ call_id: id }, { lead_id: id });
    }
    return { $or: or };
}

function extractDurationFromEvents(callData) {
    const events = Array.isArray(callData?.events) ? callData.events : [];
    for (let i = events.length - 1; i >= 0; i -= 1) {
        const e = events[i];
        const t = String(e?.event_type || "").toLowerCase();
        if (t === "call_hangup" || t === "call.hangup" || t === "cdr_push") {
            const d = e?.data?.duration ?? e?.data?.data?.duration;
            const n = parseInt(d, 10);
            if (Number.isFinite(n) && n > 0) return n;
        }
    }
    return null;
}

function durationForWebhook(seconds) {
    const n =
        typeof seconds === "number" && Number.isFinite(seconds) ? Math.max(0, Math.floor(seconds)) : 0;
    return n < 1 ? 1 : n;
}

function ondialAppBaseUrl() {
    const u =
        process.env.ONDIAL_APP_URL ||
        process.env.INTERNAL_APP_URL ||
        process.env.NEXT_PUBLIC_APP_URL ||
        process.env.APP_BASE_URL ||
        "";
    return String(u).replace(/\/$/, "");
}

/**
 * @returns {Promise<{ outcome: string }>}
 */
async function tryFinalizeTestCallCredits({ callId, leadId, durationSec }) {
    if (process.env.ONDIAL_TEST_CALL_CREDIT_ENABLED === "0") {
        return { outcome: "disabled" };
    }

    const filter = testCallMatchFilter(callId, leadId);
    if (!filter) return { outcome: "no_call_id" };

    const db = getDb();
    const row = await db.collection(TESTCALL_COLLECTION).findOne(filter, {
        projection: {
            _id: 1,
            call_id: 1,
            lead_id: 1,
            call_data: 1,
            creditsDeducted: 1,
            campaign_id: 1,
            contact_id: 1,
        },
    });

    if (!row) return { outcome: "missing_doc" };
    if (row.creditsDeducted === true) return { outcome: "already_done" };

    let durationSeconds =
        durationSec != null && Number(durationSec) > 0
            ? Math.floor(Number(durationSec))
            : extractDurationFromEvents(row.call_data);
    if (durationSeconds == null || durationSeconds <= 0) {
        return { outcome: "not_ready" };
    }

    const billSeconds = durationForWebhook(durationSeconds);
    const call_data =
        row.call_data && typeof row.call_data === "object"
            ? { ...row.call_data, events: Array.isArray(row.call_data.events) ? row.call_data.events : [] }
            : { events: [] };

    await db.collection(TESTCALL_COLLECTION).updateOne(
        { _id: row._id },
        { $set: { call_data, status: "completed", updatedAt: new Date() } }
    );

    const baseUrl = ondialAppBaseUrl();
    if (!baseUrl) {
        logger.warn("[TestCallCredit] ONDIAL_APP_URL not set — cannot POST call-completed");
        return { outcome: "no_app_url" };
    }

    let isBasic = false;
    const campId = row.campaign_id;
    if (campId && ObjectId.isValid(String(campId))) {
        const campaign = await db
            .collection("campaigns")
            .findOne({ _id: new ObjectId(String(campId)) }, { projection: { "selectedVoice.tier": 1 } });
        isBasic = campaign?.selectedVoice?.tier === "basic";
    }

    const endpoint = isBasic ? "basic-call-completed" : "call-completed";
    const url = `${baseUrl}/api/webhooks/${endpoint}`;
    const webhookCallId = row.call_id || callId || leadId;
    const bodyStr = JSON.stringify({
        call_id: webhookCallId,
        campaign_id: row.campaign_id,
        contact_id: row.contact_id,
        status: "completed",
        call_data,
        source: "ondial-webhook-hangup",
    });

    const res = await fetch(url, {
        method: "POST",
        headers: getOndialWebhookFetchHeaders(bodyStr),
        body: bodyStr,
    });

    if (!res.ok) {
        const text = await res.text().catch(() => "");
        logger.warn("[TestCallCredit] call-completed failed", {
            status: res.status,
            call_id: webhookCallId,
            body: text.slice(0, 200),
        });
        return { outcome: "webhook_failed", status: res.status };
    }

    const after = await db.collection(TESTCALL_COLLECTION).findOne(
        { _id: row._id },
        { projection: { creditsDeducted: 1 } }
    );
    if (after?.creditsDeducted === true) {
        return { outcome: "webhook_ok" };
    }
    return { outcome: "webhook_accepted_pending" };
}

module.exports = { tryFinalizeTestCallCredits };
