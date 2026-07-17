const logger = require("../logger");
const { getDb } = require("../db");

/**
 * Marketing delivery / STOP updates (parity with Ondial dashboard webhook handler).
 * Call-follow-up AI replies are handled by Calling_system1 via aisensy-inbound queue.
 */

function normalizeEvents(payload) {
    if (Array.isArray(payload)) return payload;
    if (payload?.events) return payload.events;
    if (payload?.entry) {
        return payload.entry.flatMap((e) => e.changes?.map((c) => c.value) || []);
    }
    return [payload];
}

function normalizePhone(raw) {
    if (!raw) return "";
    return String(raw).replace(/[\s+\-()]/g, "");
}

function isStopMessage(text) {
    if (!text) return false;
    const normalized = String(text).trim().toUpperCase();
    return normalized === "STOP" || normalized === "UNSUBSCRIBE" || normalized === "OPT OUT";
}

function mapWebhookEvent(event) {
    const rawType = String(
        event.type || event.event || event.topic || event.status || ""
    ).toLowerCase();
    const type = normalizeAisensyWebhookType(rawType);
    const phone = normalizePhone(
        event.phone ||
            event.destination ||
            event.to ||
            event.from ||
            event.userNumber ||
            event.user_number ||
            event.contact?.phone ||
            event.data?.phone
    );
    const campaignName =
        event.campaignName ||
        event.campaign_name ||
        event.campaign ||
        event.data?.campaign_name ||
        "";
    const timestamp = event.timestamp ? new Date(event.timestamp) : new Date();

    const update = {
        phone,
        campaignName,
        lastEvent: type || rawType,
        lastEventAt: timestamp,
    };

    switch (type) {
        case "sent":
            update.sentAt = timestamp;
            break;
        case "delivered":
            update.deliveredAt = timestamp;
            break;
        case "read":
            update.readAt = timestamp;
            break;
        case "failed":
            update.failedAt = timestamp;
            update.failedReason = event.reason || event.error || event.errorMessage || "Delivery failed";
            break;
        case "replied":
            update.replyText =
                event.text ||
                event.message ||
                event.reply ||
                event.body ||
                event.data?.text ||
                event.data?.message ||
                "";
            break;
        default:
            break;
    }

    return { type: type || rawType, ...update };
}

function normalizeAisensyWebhookType(raw) {
    const t = String(raw || "").toLowerCase().trim();
    if (!t) return "";
    if (t === "sent" || t === "contact.campaign.sent") return "sent";
    if (t === "delivered" || t === "contact.campaign.delivered") return "delivered";
    if (t === "read" || t === "contact.campaign.read") return "read";
    if (t === "failed" || t.includes("failed")) return "failed";
    if (
        t === "replied" ||
        t === "message.sender.user" ||
        t === "inbound" ||
        t === "message_received"
    ) {
        return "replied";
    }
    if (t === "message.status.updated") return "status";
    return t;
}

async function processAisensyMarketingWebhook(payload) {
    const db = await getDb();
    const events = normalizeEvents(payload || {});

    for (const event of events) {
        const mapped = mapWebhookEvent(event || {});
        if (!mapped.phone) continue;

        const update = {
            lastEvent: mapped.type,
            lastEventAt: mapped.lastEventAt,
        };
        if (mapped.sentAt) update.sentAt = mapped.sentAt;
        if (mapped.deliveredAt) update.deliveredAt = mapped.deliveredAt;
        if (mapped.readAt) update.readAt = mapped.readAt;
        if (mapped.failedAt) {
            update.failedAt = mapped.failedAt;
            update.failedReason = mapped.failedReason;
        }
        if (mapped.replyText) update.replyText = mapped.replyText;

        if (mapped.campaignName) {
            await db.collection("whatsappcampaignlogs").findOneAndUpdate(
                { phone: mapped.phone, campaignName: mapped.campaignName },
                { $set: update },
                { sort: { createdAt: -1 } }
            );
        }

        await db.collection("whatsappcampaignlogs").findOneAndUpdate(
            { phone: mapped.phone },
            { $set: update },
            { sort: { createdAt: -1 } }
        );

        if (mapped.type === "replied" && isStopMessage(mapped.replyText)) {
            await db.collection("whatsappunsubscribes").updateOne(
                { phone: mapped.phone },
                {
                    $set: {
                        phone: mapped.phone,
                        source: "stop_reply",
                        campaignName: mapped.campaignName || null,
                        reason: "User replied STOP",
                        updatedAt: new Date(),
                    },
                    $setOnInsert: { createdAt: new Date() },
                },
                { upsert: true }
            );
        }
    }
}

function processAisensyMarketingWebhookSafe(payload) {
    processAisensyMarketingWebhook(payload).catch((err) => {
        logger.error("[AiSensy marketing] async error", { error: err.message });
    });
}

module.exports = {
    processAisensyMarketingWebhook,
    processAisensyMarketingWebhookSafe,
};
