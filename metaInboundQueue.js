const crypto = require("crypto");
const { Queue } = require("bullmq");
const { getBullRedis } = require("./redis");
const logger = require("./logger");

const QUEUE_NAME = process.env.WHATSAPP_META_INBOUND_QUEUE_NAME || "whatsapp-meta-inbound";
const MAX_RETRIES = Number(process.env.WHATSAPP_META_INBOUND_MAX_RETRIES || 5);
const BASE_RETRY_DELAY_MS = Number(process.env.WHATSAPP_META_INBOUND_BASE_RETRY_MS || 1000);

let queue = null;

function getQueue() {
    if (!queue) {
        queue = new Queue(QUEUE_NAME, { connection: getBullRedis() });
    }
    return queue;
}

function hashPayload(payload) {
    return crypto.createHash("sha256").update(JSON.stringify(payload || {})).digest("hex").slice(0, 40);
}

/**
 * Enqueue Meta Cloud API inbound webhook for the local BullMQ consumer
 * (whatsapp/metaInboundWorker.js) on the same Redis as this process.
 */
async function enqueueMetaWhatsappInbound(payload, meta = {}) {
    const q = getQueue();
    // Meta retries the exact same delivery when it does not receive a 2xx response.
    // Keep the id stable so BullMQ coalesces those retries while the completed job is retained.
    const jobId = `meta-wa-${hashPayload(payload)}`;
    try {
        await q.add(
            "whatsapp-meta-inbound",
            {
                payload,
                provider: "meta",
                meta: {
                    ...meta,
                    receivedAt: new Date().toISOString(),
                },
            },
            {
                jobId,
                attempts: MAX_RETRIES + 1,
                backoff: { type: "exponential", delay: BASE_RETRY_DELAY_MS },
                removeOnComplete: { age: 3600, count: 2000 },
                removeOnFail: { age: 24 * 3600 },
            }
        );
        return { accepted: true, jobId, queueName: QUEUE_NAME };
    } catch (err) {
        if (String(err.message || "").includes("already exists")) {
            logger.info("[MetaWhatsAppInbound] duplicate job coalesced", {
                jobId,
                queueName: QUEUE_NAME,
            });
            return { accepted: true, duplicate: true, jobId, queueName: QUEUE_NAME };
        }
        logger.error("[MetaWhatsAppInbound] enqueue failed", { error: err.message });
        throw err;
    }
}

async function closeMetaWhatsappInboundQueue() {
    if (queue) {
        await queue.close();
        queue = null;
    }
}

async function getMetaWhatsappInboundQueueHealth() {
    const q = getQueue();
    const counts = await q.getJobCounts("waiting", "active", "delayed", "failed");
    return {
        queueName: QUEUE_NAME,
        ...counts,
    };
}

module.exports = {
    enqueueMetaWhatsappInbound,
    closeMetaWhatsappInboundQueue,
    getMetaWhatsappInboundQueueHealth,
    WHATSAPP_META_INBOUND_QUEUE_NAME: QUEUE_NAME,
};
