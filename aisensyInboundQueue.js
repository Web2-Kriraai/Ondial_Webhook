const crypto = require("crypto");
const { Queue } = require("bullmq");
const { getBullRedis } = require("./redis");
const logger = require("./logger");

const QUEUE_NAME = process.env.AISENSY_INBOUND_QUEUE_NAME || "aisensy-inbound";
const MAX_RETRIES = Number(process.env.AISENSY_INBOUND_MAX_RETRIES || 5);
const BASE_RETRY_DELAY_MS = Number(process.env.AISENSY_INBOUND_BASE_RETRY_MS || 1000);

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
 * Enqueue AiSensy inbound webhook for Calling_system1 scheduler consumer.
 * Ondial_Webhook is producer-only for this queue (no local worker).
 */
async function enqueueAisensyInbound(payload, meta = {}) {
    const q = getQueue();
    const jobId = `aisensy-${hashPayload(payload)}-${Date.now().toString(36)}`;
    try {
        await q.add(
            "aisensy-inbound",
            {
                payload,
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
            return { accepted: true, duplicate: true, queueName: QUEUE_NAME };
        }
        logger.error("[AiSensyInbound] enqueue failed", { error: err.message });
        throw err;
    }
}

async function closeAisensyInboundQueue() {
    if (queue) {
        await queue.close();
        queue = null;
    }
}

module.exports = {
    enqueueAisensyInbound,
    closeAisensyInboundQueue,
    AISENSY_INBOUND_QUEUE_NAME: QUEUE_NAME,
};
