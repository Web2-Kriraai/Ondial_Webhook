const crypto = require("crypto");
const { Queue, Worker, QueueEvents } = require("bullmq");
const { getBullRedis } = require("./redis");
const { handleWebhook } = require("./webhookHandler");
const logger = require("./logger");

const WORKER_CONCURRENCY = Number(process.env.WEBHOOK_WORKER_CONCURRENCY || 25);
const MAX_RETRIES = Number(process.env.WEBHOOK_MAX_RETRIES || 8);
const BASE_RETRY_DELAY_MS = Number(process.env.WEBHOOK_BASE_RETRY_MS || 250);
const QUEUE_NAME = process.env.WEBHOOK_QUEUE_NAME || "webhook-events";

let queue = null;
let worker = null;
let queueEvents = null;

function hashPayload(source, body) {
    return crypto
        .createHash("sha256")
        .update(`${source}:${JSON.stringify(body || {})}`)
        .digest("hex");
}

async function enqueueWebhook(body) {
    const dedupeKey = hashPayload("webhook", body);
    const q = getQueue();
    try {
        await q.add("webhook", body, {
            jobId: dedupeKey,
            attempts: MAX_RETRIES + 1,
            backoff: { type: "exponential", delay: BASE_RETRY_DELAY_MS },
            removeOnComplete: { age: 300, count: 1000 },
            removeOnFail: false,
        });
        return { accepted: true, duplicate: false };
    } catch (err) {
        // Duplicate jobId means same payload already queued/processed recently.
        if (String(err.message || "").includes("already exists")) {
            return { accepted: true, duplicate: true };
        }
        throw err;
    }
}

async function startWebhookWorkers() {
    if (worker) return;
    const connection = getBullRedis();
    queue = new Queue(QUEUE_NAME, { connection });
    queueEvents = new QueueEvents(QUEUE_NAME, { connection });
    worker = new Worker(
        QUEUE_NAME,
        async (job) => {
            await handleWebhook(job.data);
        },
        {
            connection,
            concurrency: WORKER_CONCURRENCY,
        }
    );

    worker.on("failed", (job, err) => {
        logger.error("[Queue] Job processing error", {
            jobId: job ? String(job.id) : "",
            attempt: job ? job.attemptsMade : 0,
            error: err.message,
        });
    });

    worker.on("error", (err) => {
        logger.error("[Queue] Worker error", { error: err.message });
    });

    queueEvents.on("error", (err) => {
        logger.error("[Queue] QueueEvents error", { error: err.message });
    });

    await Promise.all([queue.waitUntilReady(), queueEvents.waitUntilReady(), worker.waitUntilReady()]);
    logger.info("[Queue] Webhook workers started", { concurrency: WORKER_CONCURRENCY, queueName: QUEUE_NAME });
}

function getQueue() {
    if (!queue) {
        const connection = getBullRedis();
        queue = new Queue(QUEUE_NAME, { connection });
    }
    return queue;
}

async function closeWebhookWorkers() {
    const closers = [];
    if (worker) closers.push(worker.close());
    if (queueEvents) closers.push(queueEvents.close());
    if (queue) closers.push(queue.close());
    if (closers.length) {
        await Promise.all(closers);
    }
    queue = null;
    worker = null;
    queueEvents = null;
}

async function getQueueLagSnapshot() {
    const q = getQueue();
    const counts = await q.getJobCounts('waiting', 'active', 'delayed', 'failed', 'completed');
    return {
        waiting: counts.waiting || 0,
        active: counts.active || 0,
        delayed: counts.delayed || 0,
        failed: counts.failed || 0,
        completed: counts.completed || 0,
    };
}

module.exports = {
    enqueueWebhook,
    startWebhookWorkers,
    closeWebhookWorkers,
    getQueueLagSnapshot,
};
