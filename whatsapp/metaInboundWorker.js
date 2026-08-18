const { Worker } = require("bullmq");
const { getBullRedis } = require("../redis");
const { processMetaInboundPayload } = require("./processMetaInbound");
const logger = require("../logger");

const QUEUE_NAME = process.env.WHATSAPP_META_INBOUND_QUEUE_NAME || "whatsapp-meta-inbound";

function isConsumerEnabled() {
  const raw = String(process.env.WHATSAPP_META_CONSUMER_ENABLED ?? "true").trim().toLowerCase();
  return raw !== "false" && raw !== "0" && raw !== "off";
}

let worker = null;
let lastCompletedAt = null;
let lastFailedAt = null;
let lastFailure = null;
let lastTemplateStatuses = null;
let lastJobSummary = null;

function summarizePayloadFields(payload) {
  if (!Array.isArray(payload?.entry)) return [];
  return [
    ...new Set(
      payload.entry.flatMap((entry) =>
        (entry.changes || [])
          .map((change) => String(change?.field || "").trim())
          .filter(Boolean)
      )
    ),
  ];
}

function startMetaWhatsappInboundWorker() {
  if (worker) return worker;

  if (!isConsumerEnabled()) {
    logger.info("[MetaWhatsAppInbound] consumer disabled (WHATSAPP_META_CONSUMER_ENABLED=false)");
    return null;
  }

  const connection = getBullRedis();
  worker = new Worker(
    QUEUE_NAME,
    async (job) => {
      const startedAt = Date.now();
      const payload = job.data?.payload;
      if (!payload || typeof payload !== "object") {
        logger.warn("[MetaWhatsAppInbound] skip empty payload", { jobId: job?.id });
        return { skipped: true, reason: "empty_payload" };
      }

      const fields = summarizePayloadFields(payload);
      const { inbound, statuses, templateStatuses } = await processMetaInboundPayload(payload);
      const failedStatuses = [];
      try {
        const { parseMetaStatusUpdates } = require("./metaStatusUpdates");
        for (const u of parseMetaStatusUpdates(payload)) {
          if (u.status === "failed") {
            failedStatuses.push({
              messageId: u.messageId,
              error: u.error,
            });
          }
        }
      } catch {
        /* ignore */
      }
      if (failedStatuses.length) {
        logger.warn("[MetaWhatsAppInbound] delivery failed", {
          jobId: job.id,
          failedStatuses,
        });
      }

      if (templateStatuses?.processed > 0) {
        lastTemplateStatuses = {
          at: new Date().toISOString(),
          ...templateStatuses,
        };
        if (
          (templateStatuses.platformMatched || 0) + (templateStatuses.tenantMatched || 0) ===
          0
        ) {
          logger.warn("[MetaWhatsAppInbound] template status processed with 0 Mongo matches", {
            jobId: job?.id,
            templateStatuses,
          });
        } else {
          logger.info("[MetaWhatsAppInbound] template status applied", {
            jobId: job?.id,
            templateStatuses,
          });
        }
      }

      const result = { inbound, statuses, templateStatuses };
      lastJobSummary = {
        at: new Date().toISOString(),
        jobId: job?.id,
        fields,
        durationMs: Date.now() - startedAt,
        inboundProcessed: inbound?.processed ?? null,
        statusUpdates: statuses?.processed ?? null,
        templateUpdates: templateStatuses?.processed ?? 0,
      };
      return result;
    },
    {
      connection,
      concurrency: Number(process.env.WHATSAPP_META_INBOUND_CONCURRENCY || 5),
    }
  );

  worker.on("failed", (job, err) => {
    lastFailedAt = new Date().toISOString();
    lastFailure = err?.message || String(err);
    logger.error("[MetaWhatsAppInbound] job failed", {
      jobId: job?.id,
      attempt: job?.attemptsMade,
      error: err?.message || err,
    });
  });

  worker.on("error", (err) => {
    lastFailedAt = new Date().toISOString();
    lastFailure = err?.message || String(err);
    logger.error("[MetaWhatsAppInbound] worker error", { error: err?.message || err });
  });

  worker.on("completed", () => {
    lastCompletedAt = new Date().toISOString();
    lastFailure = null;
  });

  logger.info(`[MetaWhatsAppInbound] consumer listening on queue "${QUEUE_NAME}"`);
  return worker;
}

async function stopMetaWhatsappInboundWorker() {
  if (!worker) return;
  await worker.close();
  worker = null;
}

function getMetaWhatsappInboundWorkerHealth() {
  return {
    queueName: QUEUE_NAME,
    enabled: isConsumerEnabled(),
    running: Boolean(worker),
    lastCompletedAt,
    lastFailedAt,
    lastFailure,
    lastTemplateStatuses,
    lastJobSummary,
  };
}

module.exports = {
  startMetaWhatsappInboundWorker,
  stopMetaWhatsappInboundWorker,
  getMetaWhatsappInboundWorkerHealth,
  WHATSAPP_META_INBOUND_QUEUE_NAME: QUEUE_NAME,
};
