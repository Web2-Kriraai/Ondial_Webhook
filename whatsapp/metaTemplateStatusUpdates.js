const {
  upsertWhatsAppTemplatesFromPlatformMeta,
} = require("./upsertMetaPlatformTemplates");

const EVENT_TO_LIFECYCLE = {
  APPROVED: "active",
  PENDING: "processing",
  IN_REVIEW: "processing",
  IN_APPEAL: "processing",
  PAUSED: "paused",
  FLAGGED: "paused",
  LIMITED: "paused",
  LIMIT_EXCEEDED: "paused",
  LOCKED: "paused",
  REJECTED: "rejected",
  FAILED: "rejected",
  DISABLED: "rejected",
  DELETED: "rejected",
  DISAPPROVED: "rejected",
};

function normalizeEvent(value) {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, "_");
}

function formatReason(value = {}) {
  const parts = [
    value.reason,
    value.rejection_info?.reason,
    value.rejection_info?.recommendation,
    value.other_info?.description,
  ]
    .map((item) => String(item || "").trim())
    .filter((item) => item && item.toUpperCase() !== "NONE");
  return [...new Set(parts)].join(" — ").slice(0, 1_000);
}

function metaIdVariants(metaTemplateId) {
  const raw = String(metaTemplateId || "").trim();
  if (!raw) return [];
  const variants = [raw];
  if (/^\d+$/.test(raw)) {
    const asNum = Number(raw);
    if (Number.isSafeInteger(asNum)) variants.push(asNum);
  }
  return variants;
}

/**
 * Extract `message_template_status_update` events from a Meta Cloud API
 * WhatsApp Business Account webhook.
 */
function parseMetaTemplateStatusUpdates(payload) {
  if (!Array.isArray(payload?.entry)) return [];

  return payload.entry.flatMap((entry) =>
    (entry.changes || [])
      .filter(
        (change) =>
          String(change?.field || "").trim() === "message_template_status_update"
      )
      .map((change) => {
        const value = change?.value || {};
        const event = normalizeEvent(value.event);
        const status = EVENT_TO_LIFECYCLE[event];
        const metaTemplateId = String(value.message_template_id || "").trim();
        if (!status || !metaTemplateId) return null;

        return {
          metaTemplateId,
          status,
          event,
          templateName: String(value.message_template_name || "").trim(),
          language: String(value.message_template_language || "").trim(),
          category: String(value.message_template_category || "").trim(),
          rejectionReason: formatReason(value),
        };
      })
      .filter(Boolean)
  );
}

/**
 * Persist status changes for platform catalog + tenant Meta templates,
 * then mirror APPROVED/REJECTED rows to owner + assigned tenant WhatsAppTemplate docs.
 */
async function applyMetaTemplateStatusUpdates(db, payload) {
  const updates = parseMetaTemplateStatusUpdates(payload);
  if (!updates.length) return { processed: 0, platformMatched: 0, tenantMatched: 0, mirrored: 0 };

  let platformMatched = 0;
  let tenantMatched = 0;
  let mirrored = 0;

  for (const update of updates) {
    const now = new Date();
    const idVariants = metaIdVariants(update.metaTemplateId);
    const common = {
      status: update.status,
      rejectionReason: update.status === "active" ? "" : update.rejectionReason,
      updatedAt: now,
    };

    const platformSet = {
      ...common,
      ...(update.category ? { category: update.category } : {}),
      ...(update.status === "active"
        ? { approvedAt: now, readyAt: now, nextPollAt: null }
        : { readyAt: null }),
      ...(update.status === "rejected" || update.status === "paused"
        ? { nextPollAt: null }
        : {}),
    };

    const [platformResult, tenantResult] = await Promise.all([
      db.collection("platform_whatsapp_templates").updateMany(
        { metaTemplateId: { $in: idVariants } },
        { $set: { ...platformSet, provider: "meta", metaTemplateId: update.metaTemplateId } }
      ),
      db.collection("whatsapptemplates").updateMany(
        { metaTemplateId: { $in: idVariants } },
        { $set: { ...common, metaTemplateId: update.metaTemplateId } }
      ),
    ]);

    let pMatched = platformResult.matchedCount || 0;
    let tMatched = tenantResult.matchedCount || 0;

    if (pMatched === 0 && update.templateName) {
      const byName = await db.collection("platform_whatsapp_templates").updateMany(
        {
          templateName: update.templateName,
          $or: [
            { metaTemplateId: { $in: ["", null] } },
            { metaTemplateId: { $exists: false } },
            { metaTemplateId: { $in: idVariants } },
          ],
        },
        {
          $set: {
            ...platformSet,
            provider: "meta",
            metaTemplateId: update.metaTemplateId,
          },
        }
      );
      pMatched += byName.matchedCount || 0;
    }
    if (tMatched === 0 && update.templateName) {
      const byName = await db.collection("whatsapptemplates").updateMany(
        {
          templateName: update.templateName,
          $or: [
            { metaTemplateId: { $in: ["", null] } },
            { metaTemplateId: { $exists: false } },
            { metaTemplateId: { $in: idVariants } },
          ],
        },
        { $set: { ...common, metaTemplateId: update.metaTemplateId } }
      );
      tMatched += byName.matchedCount || 0;
    }

    platformMatched += pMatched;
    tenantMatched += tMatched;

    // Mirror to owner + assigned users so Omni / campaigns see the new status
    // without waiting for a manual Refresh.
    if (pMatched > 0 && (update.status === "active" || update.status === "rejected" || update.status === "paused")) {
      const platformRows = await db
        .collection("platform_whatsapp_templates")
        .find({
          $or: [
            { metaTemplateId: { $in: idVariants } },
            ...(update.templateName ? [{ templateName: update.templateName }] : []),
          ],
        })
        .toArray();

      for (const row of platformRows) {
        const mirror = await upsertWhatsAppTemplatesFromPlatformMeta(db, {
          ...row,
          status: update.status,
          metaTemplateId: update.metaTemplateId,
          rejectionReason: common.rejectionReason,
        });
        mirrored += mirror.upserted || 0;
        if (mirror.upserted) {
          console.log("[MetaTemplateStatus] mirrored to tenant WhatsAppTemplate", {
            templateName: row.templateName || update.templateName,
            status: update.status,
            upserted: mirror.upserted,
            userIds: mirror.userIds,
          });
        }
      }
    }

    const matched = pMatched + tMatched;
    if (matched === 0) {
      console.warn("[MetaTemplateStatus] no Mongo rows matched metaTemplateId", {
        metaTemplateId: update.metaTemplateId,
        templateName: update.templateName || null,
        event: update.event,
        status: update.status,
      });
    } else {
      console.log("[MetaTemplateStatus] applied", {
        metaTemplateId: update.metaTemplateId,
        templateName: update.templateName || null,
        event: update.event,
        status: update.status,
        platformMatched: pMatched,
        tenantMatched: tMatched,
        mirrored,
      });
    }
  }

  return { processed: updates.length, platformMatched, tenantMatched, mirrored };
}

module.exports = {
  parseMetaTemplateStatusUpdates,
  applyMetaTemplateStatusUpdates,
};
