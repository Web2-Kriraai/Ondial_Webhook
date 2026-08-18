const STATUS_MAP = {
  sent: "sent",
  delivered: "delivered",
  read: "read",
  failed: "failed",
};

/**
 * Extract delivery events from a Meta Cloud API webhook payload.
 */
function parseMetaStatusUpdates(payload) {
  if (!payload?.entry || !Array.isArray(payload.entry)) return [];

  return payload.entry.flatMap((entry) =>
    (entry.changes || []).flatMap((change) =>
      (change.value?.statuses || [])
        .map((status) => {
          const mappedStatus = STATUS_MAP[String(status?.status || "").toLowerCase()];
          const messageId = String(status?.id || "").trim();
          if (!mappedStatus || !messageId) return null;
          const error = status?.errors?.[0];
          return {
            messageId,
            status: mappedStatus,
            error:
              mappedStatus === "failed" && error
                ? `${error.code || "unknown"}: ${error.title || error.message || "Unknown delivery error"}`
                : null,
          };
        })
        .filter(Boolean)
    )
  );
}

/**
 * Update only the contact that owns Meta's message id.
 */
async function applyMetaStatusUpdates(db, payload) {
  const updates = parseMetaStatusUpdates(payload);
  if (!updates.length) return { processed: 0, matched: 0 };

  let matched = 0;
  for (const update of updates) {
    const set = {
      whatsappStatus: update.status,
      updatedAt: new Date(),
    };
    if (update.status === "failed") set.whatsappError = update.error || "Unknown delivery error";
    else set.whatsappError = null;

    const result = await db.collection("contactprocessings").updateOne(
      { whatsappMessageId: update.messageId },
      { $set: set }
    );
    matched += result.matchedCount || 0;
  }

  return { processed: updates.length, matched };
}

module.exports = {
  parseMetaStatusUpdates,
  applyMetaStatusUpdates,
};
