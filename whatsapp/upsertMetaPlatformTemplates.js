/**
 * After Meta APPROVED/REJECTED, mirror platform template status into tenant
 * WhatsAppTemplate rows for owner + assigned users.
 */

const { ObjectId } = require("mongodb");

function mapLanguageToMetaCode(codeOrName) {
  const raw = String(codeOrName || "en").trim();
  if (!raw) return "en_US";
  if (raw.includes("_")) return raw;
  const lower = raw.toLowerCase();
  if (lower === "en" || lower === "english") return "en_US";
  if (lower === "hi" || lower === "hindi") return "hi";
  return raw;
}

function resolveMetaPlatformProfileId(user) {
  const profiles = user?.omniChannelSettings?.whatsappProfiles || [];
  const meta =
    profiles.find(
      (p) =>
        String(p.provider || "").toLowerCase() === "meta" &&
        p.usesPlatformAccount !== false
    ) || profiles.find((p) => String(p.provider || "").toLowerCase() === "meta");
  return meta?._id ? String(meta._id) : null;
}

function publicMediaUrl(url) {
  const raw = String(url || "").trim();
  return /^https?:\/\//i.test(raw) ? raw : "";
}

function toObjectId(value) {
  if (!value) return null;
  if (value instanceof ObjectId) return value;
  if (ObjectId.isValid(String(value))) return new ObjectId(String(value));
  return null;
}

/**
 * @param {import('mongodb').Db} db
 * @param {object} platformTemplate
 * @returns {Promise<{ upserted: number, userIds: string[] }>}
 */
async function upsertWhatsAppTemplatesFromPlatformMeta(db, platformTemplate) {
  if (!platformTemplate?._id) return { upserted: 0, userIds: [] };

  const templateName = String(platformTemplate.templateName || platformTemplate.name || "")
    .trim()
    .toLowerCase();
  if (!templateName) return { upserted: 0, userIds: [] };

  const userIdSet = new Set();
  if (platformTemplate.ownerUserId) {
    userIdSet.add(String(platformTemplate.ownerUserId));
  }

  const assignments = await db
    .collection("platform_whatsapp_template_assignments")
    .find({
      platformTemplateId: platformTemplate._id,
      isActive: { $ne: false },
    })
    .project({ userId: 1 })
    .toArray();

  for (const a of assignments) {
    if (a.userId) userIdSet.add(String(a.userId));
  }

  if (!userIdSet.size) return { upserted: 0, userIds: [] };

  const objectIds = [...userIdSet].map(toObjectId).filter(Boolean);
  if (!objectIds.length) return { upserted: 0, userIds: [] };

  const users = await db
    .collection("users")
    .find({ _id: { $in: objectIds } })
    .project({ email: 1, "omniChannelSettings.whatsappProfiles": 1 })
    .toArray();

  const language = mapLanguageToMetaCode(platformTemplate.language);
  const mediaUrl = publicMediaUrl(platformTemplate.headerMediaUrl);
  const metaTemplateId = String(platformTemplate.metaTemplateId || "").trim();
  let upserted = 0;
  const touched = [];

  for (const user of users) {
    const profileId = resolveMetaPlatformProfileId(user);
    if (!profileId) continue;

    const payload = {
      userId: user._id,
      userEmail: String(user.email || "").trim().toLowerCase(),
      whatsappProfileId: profileId,
      name: platformTemplate.name || templateName,
      templateName,
      aisensyCampaignName: "",
      metaTemplateId,
      status: platformTemplate.status === "active" ? "active" : "processing",
      rejectionReason: String(platformTemplate.rejectionReason || ""),
      language,
      bodyText: platformTemplate.bodyText || "",
      headerType: platformTemplate.headerType || null,
      headerText: platformTemplate.headerText || "",
      footerText: platformTemplate.footerText || "",
      buttons: Array.isArray(platformTemplate.buttons) ? platformTemplate.buttons : [],
      variableCount: Number(platformTemplate.variableCount) || 0,
      mediaUrl,
      syncedFromAisensy: false,
      description: "Meta Cloud API",
      updatedAt: new Date(),
    };

    await db.collection("whatsapptemplates").updateOne(
      {
        userId: user._id,
        whatsappProfileId: profileId,
        templateName,
      },
      {
        $set: payload,
        $setOnInsert: { createdAt: new Date() },
      },
      { upsert: true }
    );
    upserted += 1;
    touched.push(String(user._id));
  }

  return { upserted, userIds: touched };
}

module.exports = {
  upsertWhatsAppTemplatesFromPlatformMeta,
};
