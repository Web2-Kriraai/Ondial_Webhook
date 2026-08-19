const { ObjectId } = require("mongodb");

function buildSessionKey(phone, campaignId) {
  const p = String(phone || "").replace(/[\s+\-()]/g, "");
  const c = campaignId ? String(campaignId) : "none";
  return `wa:${p}:campaign:${c}`;
}

function toObjectIdOrNull(value) {
  if (!value) return null;
  if (value instanceof ObjectId) return value;
  if (ObjectId.isValid(String(value))) return new ObjectId(String(value));
  return null;
}

async function getOrCreateSession(db, {
  phone,
  campaignId = null,
  contactId = null,
  userId = null,
  userName = "",
  callId = null,
  analysisId = null,
}) {
  const sessionKey = buildSessionKey(phone, campaignId);
  const col = db.collection("whatsapp_ai_sessions");
  try {
    await col.createIndex({ sessionKey: 1 }, { unique: true });
    await col.createIndex({ phone: 1, updatedAt: -1 });
  } catch {
    // index may already exist
  }

  const patch = {
    updatedAt: new Date(),
  };
  if (userName) patch.userName = userName;
  const callIdStr = callId != null ? String(callId).trim() : "";
  if (callIdStr) patch.callId = callIdStr;
  const analysisOid = toObjectIdOrNull(analysisId);
  if (analysisOid) patch.analysisId = analysisOid;
  const contactOid = toObjectIdOrNull(contactId);
  if (contactOid) patch.contactId = contactOid;
  const userOid = toObjectIdOrNull(userId);
  if (userOid) patch.userId = userOid;
  const campaignOid = toObjectIdOrNull(campaignId);
  if (campaignOid) patch.campaignId = campaignOid;

  let session = await col.findOne({ sessionKey });
  if (session) {
    if (Object.keys(patch).length > 1) {
      await col.updateOne({ sessionKey }, { $set: patch });
      session = { ...session, ...patch };
    }
    return session;
  }

  const doc = {
    sessionKey,
    phone: String(phone || "").replace(/[\s+\-()]/g, ""),
    campaignId: campaignOid,
    contactId: contactOid,
    userId: userOid,
    callId: callIdStr || null,
    analysisId: analysisOid,
    userName: userName || "",
    history: [],
    lastInboundAt: null,
    lastOutboundAt: null,
    conversationWindowOpensUntil: null,
    lastError: null,
    createdAt: new Date(),
    updatedAt: new Date(),
  };

  try {
    await col.insertOne(doc);
    return doc;
  } catch (err) {
    session = await col.findOne({ sessionKey });
    if (session) return session;
    throw err;
  }
}

async function appendSessionHistory(db, sessionKey, entry) {
  const now = entry.timestamp || new Date();
  await db.collection("whatsapp_ai_sessions").updateOne(
    { sessionKey },
    {
      $push: {
        history: {
          $each: [
            {
              role: entry.role,
              text: String(entry.text || ""),
              messageId: entry.messageId || "",
              timestamp: now,
              isAiGenerated: Boolean(entry.isAiGenerated),
            },
          ],
          $slice: -100,
        },
      },
      $set: {
        updatedAt: now,
        ...(entry.role === "user"
          ? {
              lastInboundAt: now,
              conversationWindowOpensUntil: new Date(now.getTime() + 24 * 60 * 60 * 1000),
            }
          : { lastOutboundAt: now }),
      },
    },
    { upsert: true }
  );
}

function historyForAi(session, limit = 100) {
  const items = (session?.history || []).slice(-limit);
  return items.map((e) => ({
    role: e.role,
    text: e.text,
    timestamp: e.timestamp,
  }));
}

function phoneSessionVariants(phone) {
  const normalized = String(phone || "").replace(/[\s+\-()]/g, "");
  if (!normalized) return [];
  const variants = [normalized, `+${normalized}`];
  if (normalized.startsWith("91") && normalized.length === 12) {
    variants.push(normalized.slice(2));
  }
  return variants;
}

async function findLatestSessionByPhone(db, phone) {
  const variants = phoneSessionVariants(phone);
  if (!variants.length) return null;
  return db.collection("whatsapp_ai_sessions").findOne(
    { phone: { $in: variants } },
    { sort: { updatedAt: -1 } }
  );
}

async function findLatestOpenSessionByPhone(db, phone) {
  const variants = phoneSessionVariants(phone);
  if (!variants.length) return null;
  return db.collection("whatsapp_ai_sessions").findOne(
    {
      phone: { $in: variants },
      $or: [{ closedAt: { $exists: false } }, { closedAt: null }],
    },
    { sort: { lastOutboundAt: -1, updatedAt: -1 } }
  );
}

async function findSessionByCampaign(db, phone, campaignId) {
  if (!campaignId) return null;
  return db.collection("whatsapp_ai_sessions").findOne({
    sessionKey: buildSessionKey(phone, campaignId),
  });
}

module.exports = {
  buildSessionKey,
  getOrCreateSession,
  appendSessionHistory,
  historyForAi,
  findLatestSessionByPhone,
  findLatestOpenSessionByPhone,
  findSessionByCampaign,
};
