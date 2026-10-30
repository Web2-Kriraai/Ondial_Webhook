/**
 * Caller memory DB access for the inbound analysis trigger (load the previous overall summary, save the new
 * one). Same collection and record shape as Ondial lib/inbound/callerMemoryStore.js. Never throws.
 */
const { ObjectId } = require("mongodb");
const logger = require("../../logger");
const {
  INBOUND_CALLER_MEMORY_COLLECTION,
  CALLER_MEMORY_SEED_CALLS,
  callerMemoryFilter,
  callerMemorySince,
  callerMemoryUpdate,
  callerNumberVariants,
  nextOverallSummary,
  previousOverallFor,
  seedOverallFromConversations,
} = require("./inboundCallerMemory");

const LOOKUP_TIMEOUT_MS = 1500;
const INBOUND_COLL = process.env.INBOUNDCALLLOG_COLLECTION || "InboundConversation";
let indexEnsured = false;

async function ensureCallerMemoryIndex(db) {
  if (indexEnsured || !db) return;
  await db.collection(INBOUND_CALLER_MEMORY_COLLECTION).createIndex(
    { userId: 1, lineageKey: 1, businessKey: 1, callerKey: 1 },
    { name: "inbound_caller_memory_key", unique: true, background: true }
  );
  indexEnsured = true;
}

/** This agent + earlier versions for the same business (lineage, owner, company profile). String + ObjectId. */
async function sameBusinessConfigIds(db, config) {
  const ids = [config._id];
  if (config.lineageId && config.phoneNumber) {
    const sameBusiness = config.companyId
      ? { companyId: config.companyId }
      : { companyId: { $in: [null] }, category: config.category || null };
    const versions = await db
      .collection("inboundconfigs")
      .find(
        {
          phoneNumber: config.phoneNumber,
          userId: config.userId,
          $or: [{ lineageId: config.lineageId }, { _id: config.lineageId }],
          ...sameBusiness,
        },
        { projection: { _id: 1 }, maxTimeMS: LOOKUP_TIMEOUT_MS }
      )
      .toArray();
    for (const v of versions) ids.push(v._id);
  }
  const out = [];
  const seen = new Set();
  for (const id of ids) {
    const s = String(id);
    if (seen.has(s)) continue;
    seen.add(s);
    out.push(s);
    if (ObjectId.isValid(s) && /^[a-f0-9]{24}$/i.test(s)) out.push(new ObjectId(s));
  }
  return out;
}

async function seedFromConversations(db, { config, callerNumber, excludeCallId, excludeCallSid, now }) {
  const variants = callerNumberVariants(callerNumber);
  if (variants.length === 0) return null;
  const configIds = await sameBusinessConfigIds(db, config);
  const filter = {
    $or: [{ config_id: { $in: configIds } }, { configId: { $in: configIds } }],
    from_number: { $in: variants },
    conversationSummary: { $type: "string", $ne: "" },
  };
  const not = [];
  if (excludeCallId) not.push({ call_id: String(excludeCallId) });
  if (excludeCallSid) not.push({ call_sid: String(excludeCallSid) });
  if (not.length) filter.$nor = not;
  const rows = await db
    .collection(INBOUND_COLL)
    .find(filter, { projection: { call_id: 1, call_sid: 1, startedAt: 1, createdAt: 1, conversationSummary: 1 } })
    .sort({ startedAt: -1, _id: -1 })
    .limit(CALLER_MEMORY_SEED_CALLS * 3)
    .maxTimeMS(LOOKUP_TIMEOUT_MS)
    .toArray();
  const since = callerMemorySince(now).getTime();
  return seedOverallFromConversations(
    rows.filter((r) => {
      const t = new Date(r.startedAt || r.createdAt || 0).getTime();
      return Number.isFinite(t) && t >= since;
    })
  );
}

/** The caller's overall summary as it stood before this call, or null for a first-time caller. */
async function loadPreviousOverallSummary(db, { config, callerNumber, callId = null, callSid = null, now = new Date() }) {
  const filter = callerMemoryFilter(config, callerNumber);
  if (!db || !filter) return null;
  try {
    const record = await db
      .collection(INBOUND_CALLER_MEMORY_COLLECTION)
      .findOne(filter, { maxTimeMS: LOOKUP_TIMEOUT_MS });
    if (record) return previousOverallFor(record, callId);
    return await seedFromConversations(db, {
      config,
      callerNumber,
      excludeCallId: callId,
      excludeCallSid: callSid,
      now,
    });
  } catch (err) {
    logger.warn("[InboundAnalysis] caller memory lookup failed — continuing without it", { error: err.message });
    return null;
  }
}

/** Store the caller's new overall summary after this call's analysis. Returns it, or null. */
async function saveCallerMemoryAfterAnalysis(
  db,
  { config, callerNumber, callId, callStartedAt = null, previous = null, analysisData = null, now = new Date() }
) {
  const filter = callerMemoryFilter(config, callerNumber);
  if (!db || !filter) return null;
  const next = nextOverallSummary({ previous, analysisData, callStartedAt });
  if (!next) return null;
  // Memory only moves forward: an older call analysed late never replaces a newer summary.
  const started = callStartedAt ? new Date(callStartedAt) : null;
  const forwardOnly =
    started && !Number.isNaN(started.getTime())
      ? {
          $or: [
            { lastCallAt: { $exists: false } },
            { lastCallAt: null },
            { lastCallAt: { $lte: started } },
            { lastCallId: String(callId) },
          ],
        }
      : {};
  try {
    await ensureCallerMemoryIndex(db);
    await db.collection(INBOUND_CALLER_MEMORY_COLLECTION).updateOne(
      { ...filter, ...forwardOnly },
      callerMemoryUpdate({
        previous,
        next,
        callId,
        callerNumber,
        configId: config._id,
        callStartedAt,
        now,
      }),
      { upsert: true }
    );
    return next.summary;
  } catch (err) {
    if (err && err.code === 11000) return null;
    logger.warn("[InboundAnalysis] caller memory save failed", { error: err.message });
    return null;
  }
}

module.exports = { loadPreviousOverallSummary, saveCallerMemoryAfterAnalysis };
