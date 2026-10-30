const { ObjectId } = require("mongodb");
const { tryDecryptToken } = require("./tokenDecrypt");

/**
 * White-label (WL) partner Meta apps.
 *
 * WL customer users (`users.wlTenantId`) hold partner-managed Meta profile copies in
 * omniChannelSettings.whatsappProfiles[] (provider 'meta', usesPlatformAccount false,
 * numberId/businessNumberId = partner phone_number_id) with encrypted
 * wlWebhookAppSecret / wlWebhookVerifyToken. Many users share one partner number.
 *
 * Decrypted secrets live only in this process's memory and are never logged.
 */

// One in-memory snapshot of ALL WL partner numbers -> users / secrets / verify tokens.
// Per-request lookups never query Mongo: an unknown phone_number_id is just a map miss.
const SNAPSHOT_TTL_MS = 60 * 1000;
// Reload on a miss (or after a failed load) at most this often, globally.
const MISS_REFRESH_MIN_AGE_MS = 30 * 1000;
const MAX_IDS_PER_PAYLOAD = 10;

const PROFILE_PROJECTION = {
  _id: 1,
  email: 1,
  wlTenantId: 1,
  "omniChannelSettings.whatsappProfiles._id": 1,
  "omniChannelSettings.whatsappProfiles.provider": 1,
  "omniChannelSettings.whatsappProfiles.usesPlatformAccount": 1,
  "omniChannelSettings.whatsappProfiles.isDefault": 1,
  "omniChannelSettings.whatsappProfiles.verified": 1,
  "omniChannelSettings.whatsappProfiles.numberId": 1,
  "omniChannelSettings.whatsappProfiles.businessNumberId": 1,
  "omniChannelSettings.whatsappProfiles.wlWebhookAppSecret": 1,
  "omniChannelSettings.whatsappProfiles.wlWebhookVerifyToken": 1,
};

let snapshot = null; // { at, byNumber: Map<pnid, { users, secrets }>, verifyTokens: string[] }
let lastAttemptAt = 0;
let inflight = null;

function resetWlPartnerMetaCaches() {
  snapshot = null;
  lastAttemptAt = 0;
  inflight = null;
}

/**
 * Profile used for a campaign's WhatsApp sends/replies (same rules the relay uses).
 */
function findWhatsappProfile(user, profileId) {
  const profiles = user?.omniChannelSettings?.whatsappProfiles || [];
  const id = String(profileId || "").trim();
  if (id) {
    const match = profiles.find((p) => String(p._id) === id);
    if (match) return match;
  }
  return (
    profiles.find((p) => p.isDefault && p.verified) ||
    profiles.find((p) => p.usesPlatformAccount) ||
    profiles.find((p) => p.verified) ||
    profiles[0] ||
    null
  );
}

function profileNumberIds(profile) {
  return [...new Set([profile?.businessNumberId, profile?.numberId]
    .map((v) => String(v || "").trim())
    .filter(Boolean))];
}

function isManagedWlMetaProfile(profile) {
  if (!profile) return false;
  if (String(profile.provider || "").trim().toLowerCase() !== "meta") return false;
  return profile.usesPlatformAccount === false;
}

function isManagedWlMetaProfileFor(profile, phoneNumberId) {
  return isManagedWlMetaProfile(profile) && profileNumberIds(profile).includes(String(phoneNumberId));
}

function hasWlTenant(user) {
  const t = user?.wlTenantId;
  return t != null && String(t).trim() !== "";
}

function decryptOrEmpty(enc) {
  const v = String(enc || "").trim();
  if (!v) return "";
  const plain = tryDecryptToken(v);
  return plain ? String(plain).trim() : "";
}

/** Single query: every WL user with a partner-managed Meta profile. */
async function loadSnapshot(db, now) {
  const users = await db
    .collection("users")
    .find(
      {
        wlTenantId: { $exists: true, $nin: [null, ""] },
        "omniChannelSettings.whatsappProfiles": {
          $elemMatch: { provider: "meta", usesPlatformAccount: false },
        },
      },
      { projection: PROFILE_PROJECTION }
    )
    .toArray();

  const byNumber = new Map();
  const verifyTokens = new Set();
  for (const user of users) {
    if (!hasWlTenant(user)) continue;
    for (const profile of user?.omniChannelSettings?.whatsappProfiles || []) {
      if (!isManagedWlMetaProfile(profile)) continue;
      const token = decryptOrEmpty(profile.wlWebhookVerifyToken);
      if (token) verifyTokens.add(token);
      const secret = decryptOrEmpty(profile.wlWebhookAppSecret);
      for (const pnid of profileNumberIds(profile)) {
        let entry = byNumber.get(pnid);
        if (!entry) {
          entry = { users: [], secrets: new Set() };
          byNumber.set(pnid, entry);
        }
        if (!entry.users.includes(user)) entry.users.push(user);
        if (secret) entry.secrets.add(secret);
      }
    }
  }
  for (const entry of byNumber.values()) entry.secrets = [...entry.secrets];
  return { at: now, byNumber, verifyTokens: [...verifyTokens] };
}

function reload(db, now) {
  if (!inflight) {
    lastAttemptAt = now;
    inflight = loadSnapshot(db, now)
      .then((snap) => {
        snapshot = snap;
        return snap;
      })
      .finally(() => {
        inflight = null;
      });
  }
  return inflight;
}

/**
 * Current snapshot. Reloads when older than 60s (but never retries a failed load more
 * than once per 30s). refresh=true (after a miss): reload only if the last attempt was
 * at least 30s ago, globally; otherwise null (caller rejects as before).
 */
async function getWlPartnerSnapshot(db, { now = Date.now(), refresh = false } = {}) {
  const canAttempt = now - lastAttemptAt >= MISS_REFRESH_MIN_AGE_MS;
  if (refresh) {
    if (!canAttempt) return null;
    return reload(db, now);
  }
  if (inflight) return inflight;
  if (snapshot && now - snapshot.at < SNAPSHOT_TTL_MS) return snapshot;
  if (!canAttempt) return snapshot; // last load failed recently: serve stale (or null)
  try {
    return await reload(db, now);
  } catch (err) {
    if (snapshot) return snapshot;
    throw err;
  }
}

/** WL users sharing a partner phone_number_id (from the snapshot). */
async function findWlUsersByPhoneNumberId(db, phoneNumberId, opts = {}) {
  const pnid = String(phoneNumberId || "").trim();
  if (!pnid) return [];
  const snap = await getWlPartnerSnapshot(db, opts);
  return snap?.byNumber.get(pnid)?.users || [];
}

/**
 * Candidate partner app secrets (decrypted) for the payload's phone_number_ids.
 * Returns null when refresh=true was rate-limited.
 */
async function getPartnerAppSecretsForPhoneNumberIds(db, phoneNumberIds, { now = Date.now(), refresh = false } = {}) {
  const ids = [...new Set((phoneNumberIds || []).map((v) => String(v || "").trim()).filter((v) => /^\d{1,32}$/.test(v)))].slice(
    0,
    MAX_IDS_PER_PAYLOAD
  );
  if (!ids.length) return refresh ? null : [];
  const snap = await getWlPartnerSnapshot(db, { now, refresh });
  if (!snap) return refresh ? null : [];
  const secrets = new Set();
  for (const pnid of ids) {
    for (const s of snap.byNumber.get(pnid)?.secrets || []) secrets.add(s);
  }
  return [...secrets];
}

/**
 * All decrypted partner verify tokens. Returns null when refresh=true was rate-limited.
 */
async function getPartnerVerifyTokens(db, { now = Date.now(), refresh = false } = {}) {
  const snap = await getWlPartnerSnapshot(db, { now, refresh });
  if (!snap) return refresh ? null : [];
  return snap.verifyTokens;
}

function idForms(value) {
  if (value == null || value === "") return [];
  const s = String(value);
  const out = [s];
  if (value instanceof ObjectId) out.push(value);
  else if (ObjectId.isValid(s) && /^[0-9a-f]{24}$/i.test(s)) out.push(new ObjectId(s));
  return out;
}

/**
 * Routing scope for an inbound message on `phoneNumberId`.
 * - null  → not a WL partner number: caller keeps native routing unchanged.
 * - { campaignIds: [...] } → only campaigns whose creator is one of the WL users sharing
 *   this number AND whose send profile is that partner number. May be empty.
 */
async function resolveWlRoutingScope(db, phoneNumberId) {
  const pnid = String(phoneNumberId || "").trim();
  if (!pnid) return null;
  const users = await findWlUsersByPhoneNumberId(db, pnid);
  if (!users.length) return null;

  const ownerByKey = new Map();
  const createdByIn = [];
  for (const user of users) {
    for (const form of idForms(user._id)) createdByIn.push(form);
    ownerByKey.set(String(user._id), user);
    if (user.email) {
      createdByIn.push(String(user.email));
      ownerByKey.set(String(user.email), user);
    }
  }

  const campaigns = await db
    .collection("campaigns")
    .find(
      { createdBy: { $in: createdByIn } },
      { projection: { _id: 1, createdBy: 1, followupWhatsappProfileId: 1 } }
    )
    .toArray();

  const campaignIds = [];
  for (const campaign of campaigns) {
    const owner = ownerByKey.get(String(campaign.createdBy));
    if (!owner) continue;
    const profile = findWhatsappProfile(owner, campaign.followupWhatsappProfileId);
    if (!isManagedWlMetaProfileFor(profile, pnid)) continue;
    for (const form of idForms(campaign._id)) campaignIds.push(form);
  }
  return { phoneNumberId: pnid, userCount: users.length, campaignIds };
}

module.exports = {
  findWhatsappProfile,
  isManagedWlMetaProfileFor,
  findWlUsersByPhoneNumberId,
  getPartnerAppSecretsForPhoneNumberIds,
  getPartnerVerifyTokens,
  resolveWlRoutingScope,
  getWlPartnerSnapshot,
  resetWlPartnerMetaCaches,
  SNAPSHOT_TTL_MS,
  MISS_REFRESH_MIN_AGE_MS,
};
