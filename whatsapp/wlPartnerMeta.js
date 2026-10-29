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

const SECRET_TTL_MS = 5 * 60 * 1000;
const ROUTING_TTL_MS = 60 * 1000;
// On a miss, reload at most this often (new partner set up a minute ago).
const MISS_REFRESH_MIN_AGE_MS = 30 * 1000;

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
};

const usersByNumberCache = new Map(); // pnid -> { at, users, secrets? }
// Unsigned POSTs can carry arbitrary phone_number_ids; keep the cache bounded.
const MAX_CACHED_NUMBERS = 2000;
const MAX_IDS_PER_PAYLOAD = 10;

function cacheUsers(pnid, entry) {
  usersByNumberCache.delete(pnid);
  usersByNumberCache.set(pnid, entry);
  while (usersByNumberCache.size > MAX_CACHED_NUMBERS) {
    usersByNumberCache.delete(usersByNumberCache.keys().next().value);
  }
}
let verifyTokenCache = null; // { at, tokens }

function resetWlPartnerMetaCaches() {
  usersByNumberCache.clear();
  verifyTokenCache = null;
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
  return [profile?.businessNumberId, profile?.numberId]
    .map((v) => String(v || "").trim())
    .filter(Boolean);
}

function isManagedWlMetaProfileFor(profile, phoneNumberId) {
  if (!profile) return false;
  if (String(profile.provider || "").trim().toLowerCase() !== "meta") return false;
  if (profile.usesPlatformAccount !== false) return false;
  return profileNumberIds(profile).includes(String(phoneNumberId));
}

function hasWlTenant(user) {
  const t = user?.wlTenantId;
  return t != null && String(t).trim() !== "";
}

async function queryWlUsersByPhoneNumberId(db, phoneNumberId) {
  const pnid = String(phoneNumberId);
  const users = await db
    .collection("users")
    .find(
      {
        wlTenantId: { $exists: true, $nin: [null, ""] },
        "omniChannelSettings.whatsappProfiles": {
          $elemMatch: {
            provider: "meta",
            usesPlatformAccount: false,
            $or: [{ businessNumberId: pnid }, { numberId: pnid }],
          },
        },
      },
      { projection: PROFILE_PROJECTION }
    )
    .toArray();
  // Defensive re-check (the query is the source of truth, this guards odd shapes).
  return users.filter(
    (u) =>
      hasWlTenant(u) &&
      (u.omniChannelSettings?.whatsappProfiles || []).some((p) => isManagedWlMetaProfileFor(p, pnid))
  );
}

/**
 * WL users sharing a partner phone_number_id. Cached (negative results too).
 */
async function findWlUsersByPhoneNumberId(db, phoneNumberId, { ttlMs = ROUTING_TTL_MS, now = Date.now() } = {}) {
  const pnid = String(phoneNumberId || "").trim();
  if (!pnid) return [];
  const cached = usersByNumberCache.get(pnid);
  if (cached && now - cached.at < ttlMs) return cached.users;
  const users = await queryWlUsersByPhoneNumberId(db, pnid);
  cacheUsers(pnid, { at: now, users });
  return users;
}

function decryptedSecretsFor(users, pnid) {
  const out = new Set();
  for (const user of users || []) {
    for (const profile of user?.omniChannelSettings?.whatsappProfiles || []) {
      if (!isManagedWlMetaProfileFor(profile, pnid)) continue;
      const enc = String(profile.wlWebhookAppSecret || "").trim();
      if (!enc) continue;
      const plain = tryDecryptToken(enc);
      if (plain && String(plain).trim()) out.add(String(plain).trim());
    }
  }
  return [...out];
}

/**
 * Candidate partner app secrets (decrypted) for the given phone_number_ids.
 * Decryption results are cached together with the user lookup (~5 min).
 *
 * refresh=true (used after a signature miss): drop cache entries older than 30s and
 * reload them; returns null when nothing was old enough to reload (rate limit).
 */
async function getPartnerAppSecretsForPhoneNumberIds(db, phoneNumberIds, { now = Date.now(), refresh = false } = {}) {
  const ids = [...new Set((phoneNumberIds || []).map((v) => String(v || "").trim()).filter((v) => /^\d{1,32}$/.test(v)))].slice(
    0,
    MAX_IDS_PER_PAYLOAD
  );
  if (refresh) {
    let dropped = false;
    for (const pnid of ids) {
      const cached = usersByNumberCache.get(pnid);
      if (cached && now - cached.at >= MISS_REFRESH_MIN_AGE_MS) {
        usersByNumberCache.delete(pnid);
        dropped = true;
      }
    }
    if (!dropped) return null;
  }
  const secrets = new Set();
  for (const pnid of ids) {
    const cached = usersByNumberCache.get(pnid);
    let entry = cached && now - cached.at < SECRET_TTL_MS ? cached : null;
    if (!entry) {
      const users = await queryWlUsersByPhoneNumberId(db, pnid);
      entry = { at: now, users };
      cacheUsers(pnid, entry);
    }
    if (!entry.secrets) entry.secrets = decryptedSecretsFor(entry.users, pnid);
    for (const s of entry.secrets) secrets.add(s);
  }
  return [...secrets];
}

async function queryPartnerVerifyTokens(db) {
  const users = await db
    .collection("users")
    .find(
      {
        wlTenantId: { $exists: true, $nin: [null, ""] },
        "omniChannelSettings.whatsappProfiles": {
          $elemMatch: {
            provider: "meta",
            usesPlatformAccount: false,
            wlWebhookVerifyToken: { $exists: true, $nin: [null, ""] },
          },
        },
      },
      {
        projection: {
          _id: 1,
          wlTenantId: 1,
          "omniChannelSettings.whatsappProfiles.provider": 1,
          "omniChannelSettings.whatsappProfiles.usesPlatformAccount": 1,
          "omniChannelSettings.whatsappProfiles.wlWebhookVerifyToken": 1,
        },
      }
    )
    .toArray();
  const out = new Set();
  for (const user of users) {
    if (!hasWlTenant(user)) continue;
    for (const p of user?.omniChannelSettings?.whatsappProfiles || []) {
      if (String(p?.provider || "").toLowerCase() !== "meta" || p?.usesPlatformAccount !== false) continue;
      const enc = String(p?.wlWebhookVerifyToken || "").trim();
      if (!enc) continue;
      const plain = tryDecryptToken(enc);
      if (plain && String(plain).trim()) out.add(String(plain).trim());
    }
  }
  return [...out];
}

/**
 * All decrypted partner verify tokens (cached ~5 min).
 * refresh=true (after a miss): reload if the cache is at least 30s old, else null.
 */
async function getPartnerVerifyTokens(db, { now = Date.now(), refresh = false } = {}) {
  const fresh = verifyTokenCache && now - verifyTokenCache.at < SECRET_TTL_MS;
  if (refresh) {
    if (verifyTokenCache && now - verifyTokenCache.at < MISS_REFRESH_MIN_AGE_MS) return null;
  } else if (fresh) {
    return verifyTokenCache.tokens;
  }
  verifyTokenCache = { at: now, tokens: await queryPartnerVerifyTokens(db) };
  return verifyTokenCache.tokens;
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
  resetWlPartnerMetaCaches,
  SECRET_TTL_MS,
  ROUTING_TTL_MS,
};
