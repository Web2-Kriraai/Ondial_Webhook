/**
 * direct_<digits> = legacy test dial by phone (10–15 digits).
 * direct_<hex/session> = agent UI session id — not a phone number.
 */
const { isMongoObjectIdString } = require("./mongoObjectId");

function isDirectPhoneContactId(contactId) {
  if (typeof contactId !== "string" || !contactId.startsWith("direct_")) return false;
  const suffix = contactId.slice("direct_".length).trim();
  if (!suffix) return false;
  const digits = suffix.replace(/\D/g, "");
  return digits.length >= 10 && digits.length <= 15;
}

function isDirectSessionContactId(contactId) {
  if (typeof contactId !== "string" || !contactId.startsWith("direct_")) return false;
  if (isDirectPhoneContactId(contactId)) return false;
  const suffix = contactId.slice("direct_".length).trim();
  return suffix.length > 0;
}

/**
 * How CRS (callReceiveStatus) should be updated for this contact id.
 * @returns {{
 *   kind: 'objectId' | 'direct_phone' | 'direct_session' | 'other',
 *   phoneDigits: string | null,
 *   requiresPhoneFallback: boolean,
 * }}
 */
function classifyContactIdForCrs(contactId) {
  const cid = contactId != null ? String(contactId).trim() : "";
  if (!cid) {
    return { kind: "other", phoneDigits: null, requiresPhoneFallback: false };
  }
  if (isMongoObjectIdString(cid)) {
    return { kind: "objectId", phoneDigits: null, requiresPhoneFallback: false };
  }
  if (isDirectPhoneContactId(cid)) {
    const phoneDigits = cid.slice("direct_".length).replace(/\D/g, "") || null;
    return { kind: "direct_phone", phoneDigits, requiresPhoneFallback: false };
  }
  if (isDirectSessionContactId(cid)) {
    return { kind: "direct_session", phoneDigits: null, requiresPhoneFallback: true };
  }
  return { kind: "other", phoneDigits: null, requiresPhoneFallback: false };
}

module.exports = {
  isDirectPhoneContactId,
  isDirectSessionContactId,
  classifyContactIdForCrs,
};
