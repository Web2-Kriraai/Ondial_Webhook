/**
 * direct_<digits> = legacy test dial by phone (10–15 digits).
 * direct_<hex/session> = agent UI session id — not a phone number.
 */
function isDirectPhoneContactId(contactId) {
    if (typeof contactId !== "string" || !contactId.startsWith("direct_")) return false;
    const suffix = contactId.slice("direct_".length).trim();
    if (!suffix) return false;
    const digits = suffix.replace(/\D/g, "");
    return digits.length >= 10 && digits.length <= 15;
}

module.exports = { isDirectPhoneContactId };
