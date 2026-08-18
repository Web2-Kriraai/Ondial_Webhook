const crypto = require("crypto");

/**
 * Verify Meta Cloud API webhook signature (X-Hub-Signature-256).
 * Fail closed when app secret is missing.
 *
 * @param {string|Buffer} rawBody
 * @param {string|null} signatureHeader
 * @param {string} appSecret
 */
function verifyMetaWhatsappSignature(rawBody, signatureHeader, appSecret) {
    const secret = String(appSecret || "").trim();
    if (!secret) return false;
    if (!signatureHeader) return false;

    const normalized = String(signatureHeader).replace(/^sha256=/i, "").trim();
    if (!/^[0-9a-f]+$/i.test(normalized)) return false;

    const body = Buffer.isBuffer(rawBody) ? rawBody : Buffer.from(String(rawBody || ""), "utf8");
    const expected = crypto.createHmac("sha256", secret).update(body).digest("hex");

    try {
        const a = Buffer.from(normalized, "hex");
        const b = Buffer.from(expected, "hex");
        if (a.length !== b.length || !a.length) return false;
        return crypto.timingSafeEqual(a, b);
    } catch {
        return false;
    }
}

module.exports = { verifyMetaWhatsappSignature };
