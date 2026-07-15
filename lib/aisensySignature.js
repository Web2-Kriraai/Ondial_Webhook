const crypto = require("crypto");

/**
 * Verify AiSensy webhook HMAC when WEBHOOK_SECRET / AISENSY_WEBHOOK_SECRET is set.
 */
function verifyAisensySignature(rawBody, signatureHeader, secret) {
    if (!secret) return true;
    if (!signatureHeader) return false;

    const normalized = String(signatureHeader).replace(/^sha256=/i, "").trim();
    const body = Buffer.isBuffer(rawBody) ? rawBody : Buffer.from(String(rawBody || ""), "utf8");
    const expected = crypto.createHmac("sha256", secret).update(body).digest("hex");

    try {
        const a = Buffer.from(normalized, "hex");
        const b = Buffer.from(expected, "hex");
        if (a.length !== b.length || !a.length) {
            return normalized === expected;
        }
        return crypto.timingSafeEqual(a, b);
    } catch {
        return normalized === expected;
    }
}

module.exports = { verifyAisensySignature };
