const crypto = require("crypto");

/**
 * Headers for server-side fetch to Ondial `/api/webhooks/*` routes.
 */
function getOndialWebhookFetchHeaders(rawBodyString = "") {
    const headers = { "Content-Type": "application/json" };
    const secret = process.env.WEBHOOK_SHARED_SECRET;
    if (secret) {
        headers["x-webhook-secret"] = secret;
    }
    const hmacSecret = process.env.WEBHOOK_HMAC_SECRET;
    if (hmacSecret && typeof rawBodyString === "string") {
        const tsMs = Date.now();
        const sig = crypto
            .createHmac("sha256", hmacSecret)
            .update(`${tsMs}.${rawBodyString}`)
            .digest("hex");
        headers["x-webhook-timestamp"] = String(tsMs);
        headers["x-webhook-signature"] = sig;
    }
    return headers;
}

module.exports = { getOndialWebhookFetchHeaders };
