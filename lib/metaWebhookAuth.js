const crypto = require("crypto");
const { verifyMetaWhatsappSignature } = require("./metaWhatsappSignature");

/**
 * Meta webhook ingress auth for OnDial's own app plus white-label partner apps.
 *
 * OnDial's env secret / verify token are always tried first, exactly as before.
 * Only when that fails are partner (WL) secrets / tokens loaded and tried.
 * Loaders are injected so this stays pure and testable without Mongo.
 */

/** phone_number_id values in a Meta webhook body (deduped, order kept). */
function extractMetaPhoneNumberIds(payload) {
    const out = [];
    const seen = new Set();
    const entries = Array.isArray(payload?.entry) ? payload.entry : [];
    for (const entry of entries) {
        const changes = Array.isArray(entry?.changes) ? entry.changes : [];
        for (const change of changes) {
            const id = String(change?.value?.metadata?.phone_number_id || "").trim();
            if (id && !seen.has(id)) {
                seen.add(id);
                out.push(id);
            }
        }
    }
    return out;
}

/** Constant-time string compare (hash first so lengths never leak). */
function safeEqualString(a, b) {
    const sa = String(a ?? "");
    const sb = String(b ?? "");
    if (!sa || !sb) return false;
    const ha = crypto.createHash("sha256").update(sa, "utf8").digest();
    const hb = crypto.createHash("sha256").update(sb, "utf8").digest();
    return crypto.timingSafeEqual(ha, hb);
}

/**
 * @returns {Promise<{ok:boolean, via?:'env'|'partner', status?:number, reason?:string}>}
 *  status is only set when ok=false: 503 (nothing configured at all) or 401.
 */
async function authorizeMetaWebhookPost({
    rawBody,
    signature,
    envSecret,
    payload,
    loadPartnerSecrets,
}) {
    const secret = String(envSecret || "").trim();
    if (secret && verifyMetaWhatsappSignature(rawBody, signature, secret)) {
        return { ok: true, via: "env" };
    }

    if (signature && typeof loadPartnerSecrets === "function") {
        const ids = extractMetaPhoneNumberIds(payload);
        if (ids.length) {
            const anyMatch = (candidates) => {
                let matched = false;
                // Check every candidate (no early exit) so timing does not depend on position.
                for (const candidate of candidates || []) {
                    if (verifyMetaWhatsappSignature(rawBody, signature, candidate)) matched = true;
                }
                return matched;
            };
            if (anyMatch(await loadPartnerSecrets(ids, { refresh: false }))) {
                return { ok: true, via: "partner" };
            }
            // Cache may predate a partner's new secret; loader rate-limits the reload (null = skipped).
            const reloaded = await loadPartnerSecrets(ids, { refresh: true });
            if (reloaded && anyMatch(reloaded)) return { ok: true, via: "partner" };
        }
    }

    if (!secret) return { ok: false, status: 503, reason: "not_configured" };
    return { ok: false, status: 401, reason: "invalid_signature" };
}

/**
 * @returns {Promise<{ok:boolean, via?:'env'|'partner', status?:number, reason?:string}>}
 */
async function authorizeMetaWebhookVerify({ token, envToken, loadPartnerVerifyTokens }) {
    const expected = String(envToken || "").trim();
    const given = String(token || "").trim();
    if (expected && given === expected) return { ok: true, via: "env" };

    if (given && typeof loadPartnerVerifyTokens === "function") {
        const anyMatch = (candidates) => {
            let matched = false;
            for (const candidate of candidates || []) {
                if (safeEqualString(given, String(candidate || "").trim())) matched = true;
            }
            return matched;
        };
        if (anyMatch(await loadPartnerVerifyTokens({ refresh: false }))) {
            return { ok: true, via: "partner" };
        }
        const reloaded = await loadPartnerVerifyTokens({ refresh: true });
        if (reloaded && anyMatch(reloaded)) return { ok: true, via: "partner" };
    }

    if (!expected) return { ok: false, status: 503, reason: "not_configured" };
    return { ok: false, status: 403, reason: "token_mismatch" };
}

module.exports = {
    extractMetaPhoneNumberIds,
    safeEqualString,
    authorizeMetaWebhookPost,
    authorizeMetaWebhookVerify,
};
