/**
 * Offline smoke: Meta WhatsApp signature verify (no Redis required).
 *
 *   node scripts/test-meta-whatsapp-signature.js
 */
const assert = require("assert");
const crypto = require("crypto");
const { verifyMetaWhatsappSignature } = require("../lib/metaWhatsappSignature");

const secret = "test-app-secret";
const body = '{"object":"whatsapp_business_account","entry":[]}';
const digest = crypto.createHmac("sha256", secret).update(body).digest("hex");

assert.strictEqual(
    verifyMetaWhatsappSignature(body, `sha256=${digest}`, secret),
    true,
    "valid signature should pass"
);
assert.strictEqual(
    verifyMetaWhatsappSignature(body, `sha256=${digest}`, ""),
    false,
    "empty secret should fail closed"
);
assert.strictEqual(
    verifyMetaWhatsappSignature(body, "sha256=deadbeef", secret),
    false,
    "bad signature should fail"
);
assert.strictEqual(
    verifyMetaWhatsappSignature(body, null, secret),
    false,
    "missing header should fail"
);

console.log("OK: meta WhatsApp signature checks passed");
