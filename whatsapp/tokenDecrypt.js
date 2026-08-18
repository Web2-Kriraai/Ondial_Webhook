const crypto = require("crypto");

const ALGORITHM = "aes-256-cbc";

function getKey() {
  const keyStr = process.env.TOKEN_ENCRYPTION_KEY;
  if (!keyStr) {
    throw new Error("TOKEN_ENCRYPTION_KEY is required to decrypt WhatsApp credentials");
  }
  if (keyStr.length === 32) return Buffer.from(keyStr, "utf8");
  if (keyStr.length === 64) return Buffer.from(keyStr, "hex");
  return crypto.createHash("sha256").update(String(keyStr)).digest();
}

function isEncryptedToken(text) {
  if (!text || typeof text !== "string") return false;
  if (text.includes(":")) {
    const parts = text.split(":");
    return parts.length === 2 && parts[0].length === 32;
  }
  return false;
}

/**
 * Returns plaintext token, or null if encrypted value cannot be decrypted.
 * Plaintext (legacy) values are returned as-is.
 */
function tryDecryptToken(text) {
  if (!text) return null;
  if (!isEncryptedToken(text)) return String(text);
  const textParts = String(text).split(":");
  try {
    const iv = Buffer.from(textParts.shift(), "hex");
    const encryptedText = Buffer.from(textParts.join(":"), "hex");
    const decipher = crypto.createDecipheriv(ALGORITHM, getKey(), iv);
    let decrypted = decipher.update(encryptedText);
    decrypted = Buffer.concat([decrypted, decipher.final()]);
    return decrypted.toString();
  } catch (err) {
    console.warn(
      "[WhatsApp] Token decryption failed (wrong TOKEN_ENCRYPTION_KEY or corrupt token)",
      err?.code || err?.message
    );
    return null;
  }
}

module.exports = {
  tryDecryptToken,
  isEncryptedToken,
};
