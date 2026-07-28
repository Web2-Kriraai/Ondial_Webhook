/**
 * Sync Telnyx (and related) env keys across Ondial / Webhook / Calling / Super-Admin.
 * Copies secrets from Ondial/.env in memory — never prints secret values.
 *
 * Run from Ondial_Webhook or any repo:
 *   node path/to/scripts/sync-telnyx-env.js
 */
const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..", "..");
const PATHS = {
  ondial: path.join(ROOT, "Ondial", ".env"),
  webhook: path.join(ROOT, "Ondial_Webhook", ".env"),
  calling: path.join(ROOT, "Calling_system1", ".env"),
  superAdmin: path.join(ROOT, "Ondial-Super-Admin", ".env"),
};

function parseEnv(text) {
  const map = new Map();
  for (const line of String(text || "").split(/\r?\n/)) {
    if (!line || /^\s*#/.test(line)) continue;
    const i = line.indexOf("=");
    if (i <= 0) continue;
    const k = line.slice(0, i).trim();
    const v = line.slice(i + 1).trim();
    if (k) map.set(k, v);
  }
  return map;
}

function upsertBlock(filePath, blockLines, afterKeyHints = []) {
  if (!fs.existsSync(filePath)) {
    console.log(`SKIP missing ${filePath}`);
    return;
  }
  let text = fs.readFileSync(filePath, "utf8");
  const eol = text.includes("\r\n") ? "\r\n" : "\n";
  const lines = text.split(/\r?\n/);

  // Parse desired key=value (ignore pure comment lines in the block for upsert)
  const desired = new Map();
  for (const raw of blockLines) {
    if (!raw || /^\s*#/.test(raw) || !raw.includes("=")) continue;
    const i = raw.indexOf("=");
    desired.set(raw.slice(0, i).trim(), raw.slice(i + 1));
  }

  const present = new Set();
  const out = lines.map((line) => {
    const m = line.match(/^\s*([A-Z0-9_]+)\s*=/);
    if (!m) return line;
    const key = m[1];
    if (!desired.has(key)) return line;
    present.add(key);
    return `${key}=${desired.get(key)}`;
  });

  const missing = [...desired.keys()].filter((k) => !present.has(k));
  if (missing.length) {
    // Prefer insert after last matching hint key, else append.
    let insertAt = out.length;
    for (let i = out.length - 1; i >= 0; i--) {
      const m = out[i].match(/^\s*([A-Z0-9_]+)\s*=/);
      if (m && afterKeyHints.includes(m[1])) {
        insertAt = i + 1;
        break;
      }
      if (m && desired.has(m[1])) {
        insertAt = i + 1;
        break;
      }
    }
    const toInsert = blockLines.filter((raw) => {
      if (!raw || /^\s*#/.test(raw) || !raw.includes("=")) return true; // keep comments only when appending a fresh block
      const key = raw.slice(0, raw.indexOf("=")).trim();
      return missing.includes(key);
    });
    // If some keys already existed, only insert missing key lines (no duplicate header comments).
    const onlyMissingLines = toInsert.filter((raw) => {
      if (!raw.includes("=") || /^\s*#/.test(raw)) return missing.length === desired.size;
      const key = raw.slice(0, raw.indexOf("=")).trim();
      return missing.includes(key);
    });
    out.splice(insertAt, 0, ...onlyMissingLines);
  }

  const next = out.join(eol).replace(/\r?\n$/, "") + eol;
  fs.writeFileSync(filePath, next, "utf8");
  console.log(`OK  ${path.relative(ROOT, filePath)} — upserted ${[...desired.keys()].join(", ")}`);
}

function main() {
  const src = parseEnv(fs.readFileSync(PATHS.ondial, "utf8"));
  const apiKey = src.get("TELNYX_API_KEY") || "";
  const connId = src.get("TELNYX_CONNECTION_ID") || "3008826073862375087";
  const twilioSid = src.get("TWILIO_ACCOUNT_SID") || "";
  const twilioTok = src.get("TWILIO_AUTH_TOKEN") || "";
  const shared =
    src.get("WEBHOOK_SHARED_SECRET") ||
    src.get("WEBHOOK_SECRET") ||
    "";

  if (!apiKey) {
    console.error("TELNYX_API_KEY missing in Ondial/.env — abort");
    process.exit(1);
  }

  // Inventory notes (non-secret) discovered from Telnyx API 2026-07-25 / verified 2026-07-28:
  // Number +14078879770 (id 3008751214553728324) is on fax app Ondial-Yash
  // (3009498078286710470) — must be rebound to Call Control ondial-test.
  // Call Control ondial-test webhook_event_url must be {host}/telnyx/webhooks
  // (fix with: node scripts/fix-telnyx-webhook-url.js).

  const ondialBlock = [
    "TELNYX_API_KEY=" + apiKey,
    "TELNYX_CONNECTION_ID=" + connId,
    "# Call Control App: ondial-test. Webhook must be {WEBHOOK_HOST}/telnyx/webhooks",
    "# Owned number (must be bound to TELNYX_CONNECTION_ID): +14078879770 id=3008751214553728324",
    "TELNYX_PUBLIC_KEY=",
    "TELNYX_WEBHOOK_VERIFY=1",
    "# TELNYX_REQUIREMENT_GROUP_ID=",
    "# TELNYX_ORDER_POLL_ATTEMPTS=12",
    "# TELNYX_ORDER_POLL_MS=1500",
  ];

  const webhookBlock = [
    "TELNYX_API_KEY=" + apiKey,
    "TELNYX_CONNECTION_ID=" + connId,
    "TELNYX_PUBLIC_KEY=",
    "TELNYX_WEBHOOK_VERIFY=1",
    "# TELNYX_WEBHOOK_MAX_SKEW_SEC=300",
    "# TELNYX_WEBHOOK_EVENT_TTL_SEC=86400",
    "TWILIO_ACCOUNT_SID=" + twilioSid,
    "TWILIO_AUTH_TOKEN=" + twilioTok,
    "WEBHOOK_INTERNAL_SECRET=" + shared,
    "WEBHOOK_SHARED_SECRET=" + shared,
  ];

  const callingBlock = [
    "TELNYX_API_KEY=" + apiKey,
    "TELNYX_CONNECTION_ID=" + connId,
    "TELNYX_MAPPING_TIMEOUT_MS=2500",
  ];

  const superAdminBlock = [
    "# Carrier release (Twilio / Telnyx) when deallocating lines",
    "TWILIO_ACCOUNT_SID=" + twilioSid,
    "TWILIO_AUTH_TOKEN=" + twilioTok,
    "TELNYX_API_KEY=" + apiKey,
  ];

  upsertBlock(PATHS.ondial, ondialBlock, ["TELNYX_CONNECTION_ID", "TELNYX_API_KEY", "TWILIO_BYO_ENCRYPTION_KEY"]);
  upsertBlock(PATHS.webhook, webhookBlock, ["TELNYX_CONNECTION_ID", "TELNYX_API_KEY", "WEBHOOK_SECRET"]);
  upsertBlock(PATHS.calling, callingBlock, ["TELNYX_CONNECTION_ID", "TELNYX_API_KEY", "TELNYX_MAPPING_API_URL"]);
  upsertBlock(PATHS.superAdmin, superAdminBlock, ["FREJUN_API_KEY", "CALLING_API_KEY", "MONGODB_URI"]);

  console.log("");
  console.log("Synced. TELNYX_PUBLIC_KEY left empty — paste from Mission Control → API Keys → Public Key.");
  console.log("CONNECTION_ID kept as Call Control ondial-test (" + connId + ").");
  console.log("Account number +14078879770 is still on fax app Ondial-Yash — rebind required.");
}

main();
