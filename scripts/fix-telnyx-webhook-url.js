/**
 * Ensure Telnyx Call Control App webhook_event_url points at /telnyx/webhooks.
 *
 * Usage:
 *   node scripts/fix-telnyx-webhook-url.js
 *   TELNYX_WEBHOOK_EVENT_URL=https://dev-api.ondial.ai/telnyx/webhooks node scripts/fix-telnyx-webhook-url.js
 */
require("dotenv").config({ quiet: true });

const CONNECTION_ID = String(process.env.TELNYX_CONNECTION_ID || "").trim();
const API_KEY = String(process.env.TELNYX_API_KEY || "").trim();
const TARGET_URL = String(
  process.env.TELNYX_WEBHOOK_EVENT_URL ||
    "https://dev-api.ondial.ai/telnyx/webhooks"
).trim();

async function telnyx(method, path, body) {
  const res = await fetch(`https://api.telnyx.com/v2${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${API_KEY}`,
      Accept: "application/json",
      ...(body ? { "Content-Type": "application/json" } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    const detail =
      json?.errors?.[0]?.detail || json?.errors?.[0]?.title || res.statusText;
    throw new Error(`${method} ${path} → ${res.status}: ${detail}`);
  }
  return json.data || json;
}

async function main() {
  if (!API_KEY) throw new Error("TELNYX_API_KEY missing");
  if (!CONNECTION_ID) throw new Error("TELNYX_CONNECTION_ID missing");
  if (!TARGET_URL.includes("/telnyx/webhooks")) {
    throw new Error(
      `Refusing target URL without /telnyx/webhooks: ${TARGET_URL}`
    );
  }

  const before = await telnyx(
    "GET",
    `/call_control_applications/${CONNECTION_ID}`
  );
  const prev = String(before.webhook_event_url || "").trim();
  console.log(
    JSON.stringify(
      {
        connection_id: CONNECTION_ID,
        name: before.application_name || null,
        before: prev || null,
        target: TARGET_URL,
      },
      null,
      2
    )
  );

  if (prev === TARGET_URL) {
    console.log("OK — webhook_event_url already correct; no PATCH needed.");
    return;
  }

  const after = await telnyx(
    "PATCH",
    `/call_control_applications/${CONNECTION_ID}`,
    { webhook_event_url: TARGET_URL }
  );
  console.log(
    JSON.stringify(
      {
        patched: true,
        after: after.webhook_event_url || null,
      },
      null,
      2
    )
  );
  console.log("OK — Mission Control webhook_event_url updated.");
}

main().catch((err) => {
  console.error("FAIL:", err.message || err);
  process.exit(1);
});
