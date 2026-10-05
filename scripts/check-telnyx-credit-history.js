const fs = require("fs");
const path = require("path");

function loadEnv(f) {
  if (!fs.existsSync(f)) return;
  for (const line of fs.readFileSync(f, "utf8").split(/\r?\n/)) {
    const t = line.trim();
    if (!t || t.startsWith("#")) continue;
    const i = t.indexOf("=");
    if (i < 1) continue;
    let k = t.slice(0, i).trim();
    let v = t.slice(i + 1).trim();
    if (
      (v.startsWith('"') && v.endsWith('"')) ||
      (v.startsWith("'") && v.endsWith("'"))
    ) {
      v = v.slice(1, -1);
    }
    if (!process.env[k]) process.env[k] = v;
  }
}

loadEnv(path.join("C:/Users/ADMIN/Documents/GitHub/Ondial/.env"));
loadEnv(path.join("C:/Users/ADMIN/Documents/GitHub/Ondial_Webhook/.env"));

const key = process.env.TELNYX_API_KEY?.trim();
if (!key) {
  console.error("TELNYX_API_KEY missing");
  process.exit(1);
}

async function get(apiPath) {
  const res = await fetch(`https://api.telnyx.com/v2${apiPath}`, {
    headers: {
      Authorization: `Bearer ${key}`,
      Accept: "application/json",
    },
  });
  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = { raw: text.slice(0, 800) };
  }
  return { status: res.status, json };
}

(async () => {
  const paths = [
    "/balance",
    "/ledger",
    "/ledger_statements",
    "/payment/methods",
    "/payments",
    "/invoices",
    "/detail_records?filter[record_type]=payment&page[size]=50",
    "/detail_records?page[size]=5",
    "/billing_groups",
    "/user_addresses",
  ];

  for (const p of paths) {
    const r = await get(p);
    console.log(`\n=== ${p} HTTP ${r.status} ===`);
    console.log(JSON.stringify(r.json, null, 2).slice(0, 2500));
  }
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
