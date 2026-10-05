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

const key = process.env.TELNYX_API_KEY?.trim();

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
    json = { raw: text.slice(0, 1000) };
  }
  return { status: res.status, json };
}

(async () => {
  const inv = await get("/invoices?page[size]=20");
  console.log("invoices list", JSON.stringify(inv.json, null, 2));

  const ids = (inv.json?.data || []).map((x) => x.invoice_id);
  for (const id of ids) {
    const d = await get(`/invoices/${id}`);
    console.log(`\n=== invoice ${id} HTTP ${d.status} ===`);
    console.log(JSON.stringify(d.json, null, 2).slice(0, 4000));
  }

  // Try common detail record types that might show credits/payments
  for (const rt of [
    "call",
    "cost",
    "media-storage",
    "messaging",
    "wireless",
  ]) {
    const r = await get(
      `/detail_records?filter[record_type]=${encodeURIComponent(rt)}&page[size]=3`
    );
    console.log(`\n=== detail_records ${rt} HTTP ${r.status} ===`);
    console.log(JSON.stringify(r.json, null, 2).slice(0, 800));
  }
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
