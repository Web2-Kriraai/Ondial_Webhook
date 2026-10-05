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
    json = { raw: text.slice(0, 500) };
  }
  return { status: res.status, json, text };
}

(async () => {
  const fileIds = [
    "19eb7325-2bfe-472b-a6d7-7dc5ea98d8d8",
    "dc731a79-ad2f-440e-940d-c037e7003d60",
  ];
  for (const id of fileIds) {
    for (const p of [`/files/${id}`, `/invoices/${id}`, `/files/${id}/download`]) {
      const r = await get(p);
      console.log(p, r.status, JSON.stringify(r.json).slice(0, 500));
    }
  }
})().catch(console.error);
