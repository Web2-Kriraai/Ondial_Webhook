/**
 * Check Telnyx numbers: Ondial inventory first, then live Telnyx API.
 *   node scripts/check-telnyx-available-numbers.js
 */
const fs = require("fs");
const path = require("path");
const { MongoClient } = require("mongodb");

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

async function telnyxGet(apiPath) {
  const key = process.env.TELNYX_API_KEY?.trim();
  if (!key) throw new Error("TELNYX_API_KEY missing");
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
    json = { raw: text };
  }
  if (!res.ok) {
    const detail =
      json?.errors?.[0]?.detail || json?.errors?.[0]?.title || text || res.statusText;
    const err = new Error(`Telnyx ${res.status}: ${detail}`);
    err.status = res.status;
    throw err;
  }
  return json;
}

(async () => {
  loadEnv(path.join("C:/Users/ADMIN/Documents/GitHub/Ondial/.env"));
  loadEnv(path.join("C:/Users/ADMIN/Documents/GitHub/Ondial_Webhook/.env"));
  loadEnv(path.join("C:/Users/ADMIN/Documents/GitHub/Ondial-Super-Admin/.env"));

  const out = { ondailInventory: null, userAssigned: null, telnyxOwned: null, telnyxAvailableUS: null };

  // --- 1) Our Mongo inventory ---
  const client = new MongoClient(process.env.MONGODB_URI);
  await client.connect();
  const db = client.db();

  // PhoneNumber model → collection "phonenumbers"
  const inv = await db
    .collection("phonenumbers")
    .find({ provider: "telnyx" })
    .project({
      number: 1,
      status: 1,
      provider: 1,
      country: 1,
      countryName: 1,
      purchasedBy: 1,
      isTrialNumber: 1,
    })
    .limit(500)
    .toArray();
  out.inventoryCollection = "phonenumbers";

  const availableInv = inv.filter((n) => String(n.status) === "available");
  out.ondailInventory = {
    totalTelnyxInInventory: inv.length,
    available: availableInv.length,
    purchased: inv.filter((n) => n.status === "purchased").length,
    trial: inv.filter((n) => n.status === "trial").length,
    byStatus: inv.reduce((acc, n) => {
      const s = String(n.status || "unknown");
      acc[s] = (acc[s] || 0) + 1;
      return acc;
    }, {}),
    availableSample: availableInv.slice(0, 20).map((n) => ({
      number: n.number,
      country: n.country,
      countryName: n.countryName,
      isTrialNumber: n.isTrialNumber || false,
    })),
    purchasedSample: inv
      .filter((n) => n.status === "purchased")
      .slice(0, 10)
      .map((n) => ({ number: n.number, country: n.country, purchasedBy: n.purchasedBy })),
  };

  // Users with telnyx numbers on account
  const usersWithTelnyx = await db
    .collection("users")
    .find({
      $or: [
        { "phoneNumbers.provider": /telnyx/i },
        { "phoneNumbers.providerName": /telnyx/i },
        { allowedProviders: "telnyx" },
      ],
    })
    .project({ email: 1, phoneNumbers: 1, allowedProviders: 1 })
    .limit(50)
    .toArray();

  const assigned = [];
  for (const u of usersWithTelnyx) {
    for (const pn of u.phoneNumbers || []) {
      if (!/telnyx/i.test(String(pn.provider || pn.providerName || ""))) continue;
      assigned.push({
        email: u.email,
        number: pn.number || pn.phoneNumber || null,
        status: pn.status || null,
      });
    }
  }
  out.userAssigned = {
    usersWithTelnyxAllowOrNumbers: usersWithTelnyx.length,
    assignedTelnyxNumbers: assigned.length,
    sample: assigned.slice(0, 20),
  };

  await client.close();

  // --- 2) Direct Telnyx ---
  try {
    const owned = await telnyxGet("/phone_numbers?page[size]=50&page[number]=1");
    const rows = Array.isArray(owned?.data) ? owned.data : [];
    const total =
      owned?.meta?.total_results ?? owned?.metadata?.total_results ?? rows.length;
    out.telnyxOwned = {
      total,
      sample: rows.slice(0, 20).map((n) => ({
        id: n.id,
        phoneNumber: n.phone_number,
        status: n.status,
        connectionId: n.connection_id || n.connection_name || null,
      })),
    };
  } catch (e) {
    out.telnyxOwned = { error: e.message };
  }

  try {
    // Available to purchase in US (for testing USA destination / DID)
    const qs = new URLSearchParams({
      "filter[country_code]": "US",
      "filter[features]": "voice",
      "filter[limit]": "20",
    });
    const avail = await telnyxGet(`/available_phone_numbers?${qs.toString()}`);
    const rows = Array.isArray(avail?.data) ? avail.data : [];
    out.telnyxAvailableUS = {
      count: rows.length,
      totalResults: avail?.meta?.total_results ?? avail?.metadata?.total_results ?? null,
      sample: rows.slice(0, 15).map((n) => ({
        phoneNumber: n.phone_number || n?.phone_number?.phone_number || n?.record_type,
        phone:
          typeof n.phone_number === "string"
            ? n.phone_number
            : n.phone_number?.phone_number || n?.phone_number || null,
        region: n.region_information || n.region || null,
        cost: n.cost_information || n.monthly_cost || null,
      })),
    };
  } catch (e) {
    out.telnyxAvailableUS = { error: e.message };
  }

  // CA too (user matrix keeps CA)
  try {
    const qs = new URLSearchParams({
      "filter[country_code]": "CA",
      "filter[features]": "voice",
      "filter[limit]": "10",
    });
    const avail = await telnyxGet(`/available_phone_numbers?${qs.toString()}`);
    const rows = Array.isArray(avail?.data) ? avail.data : [];
    out.telnyxAvailableCA = {
      count: rows.length,
      sample: rows.slice(0, 5).map((n) => ({
        phone:
          typeof n.phone_number === "string"
            ? n.phone_number
            : n.phone_number?.phone_number || null,
      })),
    };
  } catch (e) {
    out.telnyxAvailableCA = { error: e.message };
  }

  console.log(JSON.stringify(out, null, 2));
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
