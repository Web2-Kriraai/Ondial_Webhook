/**
 * 1) Upsert Telnyx US rows into provider_rate_cards from telnyx_us_rows.jsonl
 * 2) Keep countryPricingV1 countries only: IN, US, CA, AU
 *
 *   node scripts/import-us-and-trim-matrix.js
 *   node scripts/import-us-and-trim-matrix.js --dry-run
 */
const fs = require("fs");
const path = require("path");
const readline = require("readline");
const { MongoClient } = require("mongodb");

const KEEP_ISOS = ["IN", "US", "CA", "AU"];
const EFFECTIVE_FROM = new Date("2025-10-01T00:00:00.000Z");

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

function routeTypeFromDescription(desc) {
  return /mobile/i.test(String(desc || "")) ? "mobile" : "landline";
}

function round6(n) {
  return parseFloat(Number(n).toFixed(6));
}

function buildSuggestedPackages(baseSell) {
  const tierMultiplier = { standard: 1, premium: 1.08, elite: 1.16 };
  const packages = {};
  for (const pkg of ["starter", "professional", "enterprise", "premium"]) {
    packages[pkg] = {};
    for (const tier of ["standard", "premium", "elite"]) {
      const rate = round6(baseSell * (tierMultiplier[tier] || 1));
      packages[pkg][tier] = { list: rate, sale: rate };
    }
  }
  return packages;
}

async function main() {
  loadEnv(path.join(__dirname, "..", ".env"));
  loadEnv(path.join("C:/Users/ADMIN/Documents/GitHub/Ondial/.env"));
  const dryRun = process.argv.includes("--dry-run");
  const uri = process.env.MONGODB_URI;
  if (!uri) {
    console.error("MONGODB_URI missing");
    process.exit(1);
  }

  const jsonl = path.join(__dirname, "telnyx_us_rows.jsonl");
  if (!fs.existsSync(jsonl)) {
    console.error("Missing", jsonl);
    process.exit(1);
  }

  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db();
  const cards = db.collection("provider_rate_cards");

  const beforeUs = await cards.countDocuments({ provider: "telnyx", countryIso: "US" });
  console.log("US cards before:", beforeUs);

  let ops = [];
  let read = 0;
  let upserted = 0;
  let modified = 0;
  const rl = readline.createInterface({
    input: fs.createReadStream(jsonl, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  async function flush() {
    if (!ops.length) return;
    if (dryRun) {
      upserted += ops.length;
      ops = [];
      return;
    }
    const res = await cards.bulkWrite(ops, { ordered: false });
    upserted += res.upsertedCount || 0;
    modified += res.modifiedCount || 0;
    ops = [];
  }

  for await (const line of rl) {
    if (!line.trim()) continue;
    const row = JSON.parse(line);
    read++;
    const destinationPrefix = String(row.destinationPrefix || "").replace(/\D/g, "");
    const rate = Number(row.rateUsdPerMin);
    if (!destinationPrefix || !Number.isFinite(rate)) continue;
    const routeType = routeTypeFromDescription(row.description);
    const doc = {
      provider: "telnyx",
      countryIso: "US",
      destinationPrefix,
      routeType,
      rateUsdPerMin: rate,
      interval1Sec: Number(row.interval1Sec) || 60,
      intervalNSec: Number(row.intervalNSec) || 60,
      perCallFeeUsd: Number(row.perCallFeeUsd) || 0,
      description: row.description || null,
      effectiveFrom: EFFECTIVE_FROM,
      source: "telnyx-rate-deck-us-detail",
      importedAt: new Date(),
    };
    ops.push({
      updateOne: {
        filter: {
          provider: "telnyx",
          destinationPrefix,
          routeType,
          effectiveFrom: EFFECTIVE_FROM,
        },
        update: { $set: doc },
        upsert: true,
      },
    });
    if (ops.length >= 500) {
      await flush();
      if (read % 5000 === 0) console.log("…progress", read);
    }
  }
  await flush();

  const afterUs = dryRun
    ? beforeUs
    : await cards.countDocuments({ provider: "telnyx", countryIso: "US" });
  console.log(
    dryRun
      ? `Dry-run US rows read=${read} wouldUpsertOps≈${upserted}`
      : `US import done read=${read} upserted=${upserted} modified=${modified} countNow=${afterUs}`
  );

  // Trim countryPricingV1
  const pricingDoc = await db.collection("systemsettings").findOne({ key: "countryPricingV1" });
  const countries =
    pricingDoc?.value?.countries && typeof pricingDoc.value.countries === "object"
      ? pricingDoc.value.countries
      : {};
  const beforeKeys = Object.keys(countries);
  const nextCountries = {};
  for (const iso of KEEP_ISOS) {
    if (countries[iso]) {
      nextCountries[iso] = countries[iso];
    }
  }

  // Ensure US has telnyx overlay from deck max×commission if missing provider overlay
  const commissionDoc = await db
    .collection("systemsettings")
    .findOne({ key: "providerCommissionV1" });
  const commissionPct =
    Number(commissionDoc?.value?.byProvider?.telnyx) ||
    Number(commissionDoc?.value?.defaultPercent) ||
    40;
  const usMax = 0.181; // from Country Summary
  const usSell = round6(usMax * (1 + commissionPct / 100));
  if (!nextCountries.US) {
    nextCountries.US = {
      salePriceEnabled: false,
      packages: buildSuggestedPackages(usSell),
      concurrentCallCost: 7,
      phoneNumberCost: 7,
      providers: {
        telnyx: {
          salePriceEnabled: false,
          packages: buildSuggestedPackages(usSell),
        },
      },
    };
  } else {
    const us = { ...nextCountries.US };
    const providers = { ...(us.providers || {}) };
    if (!providers.telnyx?.packages) {
      providers.telnyx = {
        salePriceEnabled: false,
        packages: buildSuggestedPackages(usSell),
      };
      us.providers = providers;
      nextCountries.US = us;
    }
  }

  // Ensure CA/AU/IN remain (already copied if present)
  const removed = beforeKeys.filter((k) => !KEEP_ISOS.includes(k));
  console.log("Matrix before:", beforeKeys.length, "keep:", KEEP_ISOS.join(","));
  console.log("Removing:", removed.length, removed.slice(0, 40).join(",") + (removed.length > 40 ? "…" : ""));

  if (!dryRun) {
    const nextValue = {
      ...(pricingDoc?.value && typeof pricingDoc.value === "object" ? pricingDoc.value : {}),
      enabled: true,
      countries: nextCountries,
      fallbackOrder: ["US", "IN"],
    };
    await db.collection("systemsettings").updateOne(
      { key: "countryPricingV1" },
      {
        $set: {
          key: "countryPricingV1",
          value: nextValue,
          updatedAt: new Date(),
        },
        $setOnInsert: { createdAt: new Date() },
      },
      { upsert: true }
    );
    await db
      .collection("adminauditlogs")
      .insertOne({
        action: "country_pricing.trim_and_us_import",
        resource: "countryPricingV1+provider_rate_cards",
        adminEmail: "script:import-us-and-trim-matrix",
        before: { countries: beforeKeys.length, usCards: beforeUs },
        after: {
          countries: Object.keys(nextCountries),
          removedCount: removed.length,
          usCards: afterUs,
          usSell,
        },
        createdAt: new Date(),
        updatedAt: new Date(),
      })
      .catch(() => {});
    console.log("Applied. Matrix countries now:", Object.keys(nextCountries).join(", "));
  } else {
    console.log("Dry-run only — no DB writes.");
  }

  await client.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
