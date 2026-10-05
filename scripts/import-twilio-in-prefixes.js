/**
 * Seed Twilio IN prefixes into provider_rate_cards from twilio_in_rows.jsonl
 * (India Detail rates — interim until a real Twilio deck is imported).
 *
 *   node scripts/import-twilio-in-prefixes.js
 *   node scripts/import-twilio-in-prefixes.js --dry-run
 */
const fs = require("fs");
const path = require("path");
const readline = require("readline");
const { MongoClient } = require("mongodb");

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

async function main() {
  loadEnv(path.join(__dirname, "..", ".env"));
  loadEnv(path.join("C:/Users/ADMIN/Documents/GitHub/Ondial/.env"));
  const dryRun = process.argv.includes("--dry-run");
  const uri = process.env.MONGODB_URI;
  if (!uri) {
    console.error("MONGODB_URI missing");
    process.exit(1);
  }

  const jsonl = path.join(__dirname, "twilio_in_rows.jsonl");
  if (!fs.existsSync(jsonl)) {
    console.error("Missing", jsonl);
    process.exit(1);
  }

  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db();
  const cards = db.collection("provider_rate_cards");

  const before = await cards.countDocuments({ provider: "twilio", countryIso: "IN" });
  console.log("Twilio IN cards before:", before);

  let ops = [];
  let read = 0;
  let upserted = 0;
  let modified = 0;

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

  const rl = readline.createInterface({
    input: fs.createReadStream(jsonl, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    if (!line.trim()) continue;
    const row = JSON.parse(line);
    read++;
    const destinationPrefix = String(row.destinationPrefix || "").replace(/\D/g, "");
    const rate = Number(row.rateUsdPerMin);
    if (!destinationPrefix || !Number.isFinite(rate)) continue;
    const routeType = routeTypeFromDescription(row.description);
    const doc = {
      provider: "twilio",
      countryIso: "IN",
      destinationPrefix,
      routeType,
      rateUsdPerMin: rate,
      interval1Sec: Number(row.interval1Sec) || 60,
      intervalNSec: Number(row.intervalNSec) || 60,
      perCallFeeUsd: Number(row.perCallFeeUsd) || 0,
      description: row.description || null,
      effectiveFrom: EFFECTIVE_FROM,
      source: "india-detail-seed-as-twilio",
      importedAt: new Date(),
    };
    ops.push({
      updateOne: {
        filter: {
          provider: "twilio",
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
      if (read % 2000 === 0) console.log("…", read);
    }
  }
  await flush();

  const after = dryRun
    ? before
    : await cards.countDocuments({ provider: "twilio", countryIso: "IN" });

  console.log(
    dryRun
      ? `Dry-run read=${read} ops≈${upserted}`
      : `Done read=${read} upserted=${upserted} modified=${modified} twilioIN=${after}`
  );

  if (!dryRun) {
    await db
      .collection("adminauditlogs")
      .insertOne({
        action: "provider_rate_cards.twilio_in_seed",
        resource: "provider_rate_cards",
        adminEmail: "script:import-twilio-in-prefixes",
        before: { twilioIN: before },
        after: { twilioIN: after, read },
        createdAt: new Date(),
        updatedAt: new Date(),
      })
      .catch(() => {});
  }

  await client.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
