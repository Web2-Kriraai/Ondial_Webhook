/**
 * Compare Telnyx Country Summary (Excel export JSON) vs provider_rate_cards in Mongo.
 *   node scripts/compare-telnyx-deck-vs-db.js
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

function isValidIso(iso) {
  return /^[A-Z]{2}$/.test(iso) && iso !== "XX" && iso !== "ZZ";
}

async function main() {
  loadEnv(path.join(__dirname, "..", ".env"));
  loadEnv(path.join("C:/Users/ADMIN/Documents/GitHub/Ondial/.env"));

  const summaryPath = path.join(__dirname, "telnyx_country_summary.json");
  if (!fs.existsSync(summaryPath)) {
    console.error("Missing", summaryPath);
    process.exit(1);
  }
  const deck = JSON.parse(fs.readFileSync(summaryPath, "utf8")).filter((r) =>
    isValidIso(String(r.iso || "").toUpperCase())
  );

  const client = new MongoClient(process.env.MONGODB_URI);
  await client.connect();
  const database = client.db();

  const buckets = await database
    .collection("provider_rate_cards")
    .aggregate([
      { $match: { provider: "telnyx" } },
      { $group: { _id: "$countryIso", rows: { $sum: 1 } } },
    ])
    .toArray();

  const dbByIso = new Map();
  let dbTotal = 0;
  for (const b of buckets) {
    const iso = b._id == null || b._id === "" ? "(null)" : String(b._id).toUpperCase();
    dbByIso.set(iso, b.rows);
    dbTotal += b.rows;
  }

  let excelTotal = 0;
  const missingCountries = [];
  const shortCountries = [];
  const okCountries = [];

  for (const row of deck) {
    const iso = String(row.iso).toUpperCase();
    const excelRows = Number(row.rows) || 0;
    excelTotal += excelRows;
    const dbRows = dbByIso.get(iso) || 0;
    if (dbRows === 0) {
      missingCountries.push({
        iso,
        name: row.name,
        excelRows,
        dbRows: 0,
        missingRows: excelRows,
      });
    } else if (dbRows < excelRows) {
      shortCountries.push({
        iso,
        name: row.name,
        excelRows,
        dbRows,
        missingRows: excelRows - dbRows,
      });
    } else {
      okCountries.push({ iso, excelRows, dbRows });
    }
  }

  missingCountries.sort((a, b) => b.excelRows - a.excelRows);
  shortCountries.sort((a, b) => b.missingRows - a.missingRows);

  const missingRowSum = missingCountries.reduce((s, r) => s + r.missingRows, 0);
  const shortRowSum = shortCountries.reduce((s, r) => s + r.missingRows, 0);

  console.log("=== Telnyx Excel vs provider_rate_cards ===");
  console.log("Excel countries (valid ISO):", deck.length);
  console.log("Excel rate rows (sum):", excelTotal);
  console.log("DB telnyx total rows:", dbTotal);
  console.log("DB distinct countryIso:", dbByIso.size);
  console.log("");
  console.log("Countries FULLY MISSING (0 cards):", missingCountries.length);
  console.log("  → missing rows:", missingRowSum);
  console.log("Countries PARTIAL (db < excel):", shortCountries.length);
  console.log("  → missing rows:", shortRowSum);
  console.log("Countries OK (db >= excel):", okCountries.length);
  console.log("TOTAL missing rows (full+partial):", missingRowSum + shortRowSum);
  console.log("");
  console.log("--- Top fully missing (by excel rows) ---");
  console.log(
    missingCountries
      .slice(0, 40)
      .map((r) => `${r.iso} ${r.name}: excel=${r.excelRows}`)
      .join("\n")
  );
  if (missingCountries.length > 40) {
    console.log(`… +${missingCountries.length - 40} more`);
  }
  console.log("");
  console.log("--- Partial shortfalls ---");
  console.log(
    shortCountries
      .slice(0, 20)
      .map((r) => `${r.iso}: excel=${r.excelRows} db=${r.dbRows} missing=${r.missingRows}`)
      .join("\n") || "(none)"
  );

  const out = {
    excelCountries: deck.length,
    excelRows: excelTotal,
    dbRows: dbTotal,
    fullyMissingCountries: missingCountries.length,
    fullyMissingRows: missingRowSum,
    partialCountries: shortCountries.length,
    partialMissingRows: shortRowSum,
    totalMissingRows: missingRowSum + shortRowSum,
    missingCountries,
    shortCountries,
  };
  fs.writeFileSync(
    path.join(__dirname, "telnyx_deck_vs_db_missing.json"),
    JSON.stringify(out, null, 2)
  );
  console.log("\nWrote scripts/telnyx_deck_vs_db_missing.json");

  await client.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
