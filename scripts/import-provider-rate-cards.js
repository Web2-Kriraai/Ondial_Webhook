#!/usr/bin/env node
/**
 * Import provider rate cards from CSV (Twilio or Telnyx).
 * NEVER writes customer credits / CallLogs.
 *
 * Usage:
 *   node scripts/import-provider-rate-cards.js --provider=telnyx --file=./rates.csv [--dry-run]
 *
 * Expected columns (case-insensitive):
 *   ISO, Country, Origination Prefixes, Destination Prefixes, Description,
 *   Interval 1, Interval N, Rate (USD/min), Price Per Call
 *
 * Description containing "Mobile" → routeType mobile; else landline.
 */
const fs = require("fs");
const path = require("path");
const { MongoClient } = require("mongodb");

function parseArgs(argv) {
    const out = { dryRun: false, provider: null, file: null };
    for (const a of argv.slice(2)) {
        if (a === "--dry-run") out.dryRun = true;
        else if (a.startsWith("--provider=")) out.provider = a.slice("--provider=".length).toLowerCase();
        else if (a.startsWith("--file=")) out.file = a.slice("--file=".length);
    }
    return out;
}

function splitCsvLine(line) {
    const cells = [];
    let cur = "";
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (ch === '"') {
            if (inQuotes && line[i + 1] === '"') {
                cur += '"';
                i++;
            } else inQuotes = !inQuotes;
        } else if (ch === "," && !inQuotes) {
            cells.push(cur);
            cur = "";
        } else cur += ch;
    }
    cells.push(cur);
    return cells.map((c) => c.trim());
}

function normHeader(h) {
    return String(h || "")
        .trim()
        .toLowerCase()
        .replace(/\s+/g, " ");
}

function pick(row, aliases) {
    for (const a of aliases) {
        if (row[a] != null && String(row[a]).trim() !== "") return String(row[a]).trim();
    }
    return "";
}

async function main() {
    const args = parseArgs(process.argv);
    if (!args.provider || !["twilio", "telnyx"].includes(args.provider)) {
        console.error("Require --provider=twilio|telnyx");
        process.exit(1);
    }
    if (!args.file || !fs.existsSync(args.file)) {
        console.error("Require --file=<csv path>");
        process.exit(1);
    }

    const text = fs.readFileSync(path.resolve(args.file), "utf8");
    const lines = text.split(/\r?\n/).filter((l) => l.trim());
    if (lines.length < 2) {
        console.error("CSV empty");
        process.exit(1);
    }
    const headers = splitCsvLine(lines[0]).map(normHeader);
    const rows = [];
    for (let i = 1; i < lines.length; i++) {
        const cells = splitCsvLine(lines[i]);
        const obj = {};
        headers.forEach((h, idx) => {
            obj[h] = cells[idx] ?? "";
        });
        rows.push(obj);
    }

    const now = new Date();
    const docs = [];
    for (const row of rows) {
        const iso = pick(row, ["iso", "country iso", "countryiso"]).toUpperCase();
        const destRaw = pick(row, [
            "destination prefixes",
            "destination prefix",
            "destinationprefixes",
            "prefix",
        ]);
        const desc = pick(row, ["description", "desc"]);
        const rate = Number(
            pick(row, ["rate (usd/min)", "rate", "rate usd/min", "usd/min"]).replace(/[^0-9.]/g, "")
        );
        const perCall = Number(
            pick(row, ["price per call", "per call", "percallfee"]).replace(/[^0-9.]/g, "") || "0"
        );
        const i1 = Number(pick(row, ["interval 1", "interval1"]) || "60") || 60;
        const iN = Number(pick(row, ["interval n", "intervaln"]) || String(i1)) || i1;
        const prefixes = destRaw
            .split(/[|;,\s]+/)
            .map((p) => p.replace(/\D/g, ""))
            .filter(Boolean);
        if (!prefixes.length || !Number.isFinite(rate)) continue;
        const routeType = /mobile/i.test(desc) ? "mobile" : "landline";
        for (const destinationPrefix of prefixes) {
            docs.push({
                provider: args.provider,
                countryIso: iso || null,
                destinationPrefix,
                routeType,
                rateUsdPerMin: rate,
                interval1Sec: i1,
                intervalNSec: iN,
                perCallFeeUsd: Number.isFinite(perCall) ? perCall : 0,
                effectiveFrom: now,
                source: path.basename(args.file),
                importedAt: now,
                description: desc || null,
            });
        }
    }

    console.log(`Parsed ${docs.length} rate-card rows for ${args.provider}`);
    if (args.dryRun) {
        console.log(JSON.stringify(docs.slice(0, 5), null, 2));
        console.log("(dry-run — no Mongo writes)");
        return;
    }

    const uri = process.env.MONGODB_URI;
    if (!uri) {
        console.error("MONGODB_URI required unless --dry-run");
        process.exit(1);
    }
    const client = new MongoClient(uri);
    await client.connect();
    const db = client.db();
    let upserted = 0;
    for (const doc of docs) {
        await db.collection("provider_rate_cards").updateOne(
            {
                provider: doc.provider,
                destinationPrefix: doc.destinationPrefix,
                routeType: doc.routeType,
                effectiveFrom: doc.effectiveFrom,
            },
            { $set: doc },
            { upsert: true }
        );
        upserted++;
    }
    await client.close();
    console.log(`Upserted ${upserted} provider_rate_cards (no customer credit writes)`);
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
