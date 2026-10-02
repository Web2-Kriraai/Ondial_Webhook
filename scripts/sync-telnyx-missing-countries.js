/**
 * Review Telnyx Country Summary vs Mongo and add MISSING countries
 * into countryPricingV1 (providers.telnyx overlay = max cost × commission).
 *
 * Usage:
 *   node scripts/sync-telnyx-missing-countries.js           # dry-run
 *   node scripts/sync-telnyx-missing-countries.js --apply   # write DB
 *
 * Reads: scripts/telnyx_country_summary.json (from Excel Country Summary)
 * Env: MONGODB_URI from Ondial_Webhook/.env or parent Ondial/.env
 */
const fs = require("fs");
const path = require("path");
const { MongoClient } = require("mongodb");

function loadEnvFiles() {
    const candidates = [
        path.join(__dirname, "..", ".env"),
        path.join(__dirname, "..", "..", "Ondial", ".env"),
        path.join("C:", "Users", "ADMIN", "Documents", "GitHub", "Ondial", ".env"),
    ];
    for (const file of candidates) {
        if (!fs.existsSync(file)) continue;
        const text = fs.readFileSync(file, "utf8");
        for (const line of text.split(/\r?\n/)) {
            const t = line.trim();
            if (!t || t.startsWith("#")) continue;
            const eq = t.indexOf("=");
            if (eq < 1) continue;
            const key = t.slice(0, eq).trim();
            let val = t.slice(eq + 1).trim();
            if (
                (val.startsWith('"') && val.endsWith('"')) ||
                (val.startsWith("'") && val.endsWith("'"))
            ) {
                val = val.slice(1, -1);
            }
            if (process.env[key] == null || process.env[key] === "") {
                process.env[key] = val;
            }
        }
    }
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

function isValidIso(iso) {
    return /^[A-Z]{2}$/.test(iso) && iso !== "XX" && iso !== "ZZ";
}

async function main() {
    loadEnvFiles();
    const apply = process.argv.includes("--apply");
    const uri = process.env.MONGODB_URI;
    if (!uri) {
        console.error("MONGODB_URI missing");
        process.exit(1);
    }

    const summaryPath = path.join(__dirname, "telnyx_country_summary.json");
    if (!fs.existsSync(summaryPath)) {
        console.error("Missing", summaryPath, "— export Country Summary first");
        process.exit(1);
    }
    const deck = JSON.parse(fs.readFileSync(summaryPath, "utf8")).filter((r) =>
        isValidIso(String(r.iso || "").toUpperCase())
    );
    const deckByIso = new Map(deck.map((r) => [String(r.iso).toUpperCase(), r]));

    const client = new MongoClient(uri);
    await client.connect();
    const db = client.db();

    const pricingDoc = await db.collection("systemsettings").findOne({ key: "countryPricingV1" });
    const commissionDoc = await db
        .collection("systemsettings")
        .findOne({ key: "providerCommissionV1" });
    const commissionPct =
        Number(commissionDoc?.value?.byProvider?.telnyx) ||
        Number(commissionDoc?.value?.defaultPercent) ||
        40;

    const existingCountries =
        pricingDoc?.value?.countries && typeof pricingDoc.value.countries === "object"
            ? { ...pricingDoc.value.countries }
            : {};
    const existingIsos = new Set(Object.keys(existingCountries).map((k) => k.toUpperCase()));

    const cardIsos = await db.collection("provider_rate_cards").distinct("countryIso", {
        provider: "telnyx",
        countryIso: { $type: "string" },
    });
    const cardIsoSet = new Set(
        (cardIsos || []).map((c) => String(c).toUpperCase()).filter(isValidIso)
    );

    const SKIP = new Set(["AN"]); // deprecated Netherlands Antilles in some decks

    const missingInMatrix = [];
    const presentInMatrix = [];
    const deckButNoCards = [];

    for (const [iso, row] of deckByIso) {
        if (SKIP.has(iso)) continue;
        if (!existingIsos.has(iso)) missingInMatrix.push(row);
        else presentInMatrix.push(iso);
        if (!cardIsoSet.has(iso)) deckButNoCards.push(iso);
    }

    console.log("=== Telnyx Country Summary vs DB ===");
    console.log("Deck valid ISOs:", deckByIso.size);
    console.log("countryPricingV1 countries:", existingIsos.size);
    console.log("provider_rate_cards telnyx ISOs:", cardIsoSet.size);
    console.log("Commission telnyx %:", commissionPct);
    console.log("Already in matrix:", presentInMatrix.length);
    console.log("MISSING in matrix (will add):", missingInMatrix.length);
    console.log(
        "Deck ISO with no rate cards:",
        deckButNoCards.length,
        deckButNoCards.slice(0, 30).join(",") + (deckButNoCards.length > 30 ? "…" : "")
    );

    const preview = missingInMatrix.slice(0, 15).map((r) => {
        const sell = round6(Number(r.max) * (1 + commissionPct / 100));
        return { iso: r.iso, name: r.name, costMax: r.max, suggestedSell: sell };
    });
    console.log("Sample adds:", JSON.stringify(preview, null, 2));

    if (!apply) {
        console.log("\nDry-run only. Re-run with --apply to write countryPricingV1.");
        await client.close();
        return;
    }

    let added = 0;
    for (const row of missingInMatrix) {
        const iso = String(row.iso).toUpperCase();
        const baseSell = round6(Number(row.max) * (1 + commissionPct / 100));
        if (!Number.isFinite(baseSell) || baseSell < 0) continue;
        existingCountries[iso] = {
            salePriceEnabled: false,
            packages: buildSuggestedPackages(baseSell),
            concurrentCallCost: 7,
            phoneNumberCost: 7,
            providers: {
                telnyx: {
                    salePriceEnabled: false,
                    packages: buildSuggestedPackages(baseSell),
                },
            },
        };
        added++;
    }

    const nextValue = {
        ...(pricingDoc?.value && typeof pricingDoc.value === "object" ? pricingDoc.value : {}),
        enabled: true,
        countries: existingCountries,
        fallbackOrder:
            Array.isArray(pricingDoc?.value?.fallbackOrder) && pricingDoc.value.fallbackOrder.length
                ? pricingDoc.value.fallbackOrder
                : ["US", "IN"],
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

    await db.collection("adminauditlogs").insertOne({
        action: "country_pricing.sync_telnyx_missing",
        resource: "countryPricingV1",
        adminEmail: "script:sync-telnyx-missing-countries",
        before: { countryCount: existingIsos.size },
        after: {
            countryCount: Object.keys(existingCountries).length,
            added,
            commissionPct,
            source: "Telnyx_Rate_Deck_Summary.xlsx Country Summary",
        },
        createdAt: new Date(),
        updatedAt: new Date(),
    }).catch(() => {});

    console.log(`\nApplied: added ${added} countries into countryPricingV1.`);
    console.log("Total countries now:", Object.keys(existingCountries).length);
    await client.close();
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
