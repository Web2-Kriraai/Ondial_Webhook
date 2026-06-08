/**
 * Inspect InboundConversation rows for a call (stub vs validate UI doc).
 *
 * Usage:
 *   node scripts/inspect-inbound-call.js <provider-call-id | call_sid>
 *   node scripts/inspect-inbound-call.js a619c62b-8514-4a9c-8445-2ba1c37aff59 --watch
 *   node scripts/inspect-inbound-call.js call_a619c62b85144a9c84452ba1c37aff59 --wait 45 --to 919484957189
 *
 * Run right after hangup (or during --watch) to see whether a validate row lands in Mongo.
 */
require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });
const { connectDB, getDb } = require("../db");
const {
    providerCallIdToCallSid,
    callSidToProviderCallId,
    findValidateCompanionDoc,
    isLikelyValidateUiDoc,
} = require("../lib/inboundDocAnchor");
const { resolveCampaignIdFromInboundDid } = require("../lib/resolveInboundBillingContext");
const { INBOUNDCALLLOG_COLLECTION } = require("../lib/inboundCall");

function parseArgs(argv) {
    const positional = [];
    let watch = false;
    let waitSec = 0;
    let toPhone = null;
    let fromPhone = null;

    for (let i = 2; i < argv.length; i++) {
        const arg = argv[i];
        if (arg === "--watch") {
            watch = true;
            waitSec = 45;
        } else if (arg === "--wait" && argv[i + 1]) {
            waitSec = Math.max(0, Number(argv[++i]) || 0);
            watch = waitSec > 0;
        } else if (arg === "--to" && argv[i + 1]) {
            toPhone = argv[++i];
        } else if (arg === "--from" && argv[i + 1]) {
            fromPhone = argv[++i];
        } else if (!arg.startsWith("-")) {
            positional.push(arg);
        }
    }

    return { callKey: positional[0] || null, watch, waitSec: waitSec || (watch ? 45 : 0), toPhone, fromPhone };
}

function resolveCallKeys(raw) {
    const input = String(raw || "").trim();
    if (!input) return { providerCallId: null, callSid: null };

    let callSid = providerCallIdToCallSid(input);
    let providerCallId = callSid ? callSidToProviderCallId(callSid) : null;

    if (!callSid && /^call_[a-f0-9]{32}$/i.test(input)) {
        callSid = input.toLowerCase();
        providerCallId = callSidToProviderCallId(callSid);
    }

    if (!providerCallId && /^[0-9a-f-]{36}$/i.test(input)) {
        providerCallId = input;
        callSid = providerCallIdToCallSid(input);
    }

    return { providerCallId, callSid };
}

function summarizeDoc(doc) {
    if (!doc) return null;
    return {
        _id: String(doc._id),
        call_id: doc.call_id || null,
        call_sid: doc.call_sid || null,
        source: doc.source || null,
        userId: doc.userId != null ? String(doc.userId) : null,
        to_number: doc.to_number || doc.phone_number || doc.to || null,
        from_number: doc.from_number || doc.from || null,
        config_id: doc.config_id != null ? String(doc.config_id) : null,
        creditsDeducted: doc.creditsDeducted ?? null,
        creditsDeductedAmount: doc.creditsDeductedAmount ?? null,
        recordingUrl: doc.recordingUrl ? "(set)" : null,
        duration: doc.duration ?? null,
        status: doc.status ?? null,
        events: doc.call_data?.events?.length || 0,
        updatedAt: doc.updatedAt ?? null,
        kind: isLikelyValidateUiDoc(doc) ? "validate-ui" : "webhook-stub",
    };
}

function printReport({ providerCallId, callSid, docs, validateDoc, campaignId, toPhone }) {
    const stubDocs = docs.filter((d) => !isLikelyValidateUiDoc(d));
    const uiDocs = docs.filter((d) => isLikelyValidateUiDoc(d));

    console.log("\n=== Inbound call inspect ===");
    console.log("provider_call_id:", providerCallId || "(none)");
    console.log("call_sid:", callSid || "(none)");
    console.log("collection:", INBOUNDCALLLOG_COLLECTION);
    console.log("campaign (DID lookup):", campaignId || "(unresolved)");
    if (toPhone) console.log("DID (--to):", toPhone);

    console.log("\n--- All docs by call_sid / call_id ---");
    if (!docs.length) {
        console.log("(none found)");
    } else {
        for (const row of docs.map(summarizeDoc)) {
            console.log(JSON.stringify(row, null, 2));
        }
    }

    console.log("\n--- Summary ---");
    console.log("total docs:", docs.length);
    console.log("webhook stub(s):", stubDocs.length);
    console.log("validate / UI row(s):", uiDocs.length);
    console.log(
        "findValidateCompanionDoc:",
        validateDoc ? summarizeDoc(validateDoc) : "(not found)"
    );

    const stub = stubDocs[0] || null;
    const validate = validateDoc || uiDocs[0] || null;

    if (stub?.creditsDeducted && !validate) {
        console.log(
            "\nVerdict: credits on webhook stub only — no validate row in this MongoDB yet."
        );
        console.log("If the UI still shows creditsDeducted:false, the UI doc is elsewhere or not written.");
    } else if (validate?.creditsDeducted) {
        console.log("\nVerdict: validate/UI row has creditsDeducted:true — sync OK.");
    } else if (validate && !validate.creditsDeducted) {
        console.log("\nVerdict: validate row exists but creditsDeducted is still false — sync missed.");
    } else if (!stub && !validate) {
        console.log("\nVerdict: no InboundConversation docs for this call.");
    } else if (stub?.creditsDeducted && validate && !validate.creditsDeducted) {
        console.log("\nVerdict: billing on stub; validate row present but not synced.");
    }

    return { validateFound: Boolean(validateDoc || uiDocs.length) };
}

async function loadDocs(db, { providerCallId, callSid }) {
    const or = [];
    if (providerCallId) or.push({ call_id: providerCallId });
    if (callSid) or.push({ call_sid: callSid });
    if (!or.length) return [];
    return db.collection(INBOUNDCALLLOG_COLLECTION).find({ $or: or }).toArray();
}

function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
}

async function inspectOnce(db, keys, toPhone, fromPhone) {
    const docs = await loadDocs(db, keys);
    const toFromDoc = docs.find((d) => d.to_number || d.phone_number)?.to_number
        || docs.find((d) => d.to_number || d.phone_number)?.phone_number
        || null;
    const effectiveTo = toPhone || toFromDoc;
    const effectiveFrom =
        fromPhone || docs.find((d) => d.from_number)?.from_number || null;

    const validateDoc = await findValidateCompanionDoc(db, {
        callSid: keys.callSid,
        toPhone: effectiveTo,
        fromPhone: effectiveFrom,
    });

    let campaignId = null;
    if (effectiveTo) {
        campaignId = await resolveCampaignIdFromInboundDid(effectiveTo);
    }

    return printReport({
        providerCallId: keys.providerCallId,
        callSid: keys.callSid,
        docs,
        validateDoc,
        campaignId,
        toPhone: effectiveTo,
    });
}

async function main() {
    const { callKey, watch, waitSec, toPhone, fromPhone } = parseArgs(process.argv);

    if (!callKey) {
        console.error(
            "Usage: node scripts/inspect-inbound-call.js <provider-call-id | call_sid> [--watch] [--wait 45] [--to DID] [--from caller]"
        );
        process.exit(1);
    }

    const keys = resolveCallKeys(callKey);
    if (!keys.providerCallId && !keys.callSid) {
        console.error("Could not parse call id:", callKey);
        process.exit(1);
    }

    await connectDB();
    const db = getDb();

    if (!watch) {
        await inspectOnce(db, keys, toPhone, fromPhone);
        return;
    }

    const intervalMs = 3000;
    const deadline = Date.now() + waitSec * 1000;
    let attempt = 0;
    let validateFound = false;

    console.log(`Watching up to ${waitSec}s for validate row (every ${intervalMs / 1000}s)...`);

    while (Date.now() <= deadline) {
        attempt++;
        console.log(`\n[attempt ${attempt} @ ${new Date().toISOString()}]`);
        const result = await inspectOnce(db, keys, toPhone, fromPhone);
        if (result.validateFound) {
            validateFound = true;
            console.log("\nValidate row detected — stopping watch.");
            break;
        }
        if (Date.now() + intervalMs > deadline) break;
        await sleep(intervalMs);
    }

    if (!validateFound) {
        console.log(`\nNo validate row appeared within ${waitSec}s in ${INBOUNDCALLLOG_COLLECTION}.`);
        console.log("Check that Ondial app writes validate docs to the same MONGODB_URI as this webhook.");
    }
}

main().catch((err) => {
    console.error(err.message);
    process.exit(1);
});
