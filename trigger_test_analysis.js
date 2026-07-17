// Set up environments before requiring db and triggerCallAnalysis
process.env.MONGODB_URI = "mongodb://ondial__db:nf5IdE3JP63tXBFZ@ac-nlfhhjp-shard-00-00.ak2rgbl.mongodb.net:27017,ac-nlfhhjp-shard-00-01.ak2rgbl.mongodb.net:27017,ac-nlfhhjp-shard-00-02.ak2rgbl.mongodb.net:27017/ondial_test?ssl=true&replicaSet=atlas-16ketm-shard-0&authSource=admin&appName=ondial";
process.env.REDIS_URL = "redis://127.0.0.1:6379";
process.env.ANALYSIS_API_URL = "https://sforeignscript.ondial.ai";
process.env.ANALYSIS_USE_BODY_PAYLOAD = "1";
process.env.ONDIAL_TRIGGER_ANALYSIS_ENABLED = "1";
process.env.ONDIAL_SKIP_CREDIT_TEST_CALLS = "0";

const { connectDB } = require("./db");
const { resolveAnalysisPayload } = require("./lib/triggerCallAnalysis");

async function main() {
    try {
        console.log("Connecting to MongoDB...");
        await connectDB();
        
        const callId = "129d2bc4-6a05-4462-b654-67ab40ded47c";
        console.log(`Resolving payload for Call ID: ${callId}...`);
        const { payload, reason, callLogDoc } = await resolveAnalysisPayload(callId);
        
        if (!payload) {
            console.error("Failed to resolve payload. Reason:", reason);
            return;
        }

        // Apply same V1 payload formatting as triggerCallAnalysis.js:
        const finalPayload = JSON.parse(JSON.stringify(payload));
        delete finalPayload.service_name;
        delete finalPayload.conversation;
        delete finalPayload.payload_generated_at;
        if (!finalPayload.reason_for_calling || !finalPayload.reason_for_calling.trim()) {
            finalPayload.reason_for_calling = "Analyze the call conversation.";
        }
        if (!finalPayload.classifications || !Array.isArray(finalPayload.classifications.items) || !finalPayload.classifications.items.length) {
            finalPayload.classifications = {
                items: [
                    { name: "Interested", description: "Customer showed interest.", next_action: "" },
                    { name: "Not Interested", description: "Customer was not interested.", next_action: "" }
                ]
            };
        }
        if (finalPayload.features_enabled) {
            if (finalPayload.features_enabled.is_followup_enabled === false) {
                delete finalPayload.features_enabled.email_followup;
                delete finalPayload.features_enabled.callback_scheduling;
            }
        }
        if (finalPayload.sub_service_id !== "win_back_campaigns") {
            delete finalPayload.agent;
        }

        const requestBody = { payload: finalPayload };
        console.log("=== REQUEST PAYLOAD ===");
        console.log(JSON.stringify(requestBody, null, 2));

        const analysisUrl = "https://sscript.ondial.ai/v1/analysis/call";
        console.log(`Sending POST request to ${analysisUrl}...`);

        const res = await fetch(analysisUrl, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "x-header-key": "1"
            },
            body: JSON.stringify(requestBody)
        });

        console.log("=== API RESPONSE STATUS ===");
        console.log("Status:", res.status, res.statusText);

        const responseText = await res.text();
        console.log("=== API RESPONSE BODY ===");
        console.log(responseText);

    } catch (err) {
        console.error("Test Error:", err);
    } finally {
        process.exit(0);
    }
}

main();
