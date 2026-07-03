/**
 * POST to ANALYSIS_API_URL for Twilio outbound calls only (conversation webhook).
 * India hangup does not trigger this — India analysis uses the provider pipeline.
 *
 * Always HTTP POST (never GET).
 *
 * Default (ANALYSIS_USE_BODY_PAYLOAD not set):
 *   POST /analyze/call/{callId} — empty body; callId = Twilio CallSid (wizard UUID ignored)
 *
 * ANALYSIS_USE_BODY_PAYLOAD=1:
 *   POST /analyze/call — JSON body from CallLogs
 */
const logger = require("../logger");
const { getDb } = require("../db");
const { getRedis } = require("../redis");
const { ObjectId } = require("mongodb");

const ANALYSIS_API_MAX_ATTEMPTS = Number(process.env.ANALYSIS_API_MAX_ATTEMPTS || 5);
const ANALYSIS_API_RETRY_MS = Number(process.env.ANALYSIS_API_RETRY_MS || 3000);
const ANALYSIS_API_INITIAL_DELAY_MS = Number(process.env.ANALYSIS_API_INITIAL_DELAY_MS || 2000);
const ANALYSIS_LOG_PAYLOAD = process.env.ANALYSIS_LOG_PAYLOAD === "1";
const ANALYSIS_USE_BODY_PAYLOAD = process.env.ANALYSIS_USE_BODY_PAYLOAD === "1";
const ANALYSIS_TRIGGER_LOCK_SEC = Number(process.env.ANALYSIS_TRIGGER_LOCK_SEC || 120);
const {
    isUuidCallId,
    isTwilioCallSid,
    resolveLegacyAnalysisCallId,
} = require("./resolveLegacyAnalysisCallId");
const { sanitizeAnalysisPayloadForV1Api } = require("./analysis_services/shared");
const ANALYSIS_DEFAULT_SERVICE_NAME = String(process.env.ANALYSIS_DEFAULT_SERVICE_NAME || "sales").trim();
const ANALYSIS_DEFAULT_REASON = String(
    process.env.ANALYSIS_DEFAULT_REASON || "Analyze the call conversation."
).trim();

/** Legacy: .../analyze/call/{callId}  |  Body mode: .../analyze/call (no id in path) */
function buildAnalysisUrl(callId, { bodyPayloadMode = false } = {}) {
    let raw = String(process.env.ANALYSIS_API_URL || "http://72.60.221.48:5000").replace(/\/$/, "");
    const isV1 = raw.includes("/v1") || raw.includes("sscript.ondial.ai");
    if (isV1 && raw.includes("/analyze/call")) {
        raw = raw.replace("/analyze/call", "/analysis/call");
    }
    const pathPart = isV1 ? "analysis/call" : "analyze/call";

    if (bodyPayloadMode) {
        if (/\/analysis\/call\/[^/]+$/i.test(raw)) {
            return raw.replace(/\/[^/]+$/, "");
        }
        if (/\/analyze\/call\/[^/]+$/i.test(raw)) {
            return raw.replace(/\/[^/]+$/, "");
        }
        if (raw.endsWith("/analysis/call") || raw.includes("/analysis/call")) {
            return raw.endsWith("/analysis/call") ? raw : `${raw.split("/analysis/call")[0]}/analysis/call`;
        }
        if (raw.endsWith("/analyze/call") || raw.includes("/analyze/call")) {
            return raw.endsWith("/analyze/call") ? raw : `${raw.split("/analyze/call")[0]}/analyze/call`;
        }
        return `${raw}/${pathPart}`;
    }

    const id = encodeURIComponent(String(callId));
    if (raw.includes("{CALL_ID}")) {
        return raw.replaceAll("{CALL_ID}", id);
    }
    if (/\/analysis\/call\/[^/]+$/i.test(raw)) {
        return raw;
    }
    if (/\/analyze\/call\/[^/]+$/i.test(raw)) {
        return raw;
    }
    if (raw.endsWith("/analysis/call")) {
        return `${raw}/${id}`;
    }
    if (raw.endsWith("/analyze/call")) {
        return `${raw}/${id}`;
    }
    if (raw.includes("/analysis/call")) {
        return `${raw}/${id}`;
    }
    if (raw.includes("/analyze/call")) {
        return `${raw}/${id}`;
    }
    return `${raw}/${pathPart}/${id}`;
}

function pickString(...values) {
    for (const val of values) {
        if (val != null && String(val).trim() !== "") {
            return String(val).trim();
        }
    }
    return "";
}

function safeStringify(payload) {
    try {
        return JSON.stringify(payload || {});
    } catch (err) {
        return JSON.stringify({ stringify_error: err.message });
    }
}

function detectCallProvider(inputCallId, callLogDoc) {
    if (isTwilioCallSid(inputCallId) || pickString(callLogDoc?.twilio?.call_sid)) {
        return "twilio";
    }
    if (isUuidCallId(inputCallId)) {
        return "india";
    }
    if (callLogDoc?.twilio?.call_sid) {
        return "twilio";
    }
    return "unknown";
}

function countTurns(callLogDoc) {
    const direct = Array.isArray(callLogDoc?.conversation?.turns) ? callLogDoc.conversation.turns.length : 0;
    const twilio = Array.isArray(callLogDoc?.twilio?.conversation?.turns)
        ? callLogDoc.twilio.conversation.turns.length
        : 0;
    return Math.max(direct, twilio);
}

function normalizeConversationTurns(callLogDoc) {
    const directTurns = Array.isArray(callLogDoc?.conversation?.turns) ? callLogDoc.conversation.turns : [];
    const twilioTurns = Array.isArray(callLogDoc?.twilio?.conversation?.turns)
        ? callLogDoc.twilio.conversation.turns
        : [];
    const sourceTurns = directTurns.length ? directTurns : twilioTurns;

    return sourceTurns
        .map((turn) => (turn && typeof turn === "object" ? turn : null))
        .filter(Boolean)
        .map((turn) => {
            const role = pickString(turn.role, turn.speaker);
            const text = pickString(turn.text, turn.message, turn.content);
            if (role && text) {
                const label = /^(ai|agent|assistant|bot)$/i.test(role) ? "Agent" : "User";
                return { [label]: text };
            }
            const entries = Object.entries(turn).filter(
                ([k, v]) => String(k || "").trim() !== "" && v != null && String(v).trim() !== ""
            );
            if (!entries.length) return null;
            const [speaker, message] = entries[0];
            return { [String(speaker).trim()]: String(message).trim() };
        })
        .filter(Boolean);
}

function pickRecruitmentConfig(campaignDoc = {}, callLogDoc = {}) {
    return (
        callLogDoc?.recruitment_config ||
        callLogDoc?.recruitmentConfig ||
        campaignDoc?.recruitment_config ||
        campaignDoc?.recruitmentConfig ||
        null
    );
}

function enrichAnalysisPayload(payload, campaign, callLog, contact) {
    if (!campaign) return payload;

    const serviceName = (
        campaign.service_name ||
        campaign.serviceName ||
        campaign.type ||
        "sales"
    ).toLowerCase();

    // Map wizard_service_id and sub_service_id
    let subServiceId = campaign.campaignServiceSubId || campaign.sub_service_id || campaign.subServiceId || "";
    let wizardServiceId = "";

    const SUB_SERVICE_TO_WIZARD = {
        "cold_outreach_prospecting": "sales",
        "lead_qualification": "sales",
        "appointment_demo_scheduling": "sales",
        "upsell_cross_sell_campaigns": "sales",
        "win_back_campaigns": "sales",
        "product_promotion_calls": "sales",
        "loan_origination": "finance",
        "emi_payment_reminders": "finance",
        "debt_loan_recovery": "finance",
        "credit_card_insurance_sales": "finance",
        "kyc_verification_call": "finance",
        "appointment_reminders": "notifications_alerts",
        "order_delivery_updates": "notifications_alerts",
        "event_booking_confirmations": "notifications_alerts",
        "emergency_critical_alerts": "notifications_alerts",
        "policy_subscription_renewals": "notifications_alerts",
        "compliance_deadlines": "notifications_alerts",
        "nps_csat_surveys": "survey_feedback",
        "post_purchase_feedback": "survey_feedback",
        "market_research_interviews": "survey_feedback",
        "product_feature_feedback": "survey_feedback",
        "brand_awareness_surveys": "survey_feedback",
        "healthcare_patient_surveys": "survey_feedback",
        "re_engagement_campaigns": "customer_retention",
        "loyalty_reward_notifications": "customer_retention",
        "churn_prevention_calls": "customer_retention",
        "check_in_care_calls": "customer_retention",
        "subscription_renewal_pushes": "customer_retention",
        "candidate_screening": "hr_recruitment",
        "interview_scheduling": "hr_recruitment",
        "job_offer_follow_ups": "hr_recruitment",
        "employee_satisfaction_surveys": "hr_recruitment",
        "onboarding_reminders": "hr_recruitment",
        "compliance_policy_updates": "hr_recruitment"
    };

    if (subServiceId && SUB_SERVICE_TO_WIZARD[subServiceId]) {
        wizardServiceId = SUB_SERVICE_TO_WIZARD[subServiceId];
    } else {
        wizardServiceId = campaign.wizard_service_id || campaign.wizardServiceId || "";
    }

    if (!wizardServiceId) {
        if (serviceName.includes("sales") || serviceName.includes("pipeline") || campaign.serviceCategoryId === "sales_pipeline") {
            wizardServiceId = "sales";
        } else if (serviceName.includes("finance") || campaign.serviceCategoryId === "finance_lending") {
            wizardServiceId = "finance";
        } else if (serviceName.includes("notification") || serviceName.includes("alert") || campaign.serviceCategoryId === "notifications_alerts") {
            wizardServiceId = "notifications_alerts";
        } else if (serviceName.includes("survey") || serviceName.includes("feedback") || campaign.serviceCategoryId === "survey_feedback") {
            wizardServiceId = "survey_feedback";
        } else if (serviceName.includes("retention") || campaign.serviceCategoryId === "customer_retention") {
            wizardServiceId = "customer_retention";
        } else if (serviceName.includes("hr") || serviceName.includes("recruitment") || campaign.serviceCategoryId === "hr_recruitment") {
            wizardServiceId = "hr_recruitment";
        }
    }

    if (!wizardServiceId) {
        wizardServiceId = "sales"; // fallback
    }

    try {
        const serviceModule = require(`./analysis_services/${wizardServiceId}`);
        return serviceModule.enrich(payload, campaign, callLog, contact, subServiceId);
    } catch (err) {
        logger.warn(`[Analysis] Service module not found for ${wizardServiceId}, fallback to base mapping`, { error: err.message });
        const { buildBaseEnrichedFields } = require("./analysis_services/shared");
        const base = buildBaseEnrichedFields(payload, campaign, callLog, contact);
        return {
            ...payload,
            ...base,
            wizard_service_id: wizardServiceId,
            sub_service_id: subServiceId
        };
    }
}

function buildAnalysisPayload({ callLogDoc, campaignDoc, contactDoc }) {
    const serviceName = pickString(
        callLogDoc?.service_name,
        callLogDoc?.serviceName,
        campaignDoc?.service_name,
        campaignDoc?.serviceName,
        campaignDoc?.type,
        ANALYSIS_DEFAULT_SERVICE_NAME
    );
    const reasonForCalling = pickString(
        callLogDoc?.reason_for_calling,
        callLogDoc?.reasonForCalling,
        campaignDoc?.reason_for_calling,
        campaignDoc?.reasonForCalling,
        campaignDoc?.call_reason,
        ANALYSIS_DEFAULT_REASON
    );

    const turns = normalizeConversationTurns(callLogDoc);
    if (!turns.length) return null;

    let payload = {
        service_name: serviceName,
        reason_for_calling: reasonForCalling,
        conversation: { turns },
    };

    const questions = callLogDoc?.questions || campaignDoc?.questions || callLogDoc?.custom_question || campaignDoc?.custom_question || callLogDoc?.custom_questions || campaignDoc?.custom_questions;
    if (Array.isArray(questions) && questions.length) {
        const mappedQuestions = questions
            .filter((q) => q && typeof q === "object" && pickString(q.question))
            .map((q) => ({
                question: pickString(q.question),
                instructions: pickString(q.instructions || q.expectedAnswer || "text"),
            }));
        payload.custom_question = mappedQuestions;
        payload.questions = mappedQuestions.map(q => ({
            question: q.question,
            expectedAnswer: q.instructions
        }));
    }

    const classifications = callLogDoc?.classifications || campaignDoc?.classifications;
    if (Array.isArray(classifications) && classifications.length) {
        const items = classifications
            .filter((c) => c && typeof c === "object" && (pickString(c.name) || pickString(c.question)))
            .map((c) => {
                const name = pickString(c.name || c.question);
                const description = pickString(c.description || c.instructions || "");
                const next_action = pickString(c.next_action || c.nextAction || "");
                return { name, description, next_action };
            });
        payload.classifications = { items };
    }

    if (String(serviceName).toLowerCase() === "recruitment") {
        const recruitmentConfig = pickRecruitmentConfig(campaignDoc, callLogDoc);
        if (recruitmentConfig && typeof recruitmentConfig === "object") {
            payload.recruitment_config = recruitmentConfig;
        }
    }

    payload = enrichAnalysisPayload(payload, campaignDoc, callLogDoc, contactDoc);

    return payload;
}

function buildCallLogLookupFilter(callId) {
    const id = String(callId || "").trim();
    const clauses = [{ lead_id: id }, { call_id: id }];
    if (id) {
        clauses.push({ lead_id: `twilio:${id}` }, { "twilio.call_sid": id });
    }
    return { $or: clauses };
}

async function loadCallLogDoc(callId) {
    const db = getDb();
    const collectionName = process.env.CALLLOGS_COLLECTION || "CallLogs";
    return db.collection(collectionName).findOne(buildCallLogLookupFilter(callId));
}

async function acquireAnalysisTriggerLock(callId) {
    try {
        const redis = getRedis();
        const key = `analysis:trigger:${callId}`;
        const ok = await redis.set(key, "1", "EX", ANALYSIS_TRIGGER_LOCK_SEC, "NX");
        return ok === "OK";
    } catch (err) {
        logger.warn("[Analysis] Redis lock unavailable — proceeding without dedupe", {
            callId,
            error: err.message,
        });
        return true;
    }
}

async function resolveAnalysisPayload(callId) {
    const callLogDoc = await loadCallLogDoc(callId);

    if (!callLogDoc) {
        return { payload: null, reason: "call_log_not_found", callLogDoc: null };
    }

    let campaignDoc = null;
    const campaignId = pickString(callLogDoc?.campaign_id);
    const db = getDb();
    if (campaignId && /^[a-f0-9]{24}$/i.test(campaignId)) {
        campaignDoc = await db.collection("campaigns").findOne({ _id: new ObjectId(campaignId) });
        if (campaignDoc && campaignDoc.followup === true && Array.isArray(campaignDoc.followupEmailTemplateIds) && campaignDoc.followupEmailTemplateIds.length > 0) {
            try {
                const templateIds = campaignDoc.followupEmailTemplateIds.map(id => typeof id === 'string' ? new ObjectId(id) : id);
                const templates = await db.collection('emailtemplates').find({ _id: { $in: templateIds } }).toArray();
                campaignDoc.emailTemplates = templates.map(t => ({
                    id: String(t._id),
                    title: t.name || t.subject || "",
                    description: t.description || t.name || t.subject || "",
                    subject: t.subject || "",
                    body: t.body || "",
                    variables: t.variables || {}
                }));
            } catch (eError) {
                logger.warn("[Analysis] Failed to populate emailTemplates for analysis payload", { error: eError.message });
            }
        }
        if (campaignDoc) {
            try {
                const companyRefId = campaignDoc.selectedCompanyId || campaignDoc.companyId;
                if (companyRefId) {
                    const company = await db.collection('companies').findOne({
                        _id: typeof companyRefId === 'string' ? new ObjectId(companyRefId) : companyRefId,
                    });
                    if (company) {
                        if (!campaignDoc.companyName && company.name) campaignDoc.companyName = company.name;
                        if (!campaignDoc.companyDescription && company.description) {
                            campaignDoc.companyDescription = company.description;
                        }
                        if (!campaignDoc.businessHours && company.businessHours) {
                            campaignDoc.businessHours = company.businessHours;
                        }
                        if (!campaignDoc.industry && company.industry) campaignDoc.industry = company.industry;
                    }
                }
            } catch (cErr) {
                logger.warn("[Analysis] Failed to populate company fields for analysis payload", { error: cErr.message });
            }
        }
    }

    let contactDoc = null;
    const contactId = callLogDoc?.contact_id;
    if (contactId && /^[a-f0-9]{24}$/i.test(String(contactId))) {
        contactDoc = await db.collection("contactprocessings").findOne({ _id: new ObjectId(String(contactId)) });
    }

    const payload = buildAnalysisPayload({ callLogDoc, campaignDoc: campaignDoc || {}, contactDoc });
    if (!payload) {
        return { payload: null, reason: "conversation_turns_missing", callLogDoc };
    }
    return { payload, reason: null, callLogDoc, campaignDoc, contactDoc };
}

function logPayloadSummary(callId, payload, mode) {
    logger.info("[Analysis] Request body summary", {
        callId,
        mode,
        service_name: payload?.service_name || null,
        turnCount: payload?.conversation?.turns?.length || 0,
        questionCount: Array.isArray(payload?.questions) ? payload.questions.length : 0,
        classificationCount: Array.isArray(payload?.classifications) ? payload.classifications.length : 0,
        hasRecruitmentConfig: Boolean(payload?.recruitment_config),
        companyName: payload?.company_info?.company_name || null,
        hasCompanyInfo: Boolean(payload?.company_info),
    });
    if (ANALYSIS_LOG_PAYLOAD) {
        logger.info("[Analysis] Request payload (full)", {
            callId,
            payload: safeStringify(payload),
        });
    }
}

async function triggerCallAnalysis(callId, options = {}) {
    const { deferIfNoTurns = false } = options;

    if (process.env.ONDIAL_TRIGGER_ANALYSIS_ENABLED === "0") {
        logger.info("[Analysis] Skipped — ONDIAL_TRIGGER_ANALYSIS_ENABLED=0", { callId });
        return { triggered: false, reason: "disabled" };
    }

    const id = String(callId || "").trim();
    if (!id || id.startsWith("call_")) {
        logger.warn("[Analysis] Skipped — invalid call_id", { callId: id || null });
        return { triggered: false, reason: "invalid_call_id" };
    }

    const callLogDocForProvider = await loadCallLogDoc(id);
    const provider = detectCallProvider(id, callLogDocForProvider);
    if (provider !== "twilio") {
        logger.info("[Analysis] Skipped — Twilio outbound only", {
            callId: id,
            callProvider: provider,
        });
        return { triggered: false, reason: "twilio_only" };
    }

    logger.info("[Analysis] Trigger started", {
        callId: id,
        httpMethod: "POST",
        envUseBodyPayload: ANALYSIS_USE_BODY_PAYLOAD,
        envLogPayload: ANALYSIS_LOG_PAYLOAD,
        analysisApiBase: String(process.env.ANALYSIS_API_URL || "").replace(/\/$/, "") || "(default)",
    });

    const lockKey = id;
    if (!(await acquireAnalysisTriggerLock(lockKey))) {
        logger.info("[Analysis] Trigger skipped — already in progress or recently done", { callId: id });
        return { triggered: false, reason: "analysis_trigger_locked" };
    }

    let analysisCallId = id;
    let analysisUrl = buildAnalysisUrl(id);
    let analysisMode = ANALYSIS_USE_BODY_PAYLOAD ? "body_payload_env" : "legacy_url_post";
    let callProvider = "unknown";

    try {
        const headers = {
            "Content-Type": "application/json"
        };
        if (
            String(process.env.NEXT_PUBLIC_CAMPAIGN_SERVICE_CATALOG_UI_SCRIPT || '').trim().toLowerCase() === 'true' ||
            String(process.env.CAMPAIGN_SERVICE_CATALOG_UI_SCRIPT || '').trim().toLowerCase() === 'true'
        ) {
            headers['x-header-key'] = '1';
        }

        const fetchOptions = {
            method: "POST",
            headers,
        };

        const applyBodyPayload = (payload, modeLabel) => {
            logPayloadSummary(id, payload, modeLabel);
            analysisUrl = buildAnalysisUrl(id, { bodyPayloadMode: true });
            const isV1 = analysisUrl.includes("/v1") || analysisUrl.includes("sscript.ondial.ai");
            let finalPayload = payload;
            if (isV1) {
                finalPayload = JSON.parse(JSON.stringify(payload));
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
                finalPayload = sanitizeAnalysisPayloadForV1Api(finalPayload);
            }
            const body = isV1 ? { payload: finalPayload } : finalPayload;
            fetchOptions.body = JSON.stringify(body);
            analysisMode = modeLabel;
        };

        let forceBodyPayload = false;
        const tempCallLog = await loadCallLogDoc(id);
        let tempCampaign = null;
        if (tempCallLog) {
            const campaignId = pickString(tempCallLog?.campaign_id);
            if (campaignId && /^[a-f0-9]{24}$/i.test(campaignId)) {
                const db = getDb();
                tempCampaign = await db.collection("campaigns").findOne({ _id: new ObjectId(campaignId) });
            }
        }
        const serviceName = pickString(
            tempCallLog?.service_name,
            tempCallLog?.serviceName,
            tempCampaign?.service_name,
            tempCampaign?.serviceName,
            tempCampaign?.type,
            "sales"
        ).toLowerCase();
        let wizardServiceId = tempCampaign?.wizard_service_id || tempCampaign?.wizardServiceId || "";
        if (!wizardServiceId) {
            if (serviceName.includes("sales") || serviceName.includes("pipeline") || tempCampaign?.serviceCategoryId === "sales_pipeline") {
                wizardServiceId = "sales";
            } else if (serviceName.includes("finance") || tempCampaign?.serviceCategoryId === "finance_lending") {
                wizardServiceId = "finance";
            } else if (serviceName.includes("notification") || serviceName.includes("alert") || tempCampaign?.serviceCategoryId === "notifications_alerts") {
                wizardServiceId = "notifications_alerts";
            } else if (serviceName.includes("survey") || serviceName.includes("feedback") || tempCampaign?.serviceCategoryId === "survey_feedback") {
                wizardServiceId = "survey_feedback";
            } else if (serviceName.includes("retention") || tempCampaign?.serviceCategoryId === "customer_retention") {
                wizardServiceId = "customer_retention";
            } else if (serviceName.includes("hr") || serviceName.includes("recruitment") || tempCampaign?.serviceCategoryId === "hr_recruitment") {
                wizardServiceId = "hr_recruitment";
            }
        }
        const isV1 = analysisUrl.includes("/v1") || analysisUrl.includes("sscript.ondial.ai");
        if (wizardServiceId || isV1) {
            forceBodyPayload = true;
        }

        let campaignDoc = tempCampaign;
        let contactDoc = null;

        if (ANALYSIS_USE_BODY_PAYLOAD || forceBodyPayload) {
            logger.info("[Analysis] Route: body payload (ANALYSIS_USE_BODY_PAYLOAD=1 or Sales/Notifications) — India + Twilio", {
                callId: id,
            });
            const { payload, reason, callLogDoc, campaignDoc: resolvedCampaign, contactDoc: resolvedContact } = await resolveAnalysisPayload(id);
            if (resolvedCampaign) campaignDoc = resolvedCampaign;
            if (resolvedContact) contactDoc = resolvedContact;
            callProvider = detectCallProvider(id, callLogDoc);
            if (!payload) {
                const turnCount = countTurns(callLogDoc);
                if (deferIfNoTurns && reason === "conversation_turns_missing") {
                    logger.info("[Analysis] Deferred until conversation.turns are stored", {
                        callId: id,
                        callProvider,
                        turnCount,
                    });
                    return { triggered: false, reason, deferred: true };
                }
                logger.warn("[Analysis] Skipping — payload unresolved", {
                    callId: id,
                    callProvider,
                    reason,
                    turnCount,
                });
                return { triggered: false, reason: reason || "payload_unresolved" };
            }
            applyBodyPayload(payload, forceBodyPayload ? "body_payload_forced" : "body_payload_env");
        } else {
            const callLogDoc = tempCallLog || await loadCallLogDoc(id);
            callProvider = detectCallProvider(id, callLogDoc);
            analysisCallId = resolveLegacyAnalysisCallId(callLogDoc, id);
            analysisUrl = buildAnalysisUrl(analysisCallId);
            analysisMode = "legacy_url_post";
            logger.info("[Analysis] Route: legacy POST (empty body, id in URL) — India + Twilio", {
                inputCallId: id,
                analysisCallId,
                callProvider,
                idType: isUuidCallId(analysisCallId)
                    ? "uuid"
                    : isTwilioCallSid(analysisCallId)
                      ? "twilio_call_sid"
                      : "other",
                analysisUrl,
            });
        }

        logger.info("[Analysis] Request plan", {
            callId: id,
            analysisCallId,
            callProvider,
            httpMethod: "POST",
            mode: analysisMode,
            hasBody: Boolean(fetchOptions.body),
            bodyBytes: fetchOptions.body ? Buffer.byteLength(fetchOptions.body, "utf8") : 0,
            analysisUrl,
        });

        if (ANALYSIS_API_INITIAL_DELAY_MS > 0) {
            await new Promise((r) => setTimeout(r, ANALYSIS_API_INITIAL_DELAY_MS));
        }

        for (let attempt = 1; attempt <= ANALYSIS_API_MAX_ATTEMPTS; attempt++) {
            logger.info("[Analysis] HTTP POST → analysis API", {
                callId: id,
                analysisCallId,
                callProvider,
                attempt,
                maxAttempts: ANALYSIS_API_MAX_ATTEMPTS,
                analysisUrl,
                mode: analysisMode,
                hasBody: Boolean(fetchOptions.body),
                requestBody: fetchOptions.body || "(empty body)"
            });

            const res = await fetch(analysisUrl, fetchOptions);

            if (res.ok) {
                const responseText = await res.text();
                logger.info("[Analysis] Analysis API OK", {
                    callId: id,
                    callProvider,
                    attempt,
                    httpStatus: res.status,
                    mode: analysisMode,
                    responseBody: responseText,
                });

                // Persist call analysis result to database
                try {
                    let analysisResult = null;
                    try {
                        analysisResult = JSON.parse(responseText);
                    } catch (parseErr) {
                        logger.warn("[Analysis] Response is not valid JSON", { callId: id, error: parseErr.message });
                    }

                    if (analysisResult) {
                        const db = getDb();
                        const analysisData = analysisResult.analysis || analysisResult;

                        let userIdObj = null;
                        if (campaignDoc?.createdBy && typeof campaignDoc.createdBy === 'string') {
                            const userDoc = await db.collection("users").findOne({ email: campaignDoc.createdBy });
                            if (userDoc) userIdObj = userDoc._id;
                        } else if (campaignDoc?.userId) {
                            userIdObj = campaignDoc.userId;
                        }

                        const updateObj = {
                            call_id: id,
                            internalCallId: id,
                            campaign_id: campaignDoc?._id || null,
                            campaign_name: campaignDoc?.campaignName || campaignDoc?.name || "",
                            contact_id: contactDoc?._id || null,
                            userId: userIdObj,
                            organizationId: campaignDoc?.organizationId || null,
                            service_name: serviceName,
                            analysis_data: analysisData,
                            analysisStatus: 'completed',
                            followUpProcessed: false,
                            followUpError: null,
                            updated_at: new Date()
                        };

                        await db.collection("call_analysis").updateOne(
                            { call_id: id },
                            {
                                $set: updateObj,
                                $setOnInsert: { created_at: new Date() }
                            },
                            { upsert: true }
                        );
                        logger.info("[Analysis] Persisted analysis to call_analysis collection", { callId: id });
                    }
                } catch (dbErr) {
                    logger.error("[Analysis] Failed to persist call analysis to DB", { callId: id, error: dbErr.message });
                }

                return { triggered: true, attempt, mode: analysisMode };
            }

            const errText = await res.text();
            const notReady =
                res.status === 404 ||
                res.status === 503 ||
                res.status === 502 ||
                /not\s*found/i.test(errText);

            if (notReady && attempt < ANALYSIS_API_MAX_ATTEMPTS) {
                logger.info("[Analysis] Retrying — API not ready", {
                    callId: id,
                    attempt,
                    httpStatus: res.status,
                    retryInMs: ANALYSIS_API_RETRY_MS,
                    responseBody: errText,
                });
                await new Promise((r) => setTimeout(r, ANALYSIS_API_RETRY_MS));
                continue;
            }

            logger.warn("[Analysis] Analysis API failed (final)", {
                callId: id,
                callProvider,
                attempt,
                httpStatus: res.status,
                mode: analysisMode,
                analysisUrl,
                responseBody: errText,
            });
            return { triggered: false, reason: "api_error", status: res.status, mode: analysisMode };
        }
    } catch (err) {
        logger.error("[Analysis] Analysis API error", {
            callId: id,
            callProvider,
            mode: analysisMode,
            error: err.message,
        });
        return { triggered: false, reason: err.message };
    }

    return { triggered: false, reason: "exhausted_retries" };
}

module.exports = { triggerCallAnalysis, resolveAnalysisPayload };
