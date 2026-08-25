const { ObjectId } = require("mongodb");
const {
    buildWhatsappSampleValues,
    mapWhatsappTemplateDocForAnalysis,
    resolveWhatsappTemplateDescription,
} = require("./whatsappSampleValues");

function getCleanString(val) {
    return val != null ? String(val).trim() : "";
}

/** Same shape as Calling_system1 shared-lib toCallbackSchedulingPayload. */
function toCallbackSchedulingPayload(campaign = {}) {
    const block = campaign.callbackScheduling || campaign.callback_scheduling || {};
    let minDays = Math.floor(Number(block.minDays ?? block.min_days ?? campaign.callbackMinDays ?? 2));
    let maxDays = Math.floor(Number(block.maxDays ?? block.max_days ?? campaign.callbackMaxDays ?? 5));
    if (!Number.isFinite(minDays) || minDays < 0) minDays = 2;
    if (!Number.isFinite(maxDays) || maxDays < 1) maxDays = 5;
    if (minDays > maxDays) maxDays = minDays;
    const channels = Array.isArray(campaign.followupChannels) ? campaign.followupChannels : ["call"];
    const status = block.status !== false
        && campaign.followup !== false
        && channels.includes("call");
    if (!status) {
        return { status: false };
    }
    return {
        status: true,
        min_days: minDays,
        max_days: maxDays,
    };
}

function normalizeAnalysisSpeakerLabel(raw) {
    const s = String(raw || "").trim();
    if (!s) return null;
    if (/^(ai|agent|assistant|bot)$/i.test(s)) return "AI";
    if (/^(user|customer|human|caller)$/i.test(s)) return "User";
    return s;
}

function normalizeConversationTurns(callLogDoc) {
    const directTurns = Array.isArray(callLogDoc?.conversation?.turns) ? callLogDoc.conversation.turns : [];
    const twilioTurns = Array.isArray(callLogDoc?.twilio?.conversation?.turns)
        ? callLogDoc.twilio.conversation.turns
        : [];
    const telnyxTurns = Array.isArray(callLogDoc?.telnyx?.conversation?.turns)
        ? callLogDoc.telnyx.conversation.turns
        : [];
    const sourceTurns = directTurns.length
        ? directTurns
        : twilioTurns.length
          ? twilioTurns
          : telnyxTurns;

    return sourceTurns
        .map((turn) => (turn && typeof turn === "object" ? turn : null))
        .filter(Boolean)
        .map((turn) => {
            const role = turn.role || turn.speaker || "";
            const text = turn.text || turn.message || turn.content || "";
            if (role && text) {
                const label = /^(ai|agent|assistant|bot)$/i.test(String(role)) ? "AI" : "User";
                return { [label]: String(text).trim() };
            }
            const entries = Object.entries(turn).filter(
                ([k, v]) => String(k || "").trim() !== "" && v != null && String(v).trim() !== ""
            );
            if (!entries.length) return null;
            const [speaker, message] = entries[0];
            const label = normalizeAnalysisSpeakerLabel(speaker);
            const mapped =
                label === "AI" || label === "User"
                    ? label
                    : /^(ai|agent|assistant|bot)$/i.test(String(speaker))
                      ? "AI"
                      : "User";
            return { [mapped]: String(message).trim() };
        })
        .filter(Boolean);
}

function formatConversationText(turns) {
    return turns.map(t => {
        const role = Object.keys(t)[0];
        const text = t[role];
        const label = normalizeAnalysisSpeakerLabel(role) || role;
        return `${label}: ${text}`;
    }).join(", ");
}

function conversationTextHasAiOrUserTurn(conversationText) {
    const text = String(conversationText || "").trim();
    if (!text) return false;
    return /(?:^|,\s*)(?:AI|User)\s*:\s*\S+/i.test(text);
}

function buildConversationTextForAnalysis(callLogDoc) {
    const turns = normalizeConversationTurns(callLogDoc || {});
    let text = formatConversationText(turns);
    if (conversationTextHasAiOrUserTurn(text)) return text;

    const transcript = String(
        callLogDoc?.conversation?.transcript ||
            callLogDoc?.transcript ||
            callLogDoc?.conversation_text ||
            ""
    ).trim();
    if (!transcript) return text;

    return transcript
        .replace(/\bAgent\s*:/gi, "AI:")
        .replace(/\bAssistant\s*:/gi, "AI:")
        .replace(/\bBot\s*:/gi, "AI:")
        .replace(/\bCustomer\s*:/gi, "User:")
        .replace(/\bCaller\s*:/gi, "User:");
}

const WEEKDAYS = [
    'monday',
    'tuesday',
    'wednesday',
    'thursday',
    'friday',
    'saturday',
    'sunday',
];

function formatCampaignBusinessHours(businessHours) {
    if (typeof businessHours === 'string') return businessHours.trim();
    const bh = businessHours && typeof businessHours === 'object' ? businessHours : null;
    if (!bh) return '';

    return WEEKDAYS.map((day) => {
        const row = bh[day];
        if (!row || row.closed) {
            return `${day}: Closed`;
        }
        const open = String(row.open || '').trim();
        const close = String(row.close || '').trim();
        if (!open && !close) return `${day}: Closed`;
        return `${day}: ${open || '?'}-${close || '?'}`;
    }).join('; ');
}

const CONTACT_NAME_KEYS = [
    'name',
    'full name',
    'full_name',
    'firstname',
    'first name',
    'customer_name',
    'customer name',
    'contact_name',
    'contact name',
];
const CONTACT_EMAIL_KEYS = ['email', 'emailaddress', 'e-mail', 'email address', 'mail'];
const CONTACT_MOBILE_KEYS = [
    'mobile',
    'mobile number',
    'mobilenumber',
    'phone',
    'phone number',
    'phonenumber',
    'contact_mobile',
    'contact mobile',
    'cell',
    'tel',
];

function firstNonEmptyString(...values) {
    for (const v of values) {
        if (v == null) continue;
        const s = String(v).trim();
        if (s) return s;
    }
    return '';
}

function pickFromContactData(contactData, keyCandidates) {
    if (!contactData || typeof contactData !== 'object') return '';
    for (const key of keyCandidates) {
        if (contactData[key] != null) {
            const s = String(contactData[key]).trim();
            if (s) return s;
        }
    }
    const normalizedKeyMap = new Map();
    for (const [rawKey, value] of Object.entries(contactData)) {
        const nk = String(rawKey || '').trim().toLowerCase();
        if (!nk || normalizedKeyMap.has(nk)) continue;
        normalizedKeyMap.set(nk, value);
    }
    for (const key of keyCandidates) {
        const v = normalizedKeyMap.get(String(key).toLowerCase());
        if (v == null) continue;
        const s = String(v).trim();
        if (s) return s;
    }
    return '';
}

function buildUserContactInfoForAnalysis(contact, callLog) {
    const contactData =
        contact?.contactData && typeof contact.contactData === 'object' ? contact.contactData : {};

    return {
        contact_name: firstNonEmptyString(
            contact?.contactName,
            contact?.name,
            callLog?.contactName,
            pickFromContactData(contactData, CONTACT_NAME_KEYS)
        ),
        contact_mobile: firstNonEmptyString(
            contact?.mobileNumber,
            contact?.phone,
            contact?.mobile,
            callLog?.mobileNumber,
            callLog?.phone,
            pickFromContactData(contactData, CONTACT_MOBILE_KEYS)
        ),
        contact_email: firstNonEmptyString(
            contact?.email,
            contact?.contactEmail,
            pickFromContactData(contactData, CONTACT_EMAIL_KEYS)
        ),
    };
}

function buildCompanyInfoForAnalysis(campaign, companyDoc = null) {
    const companyName =
        campaign?.companyName ||
        campaign?.selectedCompany?.name ||
        companyDoc?.name ||
        '';
    const companyDescription =
        campaign?.companyDescription ||
        companyDoc?.description ||
        '';
    const businessHoursRaw =
        (campaign?.businessHours && typeof campaign.businessHours === 'object'
            ? campaign.businessHours
            : null) ||
        (companyDoc?.businessHours && typeof companyDoc.businessHours === 'object'
            ? companyDoc.businessHours
            : null);
    const businessHoursString =
        typeof campaign?.businessHours === 'string'
            ? campaign.businessHours.trim()
            : typeof companyDoc?.businessHours === 'string'
              ? companyDoc.businessHours.trim()
              : formatCampaignBusinessHours(businessHoursRaw);

    return {
        company_name: String(companyName || '').trim(),
        company_description: String(companyDescription || '').trim(),
        business_hours: businessHoursString,
    };
}

function resolveDemoBookingTypeLabel(rawType) {
    const raw = String(rawType || "").trim();
    if (raw === "site_visit") return "Site Visit Appointment";
    if (raw === "demo_call") return "Demo Call Appointment";
    return raw;
}

/** Call analysis API: features_enabled.demo_booking */
function buildDemoBookingForAnalysis(campaign) {
    const demoBookingStatus =
        campaign?.appointmentsDemosEnabled === true
        || campaign?.demo_booking?.status === true;

    if (!demoBookingStatus) {
        return { status: false };
    }

    const rawDemoType =
        campaign?.appointmentsDemosType
        || campaign?.demo_booking?.type
        || "demo_call";

    return {
        status: true,
        type: resolveDemoBookingTypeLabel(rawDemoType) || "Demo Call Appointment",
        booking_condition: String(
            campaign?.appointmentsDemosBookingCondition
            || campaign?.demo_booking?.booking_condition
            || ""
        ).trim(),
    };
}

function buildBaseEnrichedFields(payload, campaign, callLog, contact, companyDoc = null) {
    const conversationText = buildConversationTextForAnalysis(callLog || {});

    let classifications = payload?.classifications;
    if (!classifications || !Array.isArray(classifications.items) || !classifications.items.length) {
        // classifications stored under salesSettings.items for ALL wizard service types
        const classificationsList =
            campaign?.classifications ||
            campaign?.salesSettings?.items ||
            callLog?.classifications ||
            [];
        classifications = {
            items: classificationsList.map(c => ({
                name: c.name || c.category || c.question || "",
                description: c.description || "",
                next_action: c.next_action || c.nextAction || ""
            }))
        };
    }

    let customQuestion = payload?.custom_question;
    if (!customQuestion || !Array.isArray(customQuestion) || !customQuestion.length) {
        // questions stored under service-specific *QuestionSettings.questions based on wizard_service_id:
        //   sales              → salesQuestionSettings
        //   finance            → loanQuestionSettings
        //   notifications_alerts / customer_retention → notificationQuestionSettings
        //   survey_feedback    → surveyQuestionSettings
        //   hr_recruitment     → recruitmentQuestionSettings
        const questionsList =
            campaign?.questions ||
            campaign?.salesQuestionSettings?.questions ||
            campaign?.loanQuestionSettings?.questions ||
            campaign?.notificationQuestionSettings?.questions ||
            campaign?.surveyQuestionSettings?.questions ||
            campaign?.recruitmentQuestionSettings?.questions ||
            campaign?.feedbackQuestionSettings?.questions ||
            campaign?.leadQuestionSettings?.questions ||
            campaign?.quoteQuestionSettings?.questions ||
            campaign?.appointmentQuestionSettings?.questions ||
            campaign?.brandingQuestionSettings?.questions ||
            callLog?.questions ||
            callLog?.custom_question ||
            callLog?.custom_questions ||
            [];
        customQuestion = questionsList.map(q => ({
            question: q.question || "",
            instructions: q.instructions || q.expectedAnswer || q.expectedAnswerType || ""
        }));
    }

    const now = new Date();
    const offsetMs = 5.5 * 60 * 60 * 1000;
    const current_time_ist = new Date(now.getTime() + offsetMs).toISOString().replace("Z", "+05:30");

    const user_contact_info = buildUserContactInfoForAnalysis(contact, callLog);

    const isFollowup = campaign?.followup === true;
    const hasEmailChannel = campaignHasEmailFollowupEnabled(campaign);
    const hasWhatsappChannel = campaignHasWhatsappFollowupEnabled(campaign);
    const emailStatus = isFollowup && hasEmailChannel;
    const whatsappStatus = isFollowup && hasWhatsappChannel;

    const features_enabled = {
        is_followup_enabled: isFollowup,
        demo_booking: buildDemoBookingForAnalysis(campaign),
        email_followup: (() => {
            if (!emailStatus) return { status: false };
            
            const contactName = user_contact_info.contact_name || "Customer";
            const agentName = campaign?.agent?.name || campaign?.agentName || "AI Assistant";
            const companyName = campaign?.companyName || campaign?.selectedCompany?.name || campaign?.agent?.company || "";

            const injectVars = (text) => {
                if (!text) return "";
                return String(text).replace(/\{\{([\w_]+)(?::"([^"]*)")?\}\}/g, (match, key) => {
                    const k = String(key || '').trim().toLowerCase();
                    if (k === 'contact_name' && contactName) {
                        return `{{contact_name:"${contactName.replace(/"/g, '\\"')}"}}`;
                    }
                    if (k === 'company_name' && companyName) {
                        return `{{company_name:"${companyName.replace(/"/g, '\\"')}"}}`;
                    }
                    if (k === 'agent_name' && agentName) {
                        return `{{agent_name:"${agentName.replace(/"/g, '\\"')}"}}`;
                    }
                    return match;
                });
            };

            const templates = (campaign?.emailTemplates || []).map(t => ({
                id: t.id || String(t._id || ""),
                title: t.title || t.name || t.subject || "",
                description: t.description || t.title || t.name || "",
                subject: injectVars(t.subject || ""),
                body: injectVars(t.body || "")
            }));
            if (templates.length === 0) return { status: false };
            return { status: true, templates };
        })(),
        whatsapp_followup: (() => {
            if (!whatsappStatus) return { status: false };

            const sampleContext = {
                contact,
                campaign,
                contactName: user_contact_info.contact_name,
                companyName:
                    campaign?.companyName ||
                    campaign?.selectedCompany?.name ||
                    companyDoc?.name ||
                    "",
                agentName: campaign?.agent?.name || campaign?.agentName || "",
            };

            const templates = (campaign?.whatsappTemplates || []).map((t) => {
                const title = String(t.title || t.name || t.templateName || "").trim();
                const campaignName = String(
                    t.aisensyCampaignName || t.campaignName || t.campaign_name || t.templateName || title || ""
                ).trim();
                return {
                    id: t.id || String(t._id || ""),
                    title: title || campaignName,
                    description: resolveWhatsappTemplateDescription(t),
                    // Analysis API requires non-empty campaign_name (Meta has no AiSensy campaign — use template name)
                    campaign_name: campaignName || title || "whatsapp_template",
                    sample_values: buildWhatsappSampleValues(t, sampleContext),
                };
            });
            if (templates.length === 0) return { status: false };
            return { status: true, templates };
        })(),
        callback_scheduling: toCallbackSchedulingPayload(campaign)
    };

    const agent = {
        name: campaign?.agent?.name || campaign?.agentName || "AI Assistant",
        tone: campaign?.agent?.tone || campaign?.agentTone || "Standard",
        primary_language: campaign?.agent?.primary_language || campaign?.agent?.primaryLanguage || campaign?.agentLanguage || "en-IN"
    };

    const company_info = buildCompanyInfoForAnalysis(campaign, companyDoc);

    return {
        reason_for_calling: campaign?.reason_for_calling || campaign?.reasonForCalling || campaign?.call_reason || payload.reason_for_calling || "",
        conversation_text: conversationText,
        classifications,
        custom_question: customQuestion,
        current_time_ist,
        payload_generated_at: current_time_ist,
        user_contact_info,
        features_enabled,
        agent,
        company_info,
    };
}

function getPrimaryFollowupEmailTemplateId(campaign) {
    const arr = campaign?.followupEmailTemplateIds;
    if (Array.isArray(arr) && arr.length > 0) {
        const first = arr[0];
        if (first != null && String(first).trim() !== "") {
            return String(first._id ?? first).trim();
        }
    }
    const legacy = campaign?.followupEmailTemplateId;
    if (legacy != null && String(legacy).trim() !== "") {
        return String(legacy._id ?? legacy).trim();
    }
    return null;
}

function getPrimaryFollowupWhatsappTemplateId(campaign) {
    const arr = campaign?.followupWhatsappTemplateIds;
    if (Array.isArray(arr) && arr.length > 0) {
        const first = arr[0];
        if (first != null && String(first).trim() !== "") {
            return String(first._id ?? first).trim();
        }
    }
    return null;
}

function hasFollowupEmailRefs(source) {
    return (
        Boolean(String(source?.followupEmailConfigId || "").trim()) ||
        Boolean(getPrimaryFollowupEmailTemplateId(source))
    );
}

function hasFollowupWhatsappRefs(source) {
    return (
        Boolean(String(source?.followupWhatsappProfileId || "").trim()) ||
        Boolean(getPrimaryFollowupWhatsappTemplateId(source))
    );
}

function campaignHasEmailFollowupEnabled(campaign) {
    if (campaign?.followup !== true) return false;
    const channels = Array.isArray(campaign?.followupChannels) ? campaign.followupChannels : [];
    if (channels.includes("email")) return true;
    return hasFollowupEmailRefs(campaign);
}

function campaignHasWhatsappFollowupEnabled(campaign) {
    if (campaign?.followup !== true) return false;
    const channels = Array.isArray(campaign?.followupChannels) ? campaign.followupChannels : [];
    if (channels.includes("whatsapp")) return true;
    return hasFollowupWhatsappRefs(campaign);
}

function stripDisabledFeatureBlock(block) {
    if (!block || typeof block !== "object") return block;
    if (block.status === false) return { status: false };
    return block;
}

function normalizeWinBackConfigForV1Api(block) {
    if (!block || typeof block !== "object") {
        return { status: false };
    }
    if (block.status === false) {
        return { status: false };
    }
    const offerPresented = String(block.offer_presented ?? block.offerPresented ?? "").trim();
    const offerValidity = String(block.offer_validity ?? block.offerValidity ?? "").trim();
    if (!offerPresented || !offerValidity) {
        return { status: false };
    }
    const reactivationCondition = String(
        block.reactivation_condition ?? block.reactivationCondition ?? ""
    ).trim();
    const out = {
        status: true,
        offer_presented: offerPresented,
        offer_validity: offerValidity,
    };
    if (reactivationCondition) {
        out.reactivation_condition = reactivationCondition;
    }
    return out;
}

/** When status is false, analysis API rejects any sibling keys (promotion_config, etc.). */
function stripDisabledStatusOnlyConfig(block) {
    if (!block || typeof block !== "object") return block;
    if (block.status === false || block.enabled === false) return { status: false };
    return block;
}

/** Analysis API requires lead_qualification for sales.lead_qualification / sales.lead_outreach. */
function ensureLeadQualificationOnAnalysisPayload(payload, campaign = null) {
    if (!payload || typeof payload !== "object") return payload;
    const subId = String(
        payload.sub_service_id ||
            campaign?.campaignServiceSubId ||
            campaign?.sub_service_id ||
            campaign?.subServiceId ||
            ""
    ).trim();
    if (subId !== "lead_qualification" && subId !== "lead_outreach") return payload;

    if (
        payload.lead_qualification &&
        typeof payload.lead_qualification === "object" &&
        (payload.lead_qualification.type === "new_lead" ||
            payload.lead_qualification.type === "existing_lead")
    ) {
        return payload;
    }

    const nested =
        (campaign?.lead_qualification && typeof campaign.lead_qualification === "object"
            ? campaign.lead_qualification.type
            : null) ||
        (campaign?.leadQualification && typeof campaign.leadQualification === "object"
            ? campaign.leadQualification.type
            : null) ||
        (payload.lead_qualification && typeof payload.lead_qualification === "object"
            ? payload.lead_qualification.type
            : null);
    const raw = String(
        nested ||
            campaign?.leadQualificationType ||
            campaign?.lead_qualification_type ||
            ""
    ).trim();

    return {
        ...payload,
        lead_qualification: {
            type: raw === "existing_lead" ? "existing_lead" : "new_lead",
        },
    };
}

function sanitizeAnalysisPayloadForV1Api(payload, campaign = null) {
    if (!payload || typeof payload !== "object") return payload;
    let out = { ...payload };
    out = ensureLeadQualificationOnAnalysisPayload(out, campaign);

    if (out.features_enabled && typeof out.features_enabled === "object") {
        if (out.features_enabled.is_followup_enabled === false) {
            out.features_enabled = { is_followup_enabled: false };
        } else {
            const fe = { ...out.features_enabled };
            if (fe.demo_booking) fe.demo_booking = stripDisabledFeatureBlock(fe.demo_booking);
            if (fe.email_followup) fe.email_followup = stripDisabledFeatureBlock(fe.email_followup);
            if (fe.whatsapp_followup) fe.whatsapp_followup = stripDisabledFeatureBlock(fe.whatsapp_followup);
            if (fe.callback_scheduling) fe.callback_scheduling = stripDisabledFeatureBlock(fe.callback_scheduling);
            out.features_enabled = fe;
        }
    }

    if (out.win_back_config !== undefined) {
        out.win_back_config = normalizeWinBackConfigForV1Api(out.win_back_config);
    }

    if (out.promotion_config !== undefined) {
        out.promotion_config = stripDisabledStatusOnlyConfig(out.promotion_config);
    }

    if (typeof out.conversation_text === "string" && out.conversation_text.trim()) {
        out.conversation_text = out.conversation_text
            .replace(/\bAgent\s*:/gi, "AI:")
            .replace(/\bAssistant\s*:/gi, "AI:")
            .replace(/\bBot\s*:/gi, "AI:")
            .replace(/\bCustomer\s*:/gi, "User:")
            .replace(/\bCaller\s*:/gi, "User:");
    }

    if (out.user_contact_info && typeof out.user_contact_info === "object") {
        const u = out.user_contact_info;
        out.user_contact_info = {
            contact_name: String(u.contact_name || "").trim(),
            contact_mobile: String(u.contact_mobile || "").trim(),
            contact_email: String(u.contact_email || "").trim(),
        };
    }

    return out;
}

module.exports = {
    getCleanString,
    normalizeConversationTurns,
    buildConversationTextForAnalysis,
    conversationTextHasAiOrUserTurn,
    buildBaseEnrichedFields,
    buildCompanyInfoForAnalysis,
    buildUserContactInfoForAnalysis,
    sanitizeAnalysisPayloadForV1Api,
    ensureLeadQualificationOnAnalysisPayload,
    buildWhatsappSampleValues,
    mapWhatsappTemplateDocForAnalysis,
    resolveWhatsappTemplateDescription,
};
