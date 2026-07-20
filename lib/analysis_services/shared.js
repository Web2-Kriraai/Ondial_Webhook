const { ObjectId } = require("mongodb");

function getCleanString(val) {
    return val != null ? String(val).trim() : "";
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
            const role = turn.role || turn.speaker || "";
            const text = turn.text || turn.message || turn.content || "";
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

function formatConversationText(turns) {
    return turns.map(t => {
        const role = Object.keys(t)[0];
        const text = t[role];
        return `${role}: ${text}`;
    }).join(", ");
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
    const turns = normalizeConversationTurns(callLog || {});
    const conversationText = formatConversationText(turns);

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
    const hasCallChannel = Array.isArray(campaign?.followupChannels) && campaign.followupChannels.includes('call');
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

            const templates = (campaign?.whatsappTemplates || []).map((t) => ({
                id: t.id || String(t._id || ""),
                title: t.title || t.name || t.templateName || "",
                description: t.description || t.bodyText || t.title || t.name || "",
                campaign_name: t.aisensyCampaignName || t.campaignName || "",
                sample_values: buildWhatsappSampleValues(t),
            }));
            if (templates.length === 0) return { status: false };
            return { status: true, templates };
        })(),
        callback_scheduling: {
            status: Boolean(isFollowup && hasCallChannel)
        }
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

/**
 * Build analysis payload sample_values as { "{{1}}": "...", "{{2}}": "..." }.
 */
function buildWhatsappSampleValues(template = {}) {
    const existing = template.sample_values ?? template.sampleValues;
    if (existing && typeof existing === "object" && !Array.isArray(existing)) {
        const out = {};
        for (const [rawKey, rawVal] of Object.entries(existing)) {
            const k = String(rawKey || "").trim();
            let key = null;
            if (/^\{\{\d+\}\}$/.test(k)) key = k;
            else if (/^\d+$/.test(k)) key = `{{${k}}}`;
            else {
                const m = k.match(/(\d+)/);
                if (m) key = `{{${m[1]}}}`;
            }
            if (key) out[key] = String(rawVal ?? "");
        }
        if (Object.keys(out).length) return out;
    }

    const arr = Array.isArray(existing)
        ? existing
        : Array.isArray(template.variableDefaults)
          ? template.variableDefaults
          : Array.isArray(template.sampleValues)
            ? template.sampleValues
            : [];

    const body = String(template.description || template.bodyText || template.body || "");
    const fromBody = [...body.matchAll(/\{\{(\d+)\}\}/g)].map((m) => Number(m[1]));
    const count = Math.max(
        Number(template.variableCount) || 0,
        fromBody.length ? Math.max(...fromBody) : 0,
        arr.length
    );
    if (count <= 0) return {};

    const out = {};
    for (let i = 1; i <= count; i += 1) {
        out[`{{${i}}}`] = String(arr[i - 1] ?? "—");
    }
    return out;
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

function sanitizeAnalysisPayloadForV1Api(payload) {
    if (!payload || typeof payload !== "object") return payload;
    const out = { ...payload };

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
    buildBaseEnrichedFields,
    buildCompanyInfoForAnalysis,
    buildUserContactInfoForAnalysis,
    sanitizeAnalysisPayloadForV1Api,
};
