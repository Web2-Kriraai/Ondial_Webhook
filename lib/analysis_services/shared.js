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

    const user_contact_info = {
        contact_name: contact?.contactData?.name || contact?.name || callLog?.contactName || "Unknown",
        contact_mobile: contact?.mobileNumber || contact?.phone || callLog?.mobileNumber || "",
        contact_email: contact?.contactData?.email || contact?.email || "",
        designation: contact?.contactData?.designation || contact?.designation || "",
        company: contact?.contactData?.company || contact?.company || ""
    };

    const isFollowup = campaign?.followup === true;
    const hasEmailChannel = Array.isArray(campaign?.followupChannels) && campaign.followupChannels.includes('email');
    const hasCallChannel = Array.isArray(campaign?.followupChannels) && campaign.followupChannels.includes('call');
    const emailStatus = isFollowup && hasEmailChannel;

    const rawDemoType = campaign?.appointmentsDemosType || campaign?.demo_booking?.type || "";
    const demoBookingType = rawDemoType === "site_visit"
        ? "Site Visit Appointment"
        : rawDemoType === "demo_call"
            ? "Demo Call Appointment"
            : rawDemoType;

    const demoBookingStatus = campaign?.appointmentsDemosEnabled === true || campaign?.demo_booking?.status === true;

    const features_enabled = {
        is_followup_enabled: isFollowup,
        demo_booking: demoBookingStatus ? {
            status: true,
            type: demoBookingType,
            booking_condition: campaign?.appointmentsDemosBookingCondition || campaign?.demo_booking?.booking_condition || ""
        } : {
            status: false
        },
        email_followup: (() => {
            if (!emailStatus) return { status: false };
            
            const contactName = contact?.contactData?.name || contact?.name || callLog?.contactName || "Customer";
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
        callback_scheduling: {
            status: hasCallChannel
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

module.exports = {
    getCleanString,
    normalizeConversationTurns,
    buildBaseEnrichedFields,
    buildCompanyInfoForAnalysis,
};
