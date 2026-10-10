const { buildBaseEnrichedFields } = require("./shared");

const APPOINTMENT_REMINDER_DEMO_TYPE_HINT =
    "CRITICAL: analysis.demo_booked.type MUST be EXACTLY \"Site Visit Appointment\" " +
    "or \"Demo Call Appointment\". For hospital/doctor appointment reminders use " +
    "\"Site Visit Appointment\". Never use bare \"appointment\".";

function enrich(payload, campaign, callLog, contact, subServiceId) {
    const base = buildBaseEnrichedFields(payload, campaign, callLog, contact);
    
    let canonicalSubId = subServiceId || campaign?.campaignServiceSubId || "";
    if (canonicalSubId === "order_delivery") canonicalSubId = "order_delivery_updates";
    if (canonicalSubId === "event_booking") canonicalSubId = "event_booking_confirmations";
    if (canonicalSubId === "emergency_alert") canonicalSubId = "emergency_critical_alerts";
    if (canonicalSubId === "compliance_deadline") canonicalSubId = "compliance_deadlines";

    const extraConfig = {};
    if (canonicalSubId === "appointment_reminders") {
        extraConfig.appointment_reminders = campaign?.appointment_reminders || {
            allow_reschedule: campaign?.allowReschedule !== false && campaign?.allow_reschedule !== false,
            cancellation_policy: {
                enabled: campaign?.cancellationPolicyEnabled === true || campaign?.cancellation_policy_enabled === true,
                text: campaign?.cancellationPolicyText || campaign?.cancellation_policy_text || ""
            }
        };
    } else if (canonicalSubId === "event_booking_confirmations") {
        extraConfig.event_booking = campaign?.event_booking || {
            allow_modifications: campaign?.allowBookingModifications !== false
        };
    } else if (canonicalSubId === "emergency_critical_alerts") {
        extraConfig.emergency_alert = campaign?.emergency_alert || {
            aggressive_retry: campaign?.emergencyAlertAggressiveRetry !== false,
            parallel_sms: campaign?.emergencyAlertParallelSms !== false,
            locked: true
        };
    }

    const merged = {
        ...payload,
        ...base,
        wizard_service_id: "notifications_alerts",
        sub_service_id: canonicalSubId || "appointment_reminders",
        ...extraConfig
    };

    if ((canonicalSubId || "appointment_reminders") === "appointment_reminders") {
        const reason = String(merged.reason_for_calling || "").trim();
        if (!reason.includes("demo_booked.type MUST be EXACTLY") && !reason.includes("Never use bare \"appointment\"")) {
            merged.reason_for_calling = [reason || "Analyze the call conversation.", APPOINTMENT_REMINDER_DEMO_TYPE_HINT]
                .filter(Boolean)
                .join("\n");
        }
        delete merged.payload_generated_at;
    }

    return merged;
}

module.exports = { enrich };
