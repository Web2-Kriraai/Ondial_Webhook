const { buildBaseEnrichedFields } = require("./shared");

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

    return {
        ...payload,
        ...base,
        wizard_service_id: "notifications_alerts",
        sub_service_id: canonicalSubId || "appointment_reminders",
        ...extraConfig
    };
}

module.exports = { enrich };
