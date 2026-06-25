const { buildBaseEnrichedFields } = require("./shared");

function enrich(payload, campaign, callLog, contact, subServiceId) {
    const base = buildBaseEnrichedFields(payload, campaign, callLog, contact);
    
    let canonicalSubId = subServiceId || campaign?.campaignServiceSubId || "";

    return {
        ...payload,
        ...base,
        wizard_service_id: "survey_feedback",
        sub_service_id: canonicalSubId || "nps_csat_surveys"
    };
}

module.exports = { enrich };
