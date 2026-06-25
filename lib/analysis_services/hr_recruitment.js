const { buildBaseEnrichedFields } = require("./shared");

function enrich(payload, campaign, callLog, contact, subServiceId) {
    const base = buildBaseEnrichedFields(payload, campaign, callLog, contact);
    
    let canonicalSubId = subServiceId || campaign?.campaignServiceSubId || "";

    return {
        ...payload,
        ...base,
        wizard_service_id: "hr_recruitment",
        sub_service_id: canonicalSubId || "candidate_screening"
    };
}

module.exports = { enrich };
