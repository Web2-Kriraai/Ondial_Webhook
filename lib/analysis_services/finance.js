const { buildBaseEnrichedFields } = require("./shared");

function enrich(payload, campaign, callLog, contact, subServiceId) {
    const base = buildBaseEnrichedFields(payload, campaign, callLog, contact);
    
    let canonicalSubId = subServiceId || campaign?.campaignServiceSubId || "";
    if (canonicalSubId === "debt_recovery") canonicalSubId = "debt_loan_recovery";

    return {
        ...payload,
        ...base,
        wizard_service_id: "finance",
        sub_service_id: canonicalSubId || "loan_origination"
    };
}

module.exports = { enrich };
