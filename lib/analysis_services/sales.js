const { buildBaseEnrichedFields } = require("./shared");

function enrich(payload, campaign, callLog, contact, subServiceId) {
    const base = buildBaseEnrichedFields(payload, campaign, callLog, contact);
    
    let canonicalSubId = subServiceId || campaign?.campaignServiceSubId || "";
    if (canonicalSubId === "win_back") canonicalSubId = "win_back_campaigns";
    if (canonicalSubId === "product_promotion") canonicalSubId = "product_promotion_calls";
    if (canonicalSubId === "cold_outreach") canonicalSubId = "cold_outreach_prospecting";

    const extraConfig = {};
    if (canonicalSubId === "win_back_campaigns") {
        const rawCfg = campaign?.winBackConfig || campaign?.win_back_config;
        extraConfig.win_back_config = {
            offer_presented: String(rawCfg?.offerPresented || rawCfg?.offer_presented || campaign?.winbackOffer || "").trim(),
            offer_validity: String(rawCfg?.offerValidity || rawCfg?.offer_validity || campaign?.winbackValidity || "").trim(),
            reactivation_condition: String(rawCfg?.reactivationCondition || rawCfg?.reactivation_condition || campaign?.winbackCondition || "").trim()
        };
    } else if (canonicalSubId === "product_promotion_calls") {
        const rawCfg = campaign?.promotionConfig || campaign?.promotion_config;
        extraConfig.promotion_config = {
            promotion_name: String(rawCfg?.promotionName || rawCfg?.promotion_name || campaign?.promotionName || "").trim(),
            promo_code: String(rawCfg?.promoCode || rawCfg?.promo_code || campaign?.promoCode || "").trim(),
            discount_details: String(rawCfg?.discountDetails || rawCfg?.discount_details || campaign?.discountDetails || "").trim(),
            offer_validity: String(rawCfg?.offerValidity || rawCfg?.offer_validity || campaign?.offerValidity || "").trim(),
            product_name: String(rawCfg?.productName || rawCfg?.product_name || campaign?.productName || "").trim(),
            original_price: String(rawCfg?.originalPrice || rawCfg?.original_price || campaign?.originalPrice || "").trim(),
            discounted_price: String(rawCfg?.discountedPrice || rawCfg?.discounted_price || campaign?.discountedPrice || "").trim(),
            store_website_url: String(rawCfg?.storeWebsiteUrl || rawCfg?.store_website_url || campaign?.storeWebsiteUrl || "").trim()
        };
    } else if (canonicalSubId === "lead_qualification") {
        extraConfig.lead_qualification = campaign?.lead_qualification || campaign?.leadQualification || {
            type: campaign?.leadQualificationType || "new_lead",
            is_new_lead: campaign?.isNewLead !== false
        };
    }

    return {
        ...payload,
        ...base,
        wizard_service_id: "sales",
        sub_service_id: canonicalSubId || "cold_outreach_prospecting",
        ...extraConfig
    };
}

module.exports = { enrich };
