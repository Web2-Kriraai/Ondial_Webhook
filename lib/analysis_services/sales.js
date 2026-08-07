const { buildBaseEnrichedFields } = require("./shared");

function isWinBackConfigEnabled(raw) {
    if (!raw || typeof raw !== "object") return false;
    if (raw.enabled === true || raw.status === true) return true;
    return false;
}

function isPromotionConfigEnabled(raw) {
    if (!raw || typeof raw !== "object") return false;
    if (raw.enabled === true || raw.status === true) return true;
    return false;
}

function mapPromotionConfigForAnalysis(campaign) {
    const rawCfg = campaign?.promotionConfig || campaign?.promotion_config;
    if (!isPromotionConfigEnabled(rawCfg)) {
        return null;
    }
    return {
        status: true,
        promotion_name: String(rawCfg?.promotionName || rawCfg?.promotion_name || campaign?.promotionName || "").trim(),
        promo_code: String(rawCfg?.promoCode || rawCfg?.promo_code || campaign?.promoCode || "").trim(),
        discount_details: String(rawCfg?.discountDetails || rawCfg?.discount_details || campaign?.discountDetails || "").trim(),
        offer_validity: String(rawCfg?.offerValidity || rawCfg?.offer_validity || campaign?.offerValidity || "").trim(),
        product_name: String(rawCfg?.productName || rawCfg?.product_name || campaign?.productName || "").trim(),
        original_price: String(rawCfg?.originalPrice || rawCfg?.original_price || campaign?.originalPrice || "").trim(),
        discounted_price: String(rawCfg?.discountedPrice || rawCfg?.discounted_price || campaign?.discountedPrice || "").trim(),
        store_website_url: String(rawCfg?.storeWebsiteUrl || rawCfg?.store_website_url || campaign?.storeWebsiteUrl || "").trim(),
    };
}

function mapWinBackConfigForAnalysis(campaign) {
    const rawCfg = campaign?.winBackConfig || campaign?.win_back_config;
    if (!isWinBackConfigEnabled(rawCfg)) {
        return null;
    }
    const offerPresented = String(
        rawCfg?.offerPresented || rawCfg?.offer_presented || campaign?.winbackOffer || ""
    ).trim();
    const offerValidity = String(
        rawCfg?.offerValidity || rawCfg?.offer_validity || campaign?.winbackValidity || ""
    ).trim();
    if (!offerPresented || !offerValidity) {
        return null;
    }
    const reactivationCondition = String(
        rawCfg?.reactivationCondition ||
            rawCfg?.reactivation_condition ||
            campaign?.winbackCondition ||
            ""
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

/** Analysis API: lead_qualification.type for sales.lead_outreach / sales.lead_qualification. */
function mapLeadQualificationForAnalysis(campaign) {
    const nested =
        (campaign?.lead_qualification && typeof campaign.lead_qualification === "object"
            ? campaign.lead_qualification.type
            : null) ||
        (campaign?.leadQualification && typeof campaign.leadQualification === "object"
            ? campaign.leadQualification.type
            : null);
    const raw = String(
        nested ||
            campaign?.leadQualificationType ||
            campaign?.lead_qualification_type ||
            ""
    ).trim();
    return {
        type: raw === "existing_lead" ? "existing_lead" : "new_lead",
    };
}

function enrich(payload, campaign, callLog, contact, subServiceId) {
    const base = buildBaseEnrichedFields(payload, campaign, callLog, contact);
    
    let canonicalSubId = subServiceId || campaign?.campaignServiceSubId || "";
    if (canonicalSubId === "win_back") canonicalSubId = "win_back_campaigns";
    if (canonicalSubId === "product_promotion") canonicalSubId = "product_promotion_calls";
    if (canonicalSubId === "cold_outreach") canonicalSubId = "cold_outreach_prospecting";

    const extraConfig = {};
    if (canonicalSubId === "win_back_campaigns") {
        const winBackConfig = mapWinBackConfigForAnalysis(campaign);
        extraConfig.win_back_config = winBackConfig || { status: false };
    } else if (canonicalSubId === "product_promotion_calls") {
        const promotionConfig = mapPromotionConfigForAnalysis(campaign);
        extraConfig.promotion_config = promotionConfig || { status: false };
    } else if (canonicalSubId === "lead_outreach" || canonicalSubId === "lead_qualification") {
        // Analysis API requires lead_qualification for sales.lead_outreach and sales.lead_qualification.
        extraConfig.lead_qualification = mapLeadQualificationForAnalysis(campaign);
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
