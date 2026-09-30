/**
 * CommonJS mirror of Ondial/lib/inbound/buildInboundCallAnalysisPayload.js
 * Keep in sync — dual-repo pattern (same as outbound analysis helpers).
 */

const HEALTHCARE_FEATURE_MAP = {
  enableAppointments: "appointment_booking",
  enableLabReportInquiry: "lab_report",
  enablePrescriptionRouting: "prescription_routing",
};

const ECOMMERCE_FEATURE_MAP = {
  enableOrderLookup: "order_lookup",
  enableComplaintWrite: "complaint",
  enableReturnStatus: "return_status",
  enableExchangeStatus: "exchange_status",
  enableRefundStatus: "refund_status",
  enableProductCategoryInfo: "product_category_info",
};

const LOGISTICS_FEATURE_MAP = {
  enableShipmentTracking: "shipment_tracking",
  enableDeliveryExceptionLookup: "delivery_exception",
  enableZoneAvailability: "zone_availability",
};

const AUTOMOBILE_FEATURE_MAP = {
  enableServiceStatusLookup: "service_status",
  enableVehicleInformation: "vehicle_information",
  enableDeliveryStatusLookup: "delivery_status",
  enableSparePartsLookup: "spare_parts",
  enableServiceBooking: "service_booking",
  enableTestDriveBooking: "test_drive_booking",
  enableSalesInquiry: "sales_inquiry",
  enableComplaint: "complaint",
  enableRoadsideAssistance: "roadside_assistance",
};

const HEALTHCARE_EXTRA_KEYS = [
  "doctors",
  "maxSlotsOfferedPerCall",
  "maxAdvanceDays",
  "emergencyKeywords",
  "facilityName",
  "appointmentDurationMin",
  "emergencyNumber",
];

function omitEmpty(value) {
  if (value === null || value === undefined) return true;
  if (typeof value === "string" && !String(value).trim()) return true;
  return false;
}

function pickTruthyEnableMap(scope, map) {
  const src = scope && typeof scope === "object" && !Array.isArray(scope) ? scope : {};
  const features = [];
  for (const [flag, feature] of Object.entries(map)) {
    if (src[flag] === true) features.push(feature);
  }
  return features;
}

function realEstateFeatureOn(scope, key) {
  return scope?.[key] !== false;
}

function realEstateSiteVisitOn(scope = {}) {
  if (scope?.enableSiteVisit === false) return false;
  if (scope?.enableSiteVisitBooking === false) return false;
  return true;
}

function mapInboundCallFeatures(category, categoryScope = {}) {
  const cat = String(category || "custom").trim() || "custom";
  const scope =
    categoryScope && typeof categoryScope === "object" && !Array.isArray(categoryScope)
      ? categoryScope
      : {};

  if (cat === "healthcare_reception") {
    return pickTruthyEnableMap(scope, HEALTHCARE_FEATURE_MAP);
  }
  if (cat === "appointment_booking") {
    return ["appointment_booking"];
  }
  if (cat === "ecommerce_support") {
    return pickTruthyEnableMap(scope, ECOMMERCE_FEATURE_MAP);
  }
  if (cat === "logistics_courier") {
    return pickTruthyEnableMap(scope, LOGISTICS_FEATURE_MAP);
  }
  if (cat === "automobile") {
    return pickTruthyEnableMap(scope, AUTOMOBILE_FEATURE_MAP);
  }
  if (cat === "real_estate") {
    const features = [];
    if (realEstateFeatureOn(scope, "enableProjectCatalog")) features.push("project_catalog");
    if (realEstateFeatureOn(scope, "enableLeadCapture")) features.push("lead_capture");
    if (realEstateSiteVisitOn(scope)) features.push("site_visit");
    return features;
  }
  return [];
}

function normalizeSpeakerLabel(role) {
  const r = String(role || "").trim();
  if (!r) return null;
  if (/^(ai|agent|assistant|bot)$/i.test(r)) return "Agent";
  if (/^(user|customer|caller|human)$/i.test(r)) return "User";
  return null;
}

function buildInboundConversationText(conversationDoc = {}) {
  const directTurns = Array.isArray(conversationDoc?.conversation?.turns)
    ? conversationDoc.conversation.turns
    : [];
  const rootTurns = Array.isArray(conversationDoc?.turns) ? conversationDoc.turns : [];
  const sourceTurns = directTurns.length ? directTurns : rootTurns;

  const lines = [];
  for (const turn of sourceTurns) {
    if (!turn || typeof turn !== "object") continue;

    const role = turn.role || turn.speaker || "";
    const text = turn.text || turn.message || turn.content || "";
    if (role && text != null && String(text).trim()) {
      const label = normalizeSpeakerLabel(role) || "User";
      lines.push(`${label}: ${String(text).trim()}`);
      continue;
    }

    const user =
      turn.user ?? turn.User ?? turn.customer ?? turn.Customer ?? turn.Caller ?? null;
    const agent =
      turn.ai ?? turn.AI ?? turn.agent ?? turn.Agent ?? turn.assistant ?? turn.Assistant ?? null;

    if (agent != null && String(agent).trim()) {
      lines.push(`Agent: ${String(agent).trim()}`);
    }
    if (user != null && String(user).trim()) {
      lines.push(`User: ${String(user).trim()}`);
    }

    if (
      (agent == null || !String(agent).trim()) &&
      (user == null || !String(user).trim())
    ) {
      const entries = Object.entries(turn).filter(
        ([k, v]) =>
          String(k || "").trim() !== "" &&
          v != null &&
          String(v).trim() !== "" &&
          !["role", "speaker", "at", "timestamp", "ts"].includes(String(k))
      );
      if (!entries.length) continue;
      const [speaker, message] = entries[0];
      const label =
        normalizeSpeakerLabel(speaker) || (/^(ai|agent)/i.test(speaker) ? "Agent" : "User");
      lines.push(`${label}: ${String(message).trim()}`);
    }
  }

  let text = lines.join(", ");
  if (/(?:^|,\s*)(?:Agent|User)\s*:\s*\S+/i.test(text)) return text;

  const transcript = String(
    conversationDoc?.conversation?.transcript ||
      conversationDoc?.transcript ||
      conversationDoc?.conversation_text ||
      ""
  ).trim();
  if (!transcript) return text;

  return transcript
    .replace(/\bAI\s*:/gi, "Agent:")
    .replace(/\bAssistant\s*:/gi, "Agent:")
    .replace(/\bBot\s*:/gi, "Agent:")
    .replace(/\bCustomer\s*:/gi, "User:")
    .replace(/\bCaller\s*:/gi, "User:");
}

function inboundConversationTextHasTurn(conversationText) {
  const text = String(conversationText || "").trim();
  if (!text) return false;
  return /(?:^|,\s*)(?:Agent|AI|User)\s*:\s*\S+/i.test(text);
}

function formatCurrentTimeIst(date = new Date()) {
  try {
    return new Intl.DateTimeFormat("en-IN", {
      timeZone: "Asia/Kolkata",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    }).format(date);
  } catch {
    return date.toISOString();
  }
}

function resolveDurationSeconds(conversationDoc) {
  const sec = Number(conversationDoc?.duration);
  if (Number.isFinite(sec) && sec > 0) return Math.floor(sec);
  const ms = Number(conversationDoc?.duration_ms);
  if (Number.isFinite(ms) && ms > 0) {
    return ms < 1000 ? Math.floor(ms) : Math.floor(ms / 1000);
  }
  return 0;
}

function pickCategoryExtras(category, scope) {
  if (category !== "healthcare_reception") return {};
  const out = {};
  for (const key of HEALTHCARE_EXTRA_KEYS) {
    if (!Object.prototype.hasOwnProperty.call(scope, key)) continue;
    const value = scope[key];
    if (omitEmpty(value)) continue;
    out[key] = value;
  }
  return out;
}

function buildInboundCallAnalysisPayload({
  config,
  company = null,
  conversation,
  extras = {},
} = {}) {
  if (!config || typeof config !== "object") {
    return { ok: false, reason: "config_missing" };
  }
  if (!conversation || typeof conversation !== "object") {
    return { ok: false, reason: "conversation_missing" };
  }

  const configObj = config.toObject ? config.toObject() : config;
  const category = configObj.category || "custom";
  const scope = configObj?.categoryConfig?.[category];
  const categoryScope =
    scope && typeof scope === "object" && !Array.isArray(scope) ? scope : {};

  const conversation_text =
    extras.conversation_text != null
      ? String(extras.conversation_text)
      : buildInboundConversationText(conversation);

  if (!inboundConversationTextHasTurn(conversation_text)) {
    return { ok: false, reason: "conversation_text_empty" };
  }

  const agentName = String(
    configObj.callerFacingName || configObj.name || "Inbound Agent"
  ).trim();
  const businessName = String(
    company?.name || configObj.companyContextSnapshot?.name || ""
  ).trim();
  const companyDesc = [company?.description, company?.industry]
    .filter(Boolean)
    .join(". ")
    .trim();

  const call_feature = mapInboundCallFeatures(category, categoryScope);
  const categoryExtras = pickCategoryExtras(category, categoryScope);

  const callId =
    extras.callId ||
    conversation.call_id ||
    conversation.callId ||
    (conversation._id != null ? String(conversation._id) : null);
  const call_sid =
    extras.call_sid || conversation.call_sid || conversation.callSid || null;

  const duration_seconds =
    extras.duration_seconds != null
      ? Number(extras.duration_seconds)
      : resolveDurationSeconds(conversation);

  const payload = {
    configId: String(configObj._id),
    config: {
      agentName,
      buisnessDetails: {
        businessName,
        businessDescription: companyDesc || businessName || "",
        businessPhone:
          company?.mobile || configObj.businessPhone || conversation.to_number || "",
        businessHours:
          extras.businessHours != null
            ? extras.businessHours
            : configObj.companyContextSnapshot?.businessHours || "",
        primaryLanguage: configObj.primaryLanguage || "hi-IN",
      },
    },
    categoryConfig: {
      category,
      call_feature,
      ...categoryExtras,
    },
    conversation_text,
    callId: callId != null ? String(callId) : undefined,
    call_sid: call_sid != null ? String(call_sid) : undefined,
    from_number:
      extras.from_number ||
      conversation.from_number ||
      conversation.fromNumber ||
      conversation.caller_number ||
      null,
    to_number:
      extras.to_number ||
      conversation.to_number ||
      conversation.toNumber ||
      conversation.phone_number ||
      null,
    current_time_ist: extras.current_time_ist || formatCurrentTimeIst(),
    primaryLanguage: configObj.primaryLanguage || "hi-IN",
    duration_seconds: Number.isFinite(duration_seconds) ? duration_seconds : 0,
    caller_name:
      extras.caller_name ||
      conversation.caller_name ||
      conversation.callerName ||
      conversation.contact_name ||
      null,
    inboundConfigVersion:
      extras.inboundConfigVersion ||
      (configObj.configVersion != null ? configObj.configVersion : null),
    lineageId:
      extras.lineageId ||
      (configObj.lineageId != null ? String(configObj.lineageId) : null),
  };

  for (const key of [
    "callId",
    "call_sid",
    "from_number",
    "to_number",
    "caller_name",
    "inboundConfigVersion",
    "lineageId",
  ]) {
    if (payload[key] == null || payload[key] === "") delete payload[key];
  }

  return { ok: true, payload };
}

function shouldSkipInboundAnalysis({
  conversation,
  isTestCall = false,
  minDurationSeconds = Number(process.env.INBOUND_ANALYSIS_MIN_DURATION_SEC || 0),
} = {}) {
  if (isTestCall === true) return { skip: true, reason: "test_call" };
  if (!conversation) return { skip: true, reason: "conversation_missing" };
  if (
    conversation.isTestCall === true ||
    conversation.is_test === true ||
    conversation.is_test_call === true ||
    conversation.validateOnly === true ||
    conversation.validate_only === true
  ) {
    return { skip: true, reason: "test_or_validate_only" };
  }
  if (
    conversation.analysisStatus === "completed" ||
    (conversation.analysis_data && typeof conversation.analysis_data === "object")
  ) {
    return { skip: true, reason: "already_analyzed" };
  }
  const text = buildInboundConversationText(conversation);
  if (!inboundConversationTextHasTurn(text)) {
    return { skip: true, reason: "conversation_text_empty" };
  }
  const minDur = Number(minDurationSeconds);
  if (Number.isFinite(minDur) && minDur > 0) {
    const dur = resolveDurationSeconds(conversation);
    if (dur > 0 && dur < minDur) {
      return { skip: true, reason: "duration_below_min" };
    }
  }
  return { skip: false, reason: null };
}

module.exports = {
  mapInboundCallFeatures,
  buildInboundConversationText,
  inboundConversationTextHasTurn,
  formatCurrentTimeIst,
  buildInboundCallAnalysisPayload,
  shouldSkipInboundAnalysis,
};
