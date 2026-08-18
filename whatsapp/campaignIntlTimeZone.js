/**
 * Campaign `timezone` is often stored as UI labels from the campaign form
 * (e.g. "(UTC+5:30) Chennai, Kolkata…") — not valid `Intl` IANA IDs.
 */

const DISPLAY_LABEL_TO_IANA = {
  "(UTC+5:30) Chennai, Kolkata, Mumbai, New Delhi (IST)": "Asia/Kolkata",
  "(UTC+5:30) India Standard Time (IST)": "Asia/Kolkata",
  "(UTC-8:00) Pacific Standard Time (PST)": "America/Los_Angeles",
  "(UTC-5:00) Eastern Standard Time (EST)": "America/New_York",
  "(UTC+3:00) Moscow Standard Time (MSK)": "Europe/Moscow",
  "(UTC+9:00) Japan Standard Time (JST)": "Asia/Tokyo",
  "(UTC+1:00) Central European Time (CET)": "Europe/Berlin",
  "(UTC+8:00) China Standard Time (CST)": "Asia/Shanghai",
  "(UTC-3:00) Argentina Standard Time (ART)": "America/Argentina/Buenos_Aires",
  "(UTC+10:00) Australian Eastern Standard Time (AEST)": "Australia/Sydney",
  "(UTC-7:00) Mountain Standard Time (MST)": "America/Denver",
  "(UTC+1:00) British Summer Time (BST, London)": "Europe/London",
  "(UTC+4:00) Gulf Standard Time (GST)": "Asia/Dubai",
  "(UTC+0:00) Coordinated Universal Time (UTC)": "UTC",
};

function isValidIanaTimeZone(id) {
  if (!id || typeof id !== "string") return false;
  try {
    Intl.DateTimeFormat("en-US", { timeZone: id }).format(new Date());
    return true;
  } catch {
    return false;
  }
}

function resolveCampaignIntlTimeZoneId(raw) {
  const s = String(raw ?? "").trim();
  if (!s) return "Asia/Kolkata";

  if (isValidIanaTimeZone(s)) return s;

  if (DISPLAY_LABEL_TO_IANA[s]) return DISPLAY_LABEL_TO_IANA[s];

  const compact = s.replace(/\s+/g, " ");
  if (DISPLAY_LABEL_TO_IANA[compact]) return DISPLAY_LABEL_TO_IANA[compact];

  const lower = s.toLowerCase();
  if (lower.includes("kolkata") || lower.includes("new delhi") || (lower.includes("ist") && lower.includes("+5:30"))) {
    return "Asia/Kolkata";
  }
  if (lower.includes("pacific standard") && lower.includes("-8:")) return "America/Los_Angeles";
  if (lower.includes("eastern standard") && lower.includes("-5:")) return "America/New_York";
  if (lower.includes("japan") && lower.includes("+9:")) return "Asia/Tokyo";
  if (lower.includes("china") && lower.includes("+8:")) return "Asia/Shanghai";
  if (lower.includes("london") || (lower.includes("bst") && lower.includes("london"))) return "Europe/London";
  if (lower.includes("gulf standard") && lower.includes("+4:")) return "Asia/Dubai";
  if (lower.includes("coordinated universal") || (lower.includes("utc") && lower.includes("+0:00"))) return "UTC";

  return "Asia/Kolkata";
}

module.exports = {
  resolveCampaignIntlTimeZoneId,
  isValidIanaTimeZone,
};
