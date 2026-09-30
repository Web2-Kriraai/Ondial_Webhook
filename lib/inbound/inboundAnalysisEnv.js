/**
 * Env helpers for inbound Analysis API (no DB imports — safe for unit tests).
 *
 * Prefer full URL:
 *   INBOUND_ANALYSIS_URL=https://foreignscript.ondial.ai/v1/analysis/inbound-call
 *
 * Fallbacks:
 *   INBOUND_ANALYSIS_PATH as full https://… URL, OR
 *   ANALYSIS_API_URL + relative INBOUND_ANALYSIS_PATH
 */

function inboundAnalysisEnabled() {
  const raw = String(process.env.INBOUND_ANALYSIS_ENABLED ?? "1").trim().toLowerCase();
  return raw !== "0" && raw !== "false" && raw !== "off" && raw !== "no";
}

function inboundAnalysisPath() {
  const path = String(process.env.INBOUND_ANALYSIS_PATH || "/v1/analysis/inbound-call").trim();
  if (/^https?:\/\//i.test(path)) return path.replace(/\/$/, "");
  return path.startsWith("/") ? path : `/${path}`;
}

function buildInboundAnalysisUrl() {
  // 1) Dedicated full inbound URL (preferred)
  const dedicated = String(process.env.INBOUND_ANALYSIS_URL || "").trim().replace(/\/$/, "");
  if (dedicated) return dedicated;

  // 2) INBOUND_ANALYSIS_PATH may itself be a full URL
  const pathOrUrl = inboundAnalysisPath();
  if (/^https?:\/\//i.test(pathOrUrl)) return pathOrUrl;

  // 3) Base ANALYSIS_API_URL + relative path
  const raw = String(process.env.ANALYSIS_API_URL || "").trim().replace(/\/$/, "");
  if (!raw) return null;
  if (/inbound-call/i.test(raw)) return raw;
  const base = raw
    .replace(/\/v1\/analysis\/call$/i, "")
    .replace(/\/v1\/analyze\/call$/i, "")
    .replace(/\/analysis\/call$/i, "")
    .replace(/\/analyze\/call$/i, "")
    .replace(/\/$/, "");
  return `${base}${pathOrUrl}`;
}

module.exports = {
  inboundAnalysisEnabled,
  inboundAnalysisPath,
  buildInboundAnalysisUrl,
};
