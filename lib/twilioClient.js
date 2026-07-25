/**
 * Minimal Twilio REST helpers for Ondial_Webhook (hangup only).
 * Uses TWILIO_ACCOUNT_SID + TWILIO_AUTH_TOKEN (same as Ondial).
 */

function getTwilioAccountSid() {
  return String(process.env.TWILIO_ACCOUNT_SID || "").trim();
}

function getTwilioAuthToken() {
  return String(process.env.TWILIO_AUTH_TOKEN || "").trim();
}

function isTwilioConfigured() {
  return Boolean(getTwilioAccountSid() && getTwilioAuthToken());
}

/**
 * Force-complete a Twilio CallSid (hangup).
 * @param {string} callSid
 */
async function hangupTwilioCall(callSid) {
  const sid = String(callSid || "").trim();
  if (!sid) throw new Error("CallSid is required");
  if (!isTwilioConfigured()) {
    throw new Error("Twilio is not configured (TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN)");
  }

  const accountSid = getTwilioAccountSid();
  const auth = Buffer.from(`${accountSid}:${getTwilioAuthToken()}`).toString("base64");
  const url = `https://api.twilio.com/2010-04-01/Accounts/${encodeURIComponent(accountSid)}/Calls/${encodeURIComponent(sid)}.json`;
  const body = new URLSearchParams({ Status: "completed" });

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Basic ${auth}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body,
  });
  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }
  if (!res.ok) {
    const detail = json?.message || text || res.statusText;
    const err = new Error(detail || `Twilio hangup ${res.status}`);
    err.status = res.status;
    throw err;
  }
  return { ok: true, callSid: sid, status: json?.status || "completed" };
}

module.exports = {
  isTwilioConfigured,
  hangupTwilioCall,
};
