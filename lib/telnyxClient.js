const TELNYX_API_BASE = 'https://api.telnyx.com/v2';

function getTelnyxApiKey() {
  return process.env.TELNYX_API_KEY?.trim() || '';
}

function isTelnyxConfigured() {
  return Boolean(getTelnyxApiKey());
}

async function telnyxRequest(path, opts = {}) {
  const apiKey = getTelnyxApiKey();
  if (!apiKey) {
    throw new Error('Telnyx is not configured (TELNYX_API_KEY)');
  }
  const method = String(opts.method || 'GET').toUpperCase();
  const headers = {
    Authorization: `Bearer ${apiKey}`,
    Accept: 'application/json',
  };
  if (opts.body != null) {
    headers['Content-Type'] = 'application/json';
  }
  const res = await fetch(`${TELNYX_API_BASE}${path}`, {
    method,
    headers,
    body: opts.body != null ? JSON.stringify(opts.body) : undefined,
  });
  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }
  if (!res.ok) {
    const detail =
      json?.errors?.[0]?.detail ||
      json?.errors?.[0]?.title ||
      text ||
      res.statusText;
    const err = new Error(detail || `Telnyx API ${res.status}`);
    err.status = res.status;
    err.telnyx = json;
    throw err;
  }
  return json;
}

/**
 * Force-hangup a Call Control leg.
 * @param {string} callControlId
 */
async function hangupCallControl(callControlId) {
  const id = String(callControlId || '').trim();
  if (!id) throw new Error('call_control_id is required');
  await telnyxRequest(`/calls/${encodeURIComponent(id)}/actions/hangup`, {
    method: 'POST',
    body: {},
  });
  return { ok: true, callControlId: id };
}

module.exports = {
  isTelnyxConfigured,
  hangupCallControl,
  telnyxRequest,
};
