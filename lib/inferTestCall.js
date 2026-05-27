const { parseJsonObject } = require('./customParameters');

function inferIsTestCallFromWebhookBody(body) {
  if (!body || typeof body !== 'object') return false;
  if (body.is_test_call === true || body.is_test_call === 'true') return true;

  const call = body._raw?.call || body.call || {};
  const callType = String(call.callType || call.call_type || '').trim().toLowerCase();
  if (callType === 'agent') return true;

  const cp =
    parseJsonObject(call.customParameters) ||
    parseJsonObject(call.custom_parameters) ||
    (call.customParameters && typeof call.customParameters === 'object' ? call.customParameters : null);

  if (cp?.is_test_call === true || cp?.is_test_call === 'true') return true;
  if (cp?.isTestCall === true || cp?.isTestCall === 'true') return true;

  return false;
}

module.exports = { inferIsTestCallFromWebhookBody };
