/**
 * Notify Ondial dashboard to fan-out tenant CRM webhooks.
 * Fire-and-forget; never throw into dial / analysis path.
 */

function resolveDashboardBase() {
  return (
    process.env.ONDIA_DASHBOARD_URL ||
    process.env.ONDIA_APP_URL ||
    process.env.ONDIAL_APP_URL ||
    process.env.NEXT_PUBLIC_APP_URL ||
    ''
  );
}

function resolveInternalSecret() {
  return (
    process.env.INTERNAL_TENANT_WEBHOOK_SECRET ||
    process.env.CRON_SECRET ||
    process.env.WEBHOOK_INTERNAL_SECRET ||
    ''
  );
}

async function postTenantWebhookDispatch({ userId, eventType, data }) {
  const base = resolveDashboardBase();
  const secret = resolveInternalSecret();
  if (!base || !secret || !userId || !eventType) return { ok: false, reason: 'not_configured' };

  try {
    const url = `${String(base).replace(/\/$/, '')}/api/internal/tenant-webhooks/dispatch`;
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${secret}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        userId: String(userId),
        eventType: String(eventType),
        data: data && typeof data === 'object' ? data : {},
      }),
      signal: AbortSignal.timeout(8000),
    });
    return { ok: res.ok, status: res.status };
  } catch (err) {
    if (process.env.DEBUG_TENANT_WEBHOOKS === '1') {
      console.warn('[notifyTenantWebhook]', eventType, err?.message || err);
    }
    return { ok: false, reason: err?.message || String(err) };
  }
}

/** @deprecated name kept — maps to call.outbound.status */
async function notifyTenantCallStatus({
  contactId,
  userId,
  callReceiveStatus,
  campaignId,
  callId,
  status,
}) {
  return postTenantWebhookDispatch({
    userId,
    eventType: 'call.outbound.status',
    data: {
      contactId: contactId ? String(contactId) : undefined,
      campaignId: campaignId ? String(campaignId) : undefined,
      callId: callId ? String(callId) : undefined,
      callReceiveStatus,
      status: status || undefined,
    },
  });
}

async function notifyTenantCallBilled({
  userId,
  contactId,
  campaignId,
  callId,
  usageId,
  balanceAfter,
  creditsCharged,
}) {
  return postTenantWebhookDispatch({
    userId,
    eventType: 'call.outbound.billed',
    data: {
      contactId: contactId ? String(contactId) : undefined,
      campaignId: campaignId ? String(campaignId) : undefined,
      callId: callId ? String(callId) : undefined,
      usageId: usageId ? String(usageId) : undefined,
      balanceAfter,
      creditsCharged,
    },
  });
}

async function notifyTenantCallAnalysis({
  userId,
  contactId,
  campaignId,
  callId,
}) {
  return postTenantWebhookDispatch({
    userId,
    eventType: 'call.outbound.analysis',
    data: {
      contactId: contactId ? String(contactId) : undefined,
      campaignId: campaignId ? String(campaignId) : undefined,
      callId: callId ? String(callId) : undefined,
    },
  });
}

module.exports = {
  notifyTenantCallStatus,
  notifyTenantCallBilled,
  notifyTenantCallAnalysis,
  postTenantWebhookDispatch,
};
