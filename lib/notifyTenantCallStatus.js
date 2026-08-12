/**
 * Notify Ondial dashboard to fan-out tenant CRM webhooks (call.status).
 * Fire-and-forget; never throw into dial path.
 */
async function notifyTenantCallStatus({ contactId, userId, callReceiveStatus, campaignId }) {
  // Prefer dedicated dashboard URL; also accept ONDIAL_APP_URL (existing webhook service name).
  const base =
    process.env.ONDIA_DASHBOARD_URL ||
    process.env.ONDIA_APP_URL ||
    process.env.ONDIAL_APP_URL ||
    process.env.NEXT_PUBLIC_APP_URL ||
    '';
  const secret =
    process.env.INTERNAL_TENANT_WEBHOOK_SECRET ||
    process.env.CRON_SECRET ||
    process.env.WEBHOOK_INTERNAL_SECRET ||
    '';
  if (!base || !secret || !userId) return;

  try {
    const url = `${String(base).replace(/\/$/, '')}/api/internal/tenant-webhooks/dispatch`;
    await fetch(url, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${secret}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        userId: String(userId),
        eventType: 'call.status',
        data: {
          contactId: contactId ? String(contactId) : undefined,
          campaignId: campaignId ? String(campaignId) : undefined,
          callReceiveStatus,
        },
      }),
      signal: AbortSignal.timeout(5000),
    });
  } catch (err) {
    // swallow — dial path must not fail on CRM notify
    if (process.env.DEBUG_TENANT_WEBHOOKS === '1') {
      console.warn('[notifyTenantCallStatus]', err?.message || err);
    }
  }
}

module.exports = { notifyTenantCallStatus };
