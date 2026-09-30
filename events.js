const EventEmitter = require('events');
const { publishCampaignDeltaEvent } = require('./lib/campaignDeltaRedisBus');

const callEvents = new EventEmitter();

function pickNonEmpty(...vals) {
  for (const v of vals) {
    if (v == null) continue;
    const s = String(v).trim();
    if (s) return s;
  }
  return null;
}

/**
 * Normalize call_update SSE payload so Ondial clients can refresh live UI
 * for inbound (direction/userId/configId/DID) without inventing campaign_id.
 */
function normalizeCallUpdateSsePayload(payload = {}) {
  const eventName = String(payload.event || payload.type || 'call_update').trim() || 'call_update';
  const directionRaw = String(payload.direction || '').trim().toLowerCase();
  const direction =
    directionRaw === 'inbound' || directionRaw === 'outbound'
      ? directionRaw
      : null;

  const userId = pickNonEmpty(payload.userId, payload.user_id);
  const configId = pickNonEmpty(
    payload.configId,
    payload.config_id,
    payload.inboundConfigId,
    payload.inbound_config_id
  );
  const phoneNumber = pickNonEmpty(
    payload.phoneNumber,
    payload.phone_number,
    payload.to,
    payload.to_number,
    payload.DID,
    payload.did
  );
  const campaignId = pickNonEmpty(payload.campaign_id, payload.campaignId);

  const event = {
    type: 'call_update',
    event: eventName,
    timestamp: payload.timestamp || new Date().toISOString(),
    ...payload,
  };

  // Canonical aliases for Ondial dashboard clients
  event.event = eventName;
  event.type = 'call_update';
  if (direction) {
    event.direction = direction;
  }
  if (userId) {
    event.userId = userId;
    event.user_id = userId;
  }
  if (configId) {
    event.configId = configId;
    event.config_id = configId;
  }
  if (phoneNumber) {
    event.phoneNumber = phoneNumber;
    event.phone_number = phoneNumber;
  }
  // Keep campaign_id only when provided — do not invent for pure inbound
  if (campaignId) {
    event.campaign_id = campaignId;
    event.campaignId = campaignId;
  } else {
    event.campaign_id = event.campaign_id ?? null;
  }

  return event;
}

/**
 * Build enrichment fields from inbound identity / conversation anchor.
 */
function buildInboundSseEnrichment({
  isInbound = false,
  identity = null,
  anchor = null,
  toPhone = null,
  billing = null,
} = {}) {
  if (!isInbound) {
    return {
      direction: 'outbound',
      campaign_id: pickNonEmpty(identity?.campaign_id, identity?.campaignId) || null,
    };
  }

  const configId = pickNonEmpty(
    billing?.inboundConfigId,
    identity?.configId,
    identity?.config_id,
    identity?.inboundConfigId,
    anchor?.campaignId, // resolveInboundConversationAnchor sets campaignId from config_id on doc
    anchor?.doc?.config_id,
    anchor?.doc?.configId,
    anchor?.doc?.inboundConfigId
  );
  const userId = pickNonEmpty(
    billing?.userId,
    identity?.userId,
    identity?.user_id,
    anchor?.userId,
    anchor?.doc?.userId
  );
  const phoneNumber = pickNonEmpty(
    toPhone,
    identity?.phoneNumber,
    identity?.to,
    anchor?.doc?.to_number,
    anchor?.doc?.phone_number
  );
  // Pure inbound: leave campaign_id null so campaign-scoped UI listeners ignore it.
  // Global / inbound analytics use direction + configId + userId instead.
  return {
    direction: 'inbound',
    userId: userId || null,
    user_id: userId || null,
    configId: configId || null,
    config_id: configId || null,
    phoneNumber: phoneNumber || null,
    phone_number: phoneNumber || null,
    campaign_id: null,
  };
}

function emitCallUpdateSse(payload = {}) {
  const event = normalizeCallUpdateSsePayload(payload);
  callEvents.emit('call_update', event);
  publishCampaignDeltaEvent(event);
}

module.exports = callEvents;
module.exports.emitCallUpdateSse = emitCallUpdateSse;
module.exports.normalizeCallUpdateSsePayload = normalizeCallUpdateSsePayload;
module.exports.buildInboundSseEnrichment = buildInboundSseEnrichment;
