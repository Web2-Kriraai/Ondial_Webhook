const EventEmitter = require('events');
const { publishCampaignDeltaEvent } = require('./lib/campaignDeltaRedisBus');

const callEvents = new EventEmitter();

function emitCallUpdateSse(payload = {}) {
    const eventName = String(payload.event || payload.type || 'call_update').trim() || 'call_update';
    const event = {
        type: 'call_update',
        event: eventName,
        timestamp: payload.timestamp || new Date().toISOString(),
        ...payload,
    };
    callEvents.emit('call_update', event);
    publishCampaignDeltaEvent(event);
}

module.exports = callEvents;
module.exports.emitCallUpdateSse = emitCallUpdateSse;
