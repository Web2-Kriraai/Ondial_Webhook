const EventEmitter = require('events');

const callEvents = new EventEmitter();

function emitCallUpdateSse(payload = {}) {
    const eventName = String(payload.event || payload.type || 'call_update').trim() || 'call_update';
    callEvents.emit('call_update', {
        type: 'call_update',
        event: eventName,
        timestamp: payload.timestamp || new Date().toISOString(),
        ...payload,
    });
}

module.exports = callEvents;
module.exports.emitCallUpdateSse = emitCallUpdateSse;
