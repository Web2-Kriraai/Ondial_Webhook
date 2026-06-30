const EventEmitter = require("events");
const logger = require("../logger");

const DEFAULT_CHANNEL = process.env.CAMPAIGN_DELTA_REDIS_CHANNEL || "campaign.delta.v1";

const localBus = new EventEmitter();
localBus.setMaxListeners(0);

let subscriber = null;
let subscriberInitPromise = null;

async function ensureRedisSubscriber() {
    if (subscriberInitPromise) return subscriberInitPromise;

    subscriberInitPromise = (async () => {
        try {
            const { getRedis } = require("../redis");
            subscriber = getRedis().duplicate();
            await subscriber.subscribe(DEFAULT_CHANNEL);
            subscriber.on("message", (channel, payload) => {
                if (channel !== DEFAULT_CHANNEL || !payload) return;
                try {
                    const event = JSON.parse(payload);
                    localBus.emit("event", event);
                } catch {
                    // ignore malformed payloads
                }
            });
            logger.info("[SSE] Redis campaign delta subscriber ready", { channel: DEFAULT_CHANNEL });
        } catch (err) {
            logger.warn("[SSE] Redis campaign delta subscriber unavailable", { error: err.message });
            subscriberInitPromise = null;
        }
    })();

    return subscriberInitPromise;
}

function publishCampaignDeltaEvent(event) {
    const safeEvent = event && typeof event === "object" ? { ...event } : { data: event };
    if (!safeEvent.ts) safeEvent.ts = Date.now();

    localBus.emit("event", safeEvent);

    try {
        const { getRedis } = require("../redis");
        void getRedis()
            .publish(DEFAULT_CHANNEL, JSON.stringify(safeEvent))
            .catch((err) => {
                logger.warn("[SSE] Redis publish failed", { error: err.message });
            });
    } catch {
        // Redis not connected yet — local bus still fanouts on this instance.
    }
}

function subscribeCampaignDelta(listener) {
    if (typeof listener !== "function") {
        throw new TypeError("listener must be a function");
    }
    void ensureRedisSubscriber();
    localBus.on("event", listener);
    return () => {
        localBus.off("event", listener);
    };
}

module.exports = {
    DEFAULT_CHANNEL,
    publishCampaignDeltaEvent,
    subscribeCampaignDelta,
};
