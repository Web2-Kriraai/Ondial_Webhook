const IORedis = require("ioredis");
const logger = require("./logger");

const REDIS_URL = process.env.REDIS_URL;
if (!REDIS_URL) {
    throw new Error("REDIS_URL is not defined in .env");
}

let redis = null;
let bullRedis = null;

async function connectRedis() {
    if (redis && bullRedis) return { redis, bullRedis };

    redis = new IORedis(REDIS_URL, {
        enableReadyCheck: true,
        maxRetriesPerRequest: 3,
    });

    bullRedis = new IORedis(REDIS_URL, {
        enableReadyCheck: true,
        maxRetriesPerRequest: null,
    });

    await Promise.all([waitUntilReady(redis), waitUntilReady(bullRedis)]);
    logger.info("[Redis] Connected");
    return { redis, bullRedis };
}

function getRedis() {
    if (!redis) throw new Error("Redis not connected. Call connectRedis() first.");
    return redis;
}

function getBullRedis() {
    if (!bullRedis) throw new Error("Bull Redis not connected. Call connectRedis() first.");
    return bullRedis;
}

function waitUntilReady(client) {
    return new Promise((resolve, reject) => {
        const onReady = () => {
            cleanup();
            resolve();
        };
        const onError = (err) => {
            cleanup();
            reject(err);
        };
        const cleanup = () => {
            client.off("ready", onReady);
            client.off("error", onError);
        };

        client.once("ready", onReady);
        client.once("error", onError);
    });
}

module.exports = {
    connectRedis,
    getRedis,
    getBullRedis,
};
