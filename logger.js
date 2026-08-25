function safeSerialize(data, maxChars = 4000) {
    if (data == null) return undefined;
    try {
        const str = JSON.stringify(data);
        const limit = Number.isFinite(maxChars) && maxChars > 0 ? maxChars : 4000;
        if (str.length > limit) {
            return `${str.slice(0, limit)}...<truncated>`;
        }
        return JSON.parse(str);
    } catch (err) {
        return { serializationError: err.message };
    }
}

function log(level, message, data = null, options = {}) {
    const payload = {
        ts: new Date().toISOString(),
        level: level.toUpperCase(),
        message,
    };

    const serializedData = safeSerialize(data, options.maxChars);
    if (serializedData !== undefined) {
        payload.data = serializedData;
    }

    const line = JSON.stringify(payload);
    if (level === "error") {
        console.error(line);
    } else if (level === "warn") {
        console.warn(line);
    } else {
        console.log(line);
    }
}

module.exports = {
    debug: (message, data, options) => log("debug", message, data, options),
    info: (message, data, options) => log("info", message, data, options),
    error: (message, data, options) => log("error", message, data, options),
    warn: (message, data, options) => log("warn", message, data, options),
};
