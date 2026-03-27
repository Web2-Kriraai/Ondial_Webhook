function safeSerialize(data) {
    if (data == null) return undefined;
    try {
        // Guard log size under heavy load.
        const str = JSON.stringify(data);
        if (str.length > 4000) {
            return `${str.slice(0, 4000)}...<truncated>`;
        }
        return JSON.parse(str);
    } catch (err) {
        return { serializationError: err.message };
    }
}

function log(level, message, data = null) {
    const payload = {
        ts: new Date().toISOString(),
        level: level.toUpperCase(),
        message,
    };

    const serializedData = safeSerialize(data);
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
    info: (message, data) => log("info", message, data),
    error: (message, data) => log("error", message, data),
    warn: (message, data) => log("warn", message, data),
};
