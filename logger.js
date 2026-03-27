const fs = require("fs");
const path = require("path");

const LOG_FILE = path.join(__dirname, "webhook.log");

function log(level, message, data = null) {
    const timestamp = new Date().toISOString();
    let entry = `[${timestamp}] [${level.toUpperCase()}] ${message}`;

    if (data) {
        entry += `\n  Data: ${JSON.stringify(data, null, 2)}`;
    }

    entry += "\n" + "-".repeat(80) + "\n";

    // Print to console
    console.log(entry);

    // Append to log file
    fs.appendFile(LOG_FILE, entry, (err) => {
        if (err) console.error("Failed to write log:", err);
    });
}

module.exports = {
    info: (message, data) => log("info", message, data),
    error: (message, data) => log("error", message, data),
    warn: (message, data) => log("warn", message, data),
};
