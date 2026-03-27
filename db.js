const { MongoClient } = require("mongodb");

const MONGODB_URI = process.env.MONGODB_URI;

if (!MONGODB_URI) {
    throw new Error("MONGODB_URI is not defined in .env");
}

let client = null;
let db = null;

async function connectDB() {
    if (db) return db;

    client = new MongoClient(MONGODB_URI);
    await client.connect();
    db = client.db(); // uses the DB name from the URI ("ondial")

    console.log("[DB] Connected to MongoDB");
    return db;
}

function getDb() {
    if (!db) throw new Error("DB not connected. Call connectDB() first.");
    return db;
}

module.exports = { connectDB, getDb };
