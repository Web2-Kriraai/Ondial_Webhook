/**
 * Local check: would CS1 isWithinBusinessHours allow dial for US DID campaign right now?
 * Mirrors Calling_system1/shared-lib/src/utils.js using Luxon if available, else Intl.
 */
const path = require("path");
const fs = require("fs");
const { MongoClient, ObjectId } = require("mongodb");

function loadEnv() {
  for (const p of [
    path.join(__dirname, "../../Ondial/.env"),
    path.join(__dirname, "../.env"),
  ]) {
    if (!fs.existsSync(p)) continue;
    for (const line of fs.readFileSync(p, "utf8").split(/\r?\n/)) {
      const m = line.match(/^([^#=]+)=(.*)$/);
      if (!m) continue;
      const k = m[1].trim();
      let v = m[2].trim().replace(/^["']|["']$/g, "");
      if (!process.env[k]) process.env[k] = v;
    }
  }
}

(async () => {
  loadEnv();
  // Import CS1 util via dynamic path
  const utilsPath = path.join(
    __dirname,
    "../../Calling_system1/shared-lib/src/utils.js"
  );
  const { isWithinBusinessHours } = await import("file:///" + utilsPath.replace(/\\/g, "/"));

  const client = new MongoClient(process.env.MONGODB_URI);
  await client.connect();
  const db = client.db();
  const camp = await db.collection("campaigns").findOne({
    _id: new ObjectId("6abf68bc9bdac890710e85fb"),
  });

  console.log({
    status: camp.status,
    isPaused: camp.isPaused,
    timezone: camp.timezone,
    callingHoursEnabled: camp.callingHours?.enabled,
    friday: camp.callingHours?.schedule?.friday,
  });

  const within = isWithinBusinessHours(camp);
  console.log("isWithinBusinessHours =>", within);

  const now = new Date();
  console.log("now EST", now.toLocaleString("en-US", { timeZone: "America/New_York" }));
  console.log("now IST", now.toLocaleString("en-IN", { timeZone: "Asia/Kolkata" }));

  // What CS1 would log
  if (String(camp.status) !== "active") {
    console.log("CS1 would SKIP: status !== active");
  } else if (!within) {
    console.log("CS1 would BLOCK: outside calling hours");
  } else {
    console.log("CS1 would ENQUEUE pending contacts");
  }

  await client.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
