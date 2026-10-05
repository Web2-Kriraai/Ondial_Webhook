const fs = require("fs");
const path = require("path");
const { MongoClient } = require("mongodb");

function loadEnv(f) {
  if (!fs.existsSync(f)) return;
  for (const line of fs.readFileSync(f, "utf8").split(/\r?\n/)) {
    const t = line.trim();
    if (!t || t.startsWith("#")) continue;
    const i = t.indexOf("=");
    if (i < 1) continue;
    let k = t.slice(0, i).trim();
    let v = t.slice(i + 1).trim();
    if (
      (v.startsWith('"') && v.endsWith('"')) ||
      (v.startsWith("'") && v.endsWith("'"))
    ) {
      v = v.slice(1, -1);
    }
    if (!process.env[k]) process.env[k] = v;
  }
}

loadEnv(path.join("C:/Users/ADMIN/Documents/GitHub/Ondial/.env"));
loadEnv(path.join("C:/Users/ADMIN/Documents/GitHub/Ondial_Webhook/.env"));

const email = "niya@gmail.com";
const apply = process.argv.includes("--apply");

(async () => {
  const client = new MongoClient(process.env.MONGODB_URI);
  await client.connect();
  const db = client.db();
  const u = await db.collection("users").findOne({
    email: { $regex: `^${email.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, $options: "i" },
  });
  if (!u) {
    console.log("USER_NOT_FOUND", email);
    await client.close();
    process.exit(1);
  }

  const before = {
    _id: String(u._id),
    email: u.email,
    billingOverride: u.billingOverride || null,
    hasOverride: Boolean(u.billingOverride?.enabled),
  };
  console.log("BEFORE:", JSON.stringify(before, null, 2));

  if (!u.billingOverride || (!u.billingOverride.enabled && !Object.keys(u.billingOverride || {}).length)) {
    console.log("No active billingOverride to remove.");
    await client.close();
    return;
  }

  if (!apply) {
    console.log("Dry-run. Re-run with --apply to clear billingOverride.");
    await client.close();
    return;
  }

  const res = await db.collection("users").updateOne(
    { _id: u._id },
    {
      $unset: { billingOverride: "" },
      $set: { updatedAt: new Date() },
    }
  );

  await db.collection("adminauditlogs").insertOne({
    action: "user.billing_override_cleared",
    resource: "users",
    adminEmail: "script:clear-niya-override",
    before: { email: u.email, billingOverride: u.billingOverride },
    after: { email: u.email, billingOverride: null },
    createdAt: new Date(),
    updatedAt: new Date(),
  }).catch(() => {});

  const after = await db.collection("users").findOne(
    { _id: u._id },
    { projection: { email: 1, billingOverride: 1 } }
  );
  console.log("UPDATE:", JSON.stringify({ matched: res.matchedCount, modified: res.modifiedCount, after }, null, 2));
  await client.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
