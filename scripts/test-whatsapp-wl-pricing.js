// White-label WhatsApp price per message (WL-HOOK:wl-whatsapp-price) for Meta session replies
// sent by Ondial_Webhook (whatsappAiRelay / whatsappFollowupService -> sendSessionReply).
// Offline: in-memory db, Meta client stubbed.
const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { ObjectId } = require("mongodb");

{
  const file = path.join(__dirname, "..", "whatsapp", "metaClient.js");
  require.cache[require.resolve(file)] = {
    id: file,
    filename: file,
    loaded: true,
    exports: {
      buildMetaClientFromProfile: () => ({
        sendTextMessage: async () => ({ ok: true, messageId: `wamid.${new ObjectId()}` }),
      }),
      getProfileAccessToken: () => "token",
      getProfileBusinessNumberId: () => "bn",
    },
  };
}
const { sendWhatsappSessionReply } = require("../whatsapp/sendSessionReply");
const {
  getWhatsappSendCost,
  loadWlWhatsappPricing,
  wlTenantIdOf,
  __resetWlWhatsappPriceCache,
} = require("../whatsapp/whatsappCredits");

function createMemoryDb() {
  const store = new Map();
  let finds = 0;
  function col(name) {
    if (!store.has(name)) store.set(name, []);
    const rows = store.get(name);
    return {
      _rows: rows,
      async findOne(query) {
        finds += 1;
        return (
          rows.find((row) => {
            for (const [k, v] of Object.entries(query || {})) {
              if (k.includes(".")) {
                const [a, b] = k.split(".");
                if (String(row?.[a]?.[b]) !== String(v)) return false;
              } else if (k === "_id" || k === "userId") {
                if (String(row[k]) !== String(v)) return false;
              } else if (v && typeof v === "object" && v.$gte !== undefined) {
                if (!(Number(row[k]) >= Number(v.$gte))) return false;
              } else if (row[k] !== v && String(row[k]) !== String(v)) {
                return false;
              }
            }
            return true;
          }) || null
        );
      },
      async insertOne(doc) {
        const withId = { ...doc, _id: doc._id || new ObjectId() };
        rows.push(withId);
        return { insertedId: withId._id };
      },
      async findOneAndUpdate(filter, update) {
        const row = rows.find(
          (r) => String(r._id) === String(filter._id) && Number(r.credits) >= Number(filter.credits?.$gte ?? 0)
        );
        if (!row) return { value: null };
        for (const [k, v] of Object.entries(update.$inc || {})) row[k] = Number(row[k] || 0) + Number(v);
        if (update.$set) Object.assign(row, update.$set);
        return { value: row };
      },
      async updateOne() {
        return {};
      },
    };
  }
  return { collection: col, finds: () => finds };
}

function setup({ whatsappPricing } = {}) {
  const db = createMemoryDb();
  db.collection("systemsettings")._rows.push({
    key: "whatsapp_pricing",
    value: {
      enabled: true,
      platform: { templateMessageCredits: 1, sessionMessageCredits: 0.5 },
      ownAccount: { templateMessageCredits: 0.3, sessionMessageCredits: 0.2 },
    },
  });
  const tenantId = new ObjectId();
  db.collection("wl_tenants")._rows.push({
    _id: tenantId,
    slug: "acme",
    ...(whatsappPricing === undefined ? {} : { billing: { whatsappPricing } }),
  });
  return { db, tenantId };
}

async function addUser(db, extra = {}) {
  const _id = new ObjectId();
  db.collection("users")._rows.push({ _id, email: `${_id}@x.test`, credits: 10, omniChannelSettings: { whatsapp: { provider: "meta" } }, ...extra });
  return db.collection("users").findOne({ _id });
}
const credits = async (db, u) => Math.round((await db.collection("users").findOne({ _id: u._id })).credits * 1e6) / 1e6;
const send = (db, u, contactId, billingUser = null) =>
  sendWhatsappSessionReply(db, {
    user: u,
    billingUser,
    profile: { provider: "meta", usesPlatformAccount: false },
    phone: "919900000001",
    text: "hi",
    campaignId: "camp",
    contactId,
  });

test.beforeEach(() => __resetWlWhatsappPriceCache());

test("white-label customer: partner session price, once per 24h window per contact", async () => {
  const { db, tenantId } = setup({ whatsappPricing: { templateMessageCredits: 0.07, sessionMessageCredits: 0.03 } });
  const u = await addUser(db, { wlTenantId: tenantId });
  assert.equal((await send(db, u, "c1")).success, true);
  assert.equal(await credits(db, u), 9.97);
  assert.equal((await send(db, u, "c1")).success, true);
  assert.equal(await credits(db, u), 9.97, "second reply in the window is free");
  const rows = db.collection("credittransactions")._rows;
  assert.equal(rows.length, 1);
  assert.equal(rows[0].type, "whatsapp_session_deduction");
  assert.equal(rows[0].amount, -0.03);
});

test("white-label org: the paying wallet's tenant decides; owner's tenant is the fallback", async () => {
  const { db, tenantId } = setup({ whatsappPricing: { sessionMessageCredits: 0.04 } });
  const admin = await addUser(db, { wlTenantId: tenantId });
  const member = await addUser(db, { wlTenantId: tenantId, credits: 0, createdBy: String(admin._id) });
  assert.equal((await send(db, member, "c2", admin)).success, true);
  assert.equal(await credits(db, admin), 9.96);
  assert.equal(await getWhatsappSendCost(db, false, "session", { payer: { _id: new ObjectId() }, user: { wlTenantId: tenantId } }), 0.04);
});

test("partner price absent / 0 -> no charge and no ledger row, never the OnDial rate", async () => {
  for (const whatsappPricing of [undefined, { sessionMessageCredits: 0 }, { sessionMessageCredits: null }]) {
    __resetWlWhatsappPriceCache();
    const { db, tenantId } = setup({ whatsappPricing });
    const u = await addUser(db, { wlTenantId: tenantId, credits: 0 });
    const res = await send(db, u, "c3");
    assert.equal(res.success, true);
    assert.equal(await credits(db, u), 0);
    assert.equal(db.collection("credittransactions")._rows.length, 0);
  }
});

test("white-label wallet short of the partner price -> not sent", async () => {
  const { db, tenantId } = setup({ whatsappPricing: { sessionMessageCredits: 2 } });
  const u = await addUser(db, { wlTenantId: tenantId, credits: 1 });
  const res = await send(db, u, "c4");
  assert.equal(res.success, false);
  assert.equal(res.error, "insufficient_credits");
});

test("native user: OnDial global rate as before", async () => {
  const { db } = setup({ whatsappPricing: { sessionMessageCredits: 0.03 } });
  const u = await addUser(db);
  assert.equal((await send(db, u, "c5")).success, true);
  assert.equal(await credits(db, u), 9.8);
  assert.equal(await getWhatsappSendCost(db, true, "session"), 0.5);
  assert.equal(await getWhatsappSendCost(db, true, "session", { payer: u, user: u }), 0.5);
});

test("partner price cached for a minute per tenant", async () => {
  const { db, tenantId } = setup({ whatsappPricing: { sessionMessageCredits: 0.1 } });
  const now = Date.now();
  assert.equal((await loadWlWhatsappPricing(db, tenantId, { now })).sessionMessageCredits, 0.1);
  db.collection("wl_tenants")._rows[0].billing.whatsappPricing.sessionMessageCredits = 0.4;
  const before = db.finds();
  assert.equal((await loadWlWhatsappPricing(db, tenantId, { now: now + 30000 })).sessionMessageCredits, 0.1);
  assert.equal(db.finds(), before, "no query while cached");
  assert.equal((await loadWlWhatsappPricing(db, tenantId, { now: now + 61000 })).sessionMessageCredits, 0.4);
  assert.equal(wlTenantIdOf(null, { wlTenantId: tenantId }), String(tenantId));
  assert.equal(wlTenantIdOf({}, undefined), null);
});
