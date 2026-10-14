const assert = require("assert");
const path = require("path");
const { ObjectId } = require("mongodb");

// Stub Meta client so sendSessionReply can "send" offline.
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
  deductWhatsappCredits,
  hasBilledWhatsappSessionInWindow,
} = require("../whatsapp/whatsappCredits");

function createMemoryDb() {
  const store = new Map();
  function col(name) {
    if (!store.has(name)) store.set(name, []);
    const rows = store.get(name);
    return {
      _rows: rows,
      async findOne(query) {
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
          (r) =>
            String(r._id) === String(filter._id) &&
            Number(r.credits) >= Number(filter.credits?.$gte ?? 0)
        );
        if (!row) return { value: null };
        if (update.$inc) {
          for (const [k, v] of Object.entries(update.$inc)) {
            row[k] = Number(row[k] || 0) + Number(v);
          }
        }
        if (update.$set) Object.assign(row, update.$set);
        return { value: row };
      },
    };
  }
  return {
    collection: col,
  };
}

(async () => {
  const db = createMemoryDb();
  const userId = new ObjectId();
  db.collection("users")._rows.push({ _id: userId, email: "niya@gmail.com", credits: 10 });
  const user = { _id: userId, email: "niya@gmail.com" };

  const first = await deductWhatsappCredits(db, {
    user,
    cost: 0.5,
    kind: "session",
    senderMode: "platform",
    campaignId: "camp1",
    contactId: "ct1",
    messageId: "wamid.1",
    phone: "919979710905",
  });
  assert.equal(first.ok, true);
  assert.equal(first.amount, 0.5);
  assert.equal(await hasBilledWhatsappSessionInWindow(db, { user, contactId: "ct1" }), true);

  const second = await deductWhatsappCredits(db, {
    user,
    cost: 0.5,
    kind: "session",
    senderMode: "platform",
    campaignId: "camp1",
    contactId: "ct1",
    messageId: "wamid.2",
    phone: "919979710905",
  });
  assert.equal(second.windowFree, true);
  assert.equal(second.amount, 0);
  assert.equal((await db.collection("users").findOne({ _id: userId })).credits, 9.5);

  const otherContact = await deductWhatsappCredits(db, {
    user,
    cost: 0.5,
    kind: "session",
    senderMode: "platform",
    campaignId: "camp1",
    contactId: "ct2",
    messageId: "wamid.3",
    phone: "916353125194",
  });
  assert.equal(otherContact.amount, 0.5);
  assert.equal((await db.collection("users").findOne({ _id: userId })).credits, 9);

  console.log("ok: session AI reply billed once per 24h window");

  // Org user (creator) sends; org admin (billingUser) wallet is checked and debited.
  {
    const db2 = createMemoryDb();
    const adminId = new ObjectId();
    const creatorId = new ObjectId();
    const creator = {
      _id: creatorId,
      email: "member@org.test",
      role: "user",
      createdBy: String(adminId),
      credits: 0,
      omniChannelSettings: { whatsapp: { provider: "meta" } },
    };
    db2.collection("users")._rows.push({ ...creator });
    db2.collection("users")._rows.push({ _id: adminId, email: "admin@org.test", credits: 2 });
    const admin = await db2.collection("users").findOne({ _id: adminId });
    const profile = { provider: "meta", usesPlatformAccount: true };

    const sent = await sendWhatsappSessionReply(db2, {
      user: creator,
      billingUser: admin,
      profile,
      phone: "919900000001",
      text: "hi",
      campaignId: "campOrg",
      contactId: "ctOrg",
    });
    assert.equal(sent.success, true);
    assert.equal((await db2.collection("users").findOne({ _id: adminId })).credits, 1.5);
    assert.equal((await db2.collection("users").findOne({ _id: creatorId })).credits, 0);
    const tx = await db2.collection("credittransactions").findOne({ "reference.contactId": "ctOrg" });
    assert.equal(String(tx.userId), String(adminId));

    // Empty admin wallet blocks before send even though the creator object is unchanged.
    const broke = { _id: new ObjectId(), email: "broke@org.test", credits: 0 };
    const blocked = await sendWhatsappSessionReply(db2, {
      user: { ...creator, credits: 100 },
      billingUser: broke,
      profile,
      phone: "919900000002",
      text: "hi",
      contactId: "ctBlocked",
    });
    assert.equal(blocked.success, false);
    assert.equal(blocked.error, "insufficient_credits");

    // Without billingUser the creator wallet is used (backward compatible).
    const fallback = await sendWhatsappSessionReply(db2, {
      user: creator,
      profile,
      phone: "919900000003",
      text: "hi",
      contactId: "ctCreator",
    });
    assert.equal(fallback.success, false);
    assert.equal(fallback.error, "insufficient_credits");
    console.log("ok: WhatsApp session reply bills the org billing user");
  }
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
