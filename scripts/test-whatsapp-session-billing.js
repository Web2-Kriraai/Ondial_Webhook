const assert = require("assert");
const { ObjectId } = require("mongodb");
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
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
