/**
 * Webhook path: pause campaigns on insufficient_credits outcome helper.
 *
 * Run from Ondial_Webhook:
 *   node --test scripts/verify-campaign-credit-pause.e2e.test.mjs
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ObjectId } from 'mongodb';
import { createRequire } from 'module';

const require = createRequire(import.meta.url);
const {
  pauseActiveCampaignsForInsufficientCredits,
} = require('../lib/pauseCampaignsForInsufficientCredits.js');

function createMemoryDb({ campaigns = [], users = [], systemsettings = [] } = {}) {
  const store = {
    campaigns: campaigns.map((c) => ({ ...c })),
    users: users.map((u) => ({ ...u })),
    systemsettings: systemsettings.map((s) => ({ ...s })),
  };

  // Mongo-like typed equality: ObjectId only equals ObjectId, string only equals string.
  function sameTyped(a, b) {
    const aOid = a instanceof ObjectId;
    const bOid = b instanceof ObjectId;
    if (aOid || bOid) return aOid && bOid && String(a) === String(b);
    return a === b;
  }

  function getPath(doc, key) {
    return key.split('.').reduce((v, k) => (v == null ? undefined : v[k]), doc);
  }

  function matchesQuery(doc, query) {
    for (const [key, val] of Object.entries(query)) {
      if (key === '$or') {
        if (!val.some((clause) => matchesQuery(doc, clause))) return false;
        continue;
      }
      if (key === 'archive' && val && typeof val === 'object' && '$ne' in val) {
        if (doc.archive === val.$ne) return false;
        continue;
      }
      if (val && typeof val === 'object' && Array.isArray(val.$in)) {
        if (!val.$in.some((x) => sameTyped(doc[key], x))) return false;
        continue;
      }
      if (val instanceof ObjectId) {
        if (String(doc[key]) !== String(val)) return false;
        continue;
      }
      if (getPath(doc, key) !== val) return false;
    }
    return true;
  }

  return {
    collection(name) {
      const rows = () => store[name] || [];
      return {
        async findOne(query) {
          return rows().find((d) => matchesQuery(d, query)) || null;
        },
        find(query) {
          const matched = rows().filter((d) => matchesQuery(d, query));
          return {
            project() {
              return {
                async toArray() {
                  return matched.map((d) => ({ ...d }));
                },
              };
            },
            async toArray() {
              return matched.map((d) => ({ ...d }));
            },
          };
        },
        async updateOne(filter, update) {
          const idx = rows().findIndex((d) => matchesQuery(d, filter));
          if (idx < 0) return { matchedCount: 0, modifiedCount: 0 };
          if (update.$set) Object.assign(rows()[idx], update.$set);
          return { matchedCount: 1, modifiedCount: 1 };
        },
      };
    },
    _store: store,
  };
}

describe('E2E: Ondial_Webhook pause on insufficient credits', () => {
  it('pauses active campaigns for billed user', async () => {
    const userId = new ObjectId();
    const campaignId = new ObjectId();
    const db = createMemoryDb({
      campaigns: [
        {
          _id: campaignId,
          createdBy: 'webhook-user@ondial.test',
          userId,
          status: 'active',
          concurrentCalls: 4,
        },
      ],
    });

    const result = await pauseActiveCampaignsForInsufficientCredits(db, {
      user: { _id: userId, email: 'webhook-user@ondial.test', credits: 0 },
    });

    assert.equal(result.pausedCount, 1);
    const row = db._store.campaigns[0];
    assert.equal(row.status, 'paused');
    assert.equal(row.pausedForInsufficientCredits, true);
    assert.equal(row.concurrentCalls, 0);
    assert.equal(row.concurrentCallsBeforePause, 4);
  });

  it('org-scoped: admin wallet pause covers org users (ObjectId + string ids/createdBy)', async () => {
    const adminId = new ObjectId();
    const memberA = new ObjectId();
    const memberB = new ObjectId();
    const outsider = new ObjectId();
    const db = createMemoryDb({
      users: [
        { _id: adminId, email: 'admin@org.test', role: 'admin' },
        { _id: memberA, email: 'a@org.test', role: 'user', createdBy: adminId },
        { _id: memberB, email: 'b@org.test', role: 'user', createdBy: String(adminId) },
        { _id: outsider, email: 'x@else.test', role: 'user', createdBy: String(new ObjectId()) },
      ],
      campaigns: [
        { _id: new ObjectId(), name: 'admin', userId: adminId, status: 'active', concurrentCalls: 1 },
        { _id: new ObjectId(), name: 'a-by-email', createdBy: 'a@org.test', status: 'active', concurrentCalls: 2 },
        { _id: new ObjectId(), name: 'b-by-string-id', userId: String(memberB), status: 'active', concurrentCalls: 3 },
        { _id: new ObjectId(), name: 'outsider', userId: outsider, createdBy: 'x@else.test', status: 'active', concurrentCalls: 1 },
        { _id: new ObjectId(), name: 'archived', userId: memberA, status: 'active', archive: true },
      ],
    });

    const result = await pauseActiveCampaignsForInsufficientCredits(db, {
      user: { _id: adminId, email: 'admin@org.test', role: 'admin', credits: 0 },
    });

    assert.equal(result.pausedCount, 3);
    const byName = Object.fromEntries(db._store.campaigns.map((c) => [c.name, c.status]));
    assert.deepEqual(byName, {
      admin: 'paused',
      'a-by-email': 'paused',
      'b-by-string-id': 'paused',
      outsider: 'active',
      archived: 'active',
    });
  });

  it('creator mode: only the billed user is paused', async () => {
    const adminId = new ObjectId();
    const memberId = new ObjectId();
    const db = createMemoryDb({
      users: [
        { _id: adminId, email: 'admin@org.test', role: 'admin' },
        { _id: memberId, email: 'm@org.test', role: 'user', createdBy: String(adminId) },
      ],
      systemsettings: [
        { key: 'billingWalletV1', value: { orgUserId: String(adminId), mode: 'creator' } },
      ],
      campaigns: [
        { _id: new ObjectId(), name: 'admin', userId: adminId, status: 'active' },
        { _id: new ObjectId(), name: 'member', userId: memberId, status: 'active' },
      ],
    });

    // Admin wallet (creator mode) → admin's own campaigns only.
    let result = await pauseActiveCampaignsForInsufficientCredits(db, {
      user: { _id: adminId, email: 'admin@org.test', role: 'admin' },
    });
    assert.equal(result.pausedCount, 1);
    assert.equal(db._store.campaigns.find((c) => c.name === 'member').status, 'active');

    // Org user billed to own wallet (creator mode) → that user only.
    result = await pauseActiveCampaignsForInsufficientCredits(db, {
      user: { _id: memberId, email: 'm@org.test', role: 'user', createdBy: String(adminId) },
    });
    assert.equal(result.pausedCount, 1);
    assert.equal(db._store.campaigns.find((c) => c.name === 'member').status, 'paused');
  });
});
