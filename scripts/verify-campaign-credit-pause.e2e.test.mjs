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

function createMemoryDb({ campaigns = [] } = {}) {
  const store = { campaigns: campaigns.map((c) => ({ ...c })) };

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
      if (val instanceof ObjectId) {
        if (String(doc[key]) !== String(val)) return false;
        continue;
      }
      if (doc[key] !== val) return false;
    }
    return true;
  }

  return {
    collection(name) {
      const rows = () => store[name] || [];
      return {
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
});
