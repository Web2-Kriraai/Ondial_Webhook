/**
 * Pure helpers for webhook validate-companion owner scoping (DID reuse).
 * Run: node --test tests/inboundDocAnchorOwnerScope.pure.test.mjs
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

function buildPhoneOwnerAnd({ userId, configId }) {
    const ownerAnd = [];
    const uid = userId != null ? String(userId).trim() : '';
    const cfgId = configId != null ? String(configId).trim() : '';
    if (uid) {
        ownerAnd.push({ $or: [{ userId: uid }, { createdBy: uid }] });
    }
    if (cfgId) {
        ownerAnd.push({
            $or: [
                { config_id: cfgId },
                { configId: cfgId },
                { inboundConfigId: cfgId },
            ],
        });
    }
    return ownerAnd;
}

describe('validate companion phone fallback owner scope', () => {
    it('refuses phone-only fallback without userId or configId', () => {
        const ownerAnd = buildPhoneOwnerAnd({});
        assert.equal(ownerAnd.length, 0);
    });

    it('scopes by userId so User B cannot sync onto User A validate row', () => {
        const ownerAnd = buildPhoneOwnerAnd({
            userId: '507f1f77bcf86cd7994390bb',
            configId: '507f1f77bcf86cd7994390b1',
        });
        assert.ok(ownerAnd.some((c) => c.$or?.some((x) => x.userId === '507f1f77bcf86cd7994390bb')));
        assert.ok(ownerAnd.some((c) => c.$or?.some((x) => x.config_id === '507f1f77bcf86cd7994390b1')));
        assert.ok(!ownerAnd.some((c) => c.$or?.some((x) => x.userId === '507f1f77bcf86cd7994390aa')));
    });
});
