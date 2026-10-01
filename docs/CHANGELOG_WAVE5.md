## Wave 5 — Phase E billing correctness

- `lib/resolveBillingUser.js` (W1 parent + W3 setting; `BILLING_WALLET_SHADOW` log).
- E11000 refunds winner `userId` from existing credittransaction.
- Unique `billingKey` index: no non-unique fallback (fatal log).
- `lib/creditHold.js` foreign-only, `CREDIT_HOLD_ENABLED=0`.
- Ondial + Super-Admin schemas: `reference.billingKey`.
