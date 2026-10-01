## Wave 3 — Phase B pricing unify (no LIVE price change)

- Canonical: `Ondial_Webhook/lib/countryPricing.js` (+ exported `DEFAULT_FALLBACK_ORDER`).
- `scripts/sync-pricing-module.js` verifies seed parity across four repos; writes hash file with `--apply`.
- `PRICING_HASH_CHECK` warn-only startup check.
- Flags default OFF: `SA_BILLING_ALIGN_PRODUCTION`, `WORKER_FLOOR_DURATION`, `COMPUTE_COST_ROUND_SIX`.
