## Wave 4 — Phase D LIVE build (not enabled)

- `resolveForeignLiveRateSelection` + `applyPriceFloor` in `lib/pricingShadow.js`.
- Wired in `campaignCreditDeduction.js` (no-op while `PRICING_BASIS=did`).
- Runbook: `docs/RUNBOOK_PRICING_BASIS_FLIP.md`.
- Pool unchanged; golden must still match baseline.
