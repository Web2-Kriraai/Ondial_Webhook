## Wave 2 — Phase C cost observe

- Added `lib/callEconomics.js` (observe-only; `CALL_ECONOMICS_ENABLED=0`).
- Hangup hook in `campaignCreditDeduction.js`; Telnyx `call.cost` actual-cost patch in `index.js`.
- Indexes via `db.js`; import + recon + margin report scripts under `scripts/`.
- Pool never writes `call_economics`. Customer charges unchanged.
