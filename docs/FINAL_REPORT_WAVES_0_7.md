# Final report — Ondial master plan Waves 0–7

Generated after local implementation. **No remotes pushed. No production writes.**

## 1. Per-wave summary

| Wave | Repos / files (high level) | Tests | Pool P1–P9 |
|---|---|---|---|
| **0** | `ondial-plans/golden/pricing-golden.mjs` + baseline | golden harness | MATCHES baseline |
| **1** | Webhook: shadow, NANP, `pricing_shadow_log`; CS1: `isForeignBillingTarget` + worker shadow | shadow 9/9; CS1 foreign tests | MATCHES |
| **2** | Webhook: `callEconomics`, hangup + `call.cost` patch, import/recon/margin scripts | economics unit OK | MATCHES |
| **3** | Webhook sync/hash; SA align flag; worker floor flag; Ondial 6dp flag | sync seed parity OK | MATCHES |
| **4** | Webhook: LIVE dest/max selection (default `did`) + runbook + price floor log | basis tests OK | MATCHES |
| **5** | Webhook: `resolveBillingUser`, wallet shadow, E11000 winner refund, credit hold off; schemas billingKey; CS1 worker wallet | golden | MATCHES |
| **6** | Webhook: `AUTH_MODE_*` gates, X-API-Key accept, Telnyx prod fail-closed; Ondial SSE token opt-in | — | MATCHES |
| **7** | CS1 scheduler ObjectId cap + `PHASE_F_DEFERRED.md`; Webhook `feesV2` | concurrency `.cjs` OK | MATCHES |

Latest tip commits (local):
- Webhook `feat/ondial-master-plan-wave7` → `e760614`
- CS1 `feat/ondial-master-plan-wave7` → (concurrency rename follow-up)
- Ondial wave6 SSE → `7836cedd`
- Super-Admin wave5 schema (+ earlier wave3)

## 2. Plan claims CORRECTED / NOT FOUND (from Step 0)

See `Ondial_Webhook/docs/MASTER_PLAN_VERIFIED.md`. Highlights:
- Plan date vs Phase A commit date conflict → CORRECTED
- `destinationCountryIso` holds DID country → CORRECTED
- `1809` → US without NANP flag → CORRECTED (flag `NANP_ISLAND_PREFIXES`)
- Telnyx fail-closed was incomplete → FIXED in Wave 6 for missing public key in production
- SA `deductCallCredits` importers → NOT FOUND in-repo (documented Wave 3)

## 3. EXTERNAL (still unverified from code)

- Dial API headers / EXTERNAL dialer conversation POSTers
- Twilio/Telnyx Mission Control StatusCallback / webhook URLs
- Whether production has `TELNYX_PUBLIC_KEY` set
- Redis sharing across hosts
- Live Mongo top-10 callee countries (`scripts/proposed_matrix_rows.json` is stub until DB available)
- Old Super-Admin deploys calling `deductCallCredits`

## 4. Open business decisions for you

1. When to flip LIVE `PRICING_BASIS` (`destination` / `max_of_both`) after shadow review — runbook ready, default stays `did`
2. Which countries from `proposed_matrix_rows.json` to insert into `countryPricingV1`
3. Auth enforce dates per `AUTH_MODE_*` group (staging `log_only` examples provided)
4. Confirm wallet policy W1 (parent) after reviewing `billing_wallet_shadow_log`

## 5. Skipped / partial

- Full byte-for-byte overwrite of ESM `countryPricing.js` copies (Option B implemented as **seed parity sync + hash**, not destructive file replace — loaders differ per repo)
- Worker LIVE destination selection (webhook path built; worker still DID for live charge; shadow already runs)
- Credit hold dial-path wiring in CS1 dialer (module ready, `CREDIT_HOLD_ENABLED=0`; acquire not hooked into dial yet)
- Auto-insert of matrix rows / auto-charge Twilio recon (report-only as required)
- Top-10 callee aggregation from live CallLogs (needs Mongo)

## Flags default (production-safe)

All LIVE behavior flags OFF except observe: `PRICING_SHADOW_MODE=1`, `BILLING_WALLET_SHADOW=1`.  
`PRICING_BASIS=did`. `CALL_ECONOMICS_ENABLED=0`. `CREDIT_HOLD_ENABLED=0`. `AUTH_MODE_*=off`.
