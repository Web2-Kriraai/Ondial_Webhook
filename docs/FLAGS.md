# Feature flags (Wave 0–3)

| Flag | Default | Scope | Purpose | Rollback |
|---|---|---|---|---|
| `PRICING_SHADOW_MODE` | `1` (on) | twilio/telnyx via providers list | Write `pricing_shadow_log` only; never changes charge | Set `0` |
| `PRICING_BASIS` | `did` | foreign only when LIVE later | Live $/min basis: `did` \| `destination` \| `max_of_both` | Keep `did` |
| `PRICING_BASIS_PROVIDERS` | `twilio,telnyx` | excludes pool | Which providers foreign pricing applies to | Remove provider or set empty |
| `PRICING_MISSING_POLICY` | `m2` | shadow / future LIVE | `m2` fallback+log; `m1` strict for LIVE dest | `m2` |
| `NANP_ISLAND_PREFIXES` | `1` (on) | phone→ISO | Enable `1809/1829/1849`→DO before `1`→US | `0` |
| `PRICING_SHADOW_TTL_DAYS` | `90` | Mongo TTL | Expire shadow docs | Raise / disable index manually |
| `ONDIAL_CREDIT_DEDUCTION_ENABLED` | unset=on | all | Existing kill switch | `0` |
| `CALL_ECONOMICS_ENABLED` | `0` | twilio/telnyx | Write `call_economics` observe rows; never changes customer charge | Keep `0` |
| `CALL_ECONOMICS_PROVIDERS` | `twilio,telnyx` | excludes pool | Providers that may write economics | Remove provider |
| `PRICING_HASH_CHECK` | `0` | webhook startup | Warn if seed SHA ≠ canonical (never crash) | `0` |
| `PRICING_MODULE_CANONICAL_HASH` | unset | webhook | Optional expected seed SHA override | unset |
| `SA_BILLING_ALIGN_PRODUCTION` | `0` | Super-Admin only | Floor seconds + last-bracket + 6dp wallet | Keep `0` |
| `WORKER_FLOOR_DURATION` | `0` | CS1 worker | Floor duration before brackets | Keep `0` |
| `COMPUTE_COST_ROUND_SIX` | `0` | Ondial `computeCallUsageCost` | Round cost to 6dp instead of 5dp | Keep `0` |
| `PRICE_FLOOR_ENABLED` | `0` | foreign LIVE | Compare customer charge to estimated provider cost | Keep `0` |
| `PRICE_FLOOR_ACTION` | `log` | when floor on | `log` only or `bump` charge to estimate | `log` |
| `BILLING_WALLET_SHADOW` | `1` (on) | all deduct paths | Log parent vs creator wallet choice; no debit change | `0` |
| `CREDIT_HOLD_ENABLED` | `0` | twilio/telnyx | Hold credits at dial; settle on hangup | Keep `0` |
| `CREDIT_HOLD_PROVIDERS` | `twilio,telnyx` | excludes pool | Hold scope | remove provider |
| `CREDIT_HOLD_TTL_SEC` | `7200` | holds | Orphan release TTL | raise |

Wave 6+ flags (not enabled yet): `AUTH_MODE_*`.

**LIVE note:** `PRICING_BASIS` stays `did`. Destination / max_of_both are implemented but OFF. See `docs/RUNBOOK_PRICING_BASIS_FLIP.md`.
