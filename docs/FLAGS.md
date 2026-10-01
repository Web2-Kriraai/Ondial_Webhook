# Feature flags (Wave 0–2)

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

Wave 3+ flags (not enabled yet): `SA_BILLING_ALIGN_PRODUCTION`, `WORKER_FLOOR_DURATION`, `COMPUTE_COST_ROUND_SIX`, `CREDIT_HOLD_*`, `AUTH_MODE_*`, `BILLING_WALLET_SHADOW`, `PRICE_FLOOR_*`, `PRICING_HASH_CHECK`.
