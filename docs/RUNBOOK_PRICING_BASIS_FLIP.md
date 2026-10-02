# Runbook: Flip `PRICING_BASIS` (Wave 4 — build complete, LIVE still `did`)

**Do not flip in production until shadow data and matrix coverage are reviewed.**

## Current defaults (safe)

| Flag | Value |
|---|---|
| `PRICING_BASIS` | `did` |
| `PRICING_BASIS_PROVIDERS` | `twilio,telnyx` (excludes `pool`) |
| `PRICING_DESTINATION_RATE` | `country` (ISO matrix). Set `prefix` for live prefix cost × commission |
| `PRICING_MISSING_POLICY` | `m2` for shadow; set `m1` before LIVE destination |
| `PRICE_FLOOR_ENABLED` | `0` |
| `PRICE_FLOOR_ACTION` | `log` |

Pool / VoiceLink charges are **never** affected by this flip (`isForeignBillingTarget` gate).

## Pre-checks

1. **Shadow report** — run `node scripts/shadow_report.js` and review `deltaDestMinusDid` by provider × destIso.
2. **Matrix coverage** — review `scripts/proposed_matrix_rows.json` (AE, DO/`1809`/`1829`/`1849`, plus top callee countries). Insert approved rows into `countryPricingV1` **before** flip (manual / Super-Admin; this runbook does not auto-insert). For **prefix-live**, import Telnyx/Twilio cost cards into `provider_rate_cards` and set `providerCommissionV1` first; country Suggested Apply is optional for live charge.
3. **Missing-country** — set `PRICING_MISSING_POLICY=m1` so unknown destinations refuse instead of silently billing US/IN (still applies when no prefix and no country-max card).
4. **Pool golden** — `node ../ondial-plans/golden/pricing-golden.mjs --compare-baseline` still MATCHES.
5. **Canary user** — set `User.billingOverride.pricingBasis = "destination"` (or `max_of_both`) on one foreign-only test account; leave env `PRICING_BASIS=did`.

## Exact flag changes (global, after canary)

```bash
# Staging first
PRICING_MISSING_POLICY=m1
PRICING_BASIS=destination   # or max_of_both
# Prefix-live (1A + MAX fallback) — optional; default country keeps ISO matrix
PRICING_DESTINATION_RATE=prefix
# optional:
# PRICE_FLOOR_ENABLED=1
# PRICE_FLOOR_ACTION=log
```

Restart Ondial_Webhook (and CS1 worker if worker live-selection is also enabled).

### Prefix-live behavior (`PRICING_DESTINATION_RATE=prefix`)

1. Longest-prefix match on `provider_rate_cards` for the callee digits.
2. **Hit:** user $/min = `cost × (1 + commission%)`.
3. **Miss:** country **MAX** cost × same commission.
4. **No cards for that ISO:** fall through to country matrix / M1 refuse.

Country Suggested Apply remains useful for UI / matrix display but is **not required** for live charge in prefix mode.

## Rollback

```bash
PRICING_BASIS=did
# or clear User.billingOverride.pricingBasis on canary users
# soft rollback of prefix only:
PRICING_DESTINATION_RATE=country
```

Single env change; no migration, no data rewrite.

## Notes

- Brackets still use talk-time seconds; only `$/min` changes.
- `destinationCountryIso` on CallLogs historically stored the **DID** country; after destination basis, live rate uses callee ISO for selection but audit fields may still say DID unless updated in a follow-up.
- Prefix-live audit on CallLog / credit tx (only when prefix or country_max applied): `pricingRateSource`, and `matchedPrefix` if prefix hit. Commission % / ENV / provider cost are **not** duplicated on every call (live in `systemsettings` / `provider_rate_cards`).
- Shadow (`pricing_shadow_log`) stays observe-only; disable with `PRICING_SHADOW_MODE=0`. Prefix mode adds at most `prefixRateSource`, `ratePrefixLive`, `chargePrefixLive`, optional `matchedPrefix`.
- EXTERNAL: Twilio/Telnyx console StatusCallback URLs and dial API headers are out of scope for this flip.
