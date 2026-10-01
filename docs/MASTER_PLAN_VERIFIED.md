# Master Plan Verification Report

**Source plan:** `C:\Users\ADMIN\Documents\ondial-plans\master_plan.md`  
**Verified against:** Calling_system1, Ondial_Webhook, Ondial, Ondial-Super-Admin (local trees)  
**Branch:** `feat/ondial-master-plan-wave0` (Ondial_Webhook)  
**Date of this verification:** 2026-10-01  

This file is the gate for Wave 0+. Claims are marked **CONFIRMED**, **CORRECTED**, or **NOT FOUND**.

---

## 0. Meta / plan hygiene

| Claim | Status | Evidence |
|---|---|---|
| Plan header date `2026-04-01` | **CORRECTED** | Plan document date was wrong relative to repo history. Twilio call-status auth was commented in commit `02b8ed8` on **2026-05-11** (`git log` on Ondial_Webhook). Latest `index.js` tip seen: `248651d` **2026-09-30**. Use commit dates, not the plan header date. |
| Four repos share Mongo via `MONGODB_URI` | **CONFIRMED** | Each repo `.env.example` documents `MONGODB_URI`. Live URI values not read (secrets). |

---

## 1. Provider / foreign gate

| Claim | Status | Evidence |
|---|---|---|
| Prefer `numberPolicySnapshot` over embedded `provider` (often `pool`) | **CONFIRMED** | `Calling_system1/shared-lib/src/policy.js:16-31` (`resolveEffectiveLineProvider`) |
| `isForeign` forces foreign routing | **CONFIRMED** | `policy.js:49-52` |
| `per_number` billing mode / twilio/telnyx → foreign | **CONFIRMED** | `policy.js:34-46`, `:60-65` |
| `isForeignBillingTarget` helper already exported | **NOT FOUND** | Must be **added** in Wave 1 (plan assumes new export). Closest: `resolveRoutingPolicy` + `isForeignProvider` (internal, not exported as the planned name). |
| Never trust `users.phoneNumbers[].provider` alone | **CONFIRMED** | Comment at `policy.js:19-21` |

---

## 2. Billing country = DID (not callee)

| Claim | Status | Evidence |
|---|---|---|
| Variable `destinationCountryIso` holds **DID/line** country today | **CORRECTED** (naming) | Webhook sets it from `resolveCountryBillingPhone` which prefers `campaign.selectedPhoneNumber` (`countryFromPhone.js:110-115`) then `countryIsoFromPhone` (`campaignCreditDeduction.js:299-301`). Worker: `campaign?.selectedPhoneNumber \|\| contact.mobileNumber \|\| contact.phone` then `countryIsoFromPhone` (`worker.js:3587-3592`). Name is misleading; value is line-based when DID present. |
| Comment explicitly says not the other party | **CONFIRMED** | `campaignCreditDeduction.js:293-296` |
| Callee available via `extractDestinationPhone` | **CONFIRMED** | `countryFromPhone.js:52-68` keys include `to_number`, `phone_number`, `mobileNumber`, `to`, etc. Does **not** yet read nested Twilio `To`/`Called` or Telnyx `payload.to` (Wave 1 adds `resolveCalleeBillingPhone`). |

---

## 3. `countryIsoFromPhone` / NANP / AE

| Claim | Status | Evidence |
|---|---|---|
| Prefix `971` → `AE` | **CONFIRMED** | Webhook `countryFromPhone.js:2`; Ondial `lib/utils/countryFromPhone.js:9`; CS1 `shared-lib/src/countryPricing.js:598` |
| Prefixes `1809`/`1829`/`1849` (DO) | **NOT FOUND** | Only `["1","US"]` at end of list. `+1809…` matches `1` → **US**. |
| Super-Admin has `countryIsoFromPhone` | **NOT FOUND** | No `countryFromPhone` file under Super-Admin. |

---

## 4. Customer charge formula / brackets

| Claim | Status | Evidence |
|---|---|---|
| Fallback brackets 25/50/75/100% | **CONFIRMED** | `campaignCreditDeduction.js:36-41`; worker `worker.js:3667-3672`; Ondial `computeCallUsageCost.js:5-10`; SA `callBillingBrackets.js:3-8` |
| Webhook floors duration before deduct | **CONFIRMED** | `campaignCreditDeduction.js:375` then `computeCost` `:172-183` |
| Webhook `roundSix` (6 dp) | **CONFIRMED** | `campaignCreditDeduction.js:43-44`, `:183` |
| Worker cost `toFixed(6)` | **CONFIRMED** | `worker.js:3687` |
| Ondial `computeCallUsageCost` uses 5 dp | **CONFIRMED** | `computeCallUsageCost.js:336` (`Math.round(cost * 100000) / 100000`) |
| SA `Math.ceil` duration | **CONFIRMED** | `callBillingCost.js:29-31` |
| SA unmatched bracket → 0% | **CONFIRMED** | `callBillingBrackets.js:77-85` (`return 0`) |
| SA wallet `Math.ceil` to whole credits | **CONFIRMED** | `deductCallCredits.js:56-61` |
| SA `deductCallCredits` used in production | **NOT FOUND** (importers) | Only definition at `lib/billing/deductCallCredits.js:24`; no static imports found. EXTERNAL: dynamic/old deploys. |
| Dial floor `0.065` | **CONFIRMED** | `Calling_system1/shared-lib/src/campaignCreditPause.js:4` |
| No balance hold at dial | **CONFIRMED** | Only `hasEnoughCreditsForDial` / `MIN_CREDITS_FOR_DIAL` gate; no hold collection FOUND. |

---

## 5. Wallet race

| Claim | Status | Evidence |
|---|---|---|
| Webhook walks up to parent when `role === "user"` and `createdBy` | **CONFIRMED** | `campaignCreditDeduction.js:542-548` |
| Worker bills `campaign.createdBy` (no parent walk in billing) | **CONFIRMED** | Worker loads user by `campaign.createdBy` email for billing (~3461+); pause helper documents creator wallet (`campaignCreditPause.js:89-92`) |
| Idempotency via `reference.billingKey` | **CONFIRMED** | Key format `${billingId}:call:${callId}` in `campaignCreditDeduction.js:408`; unique partial index in `db.js:360-372` |
| Non-unique fallback on duplicate key at index create | **CONFIRMED** | `db.js:374-388` creates `credittx_type_billingKey_nonunique` |

---

## 6. Telnyx cost fields / Twilio zero duration

| Claim | Status | Evidence |
|---|---|---|
| `call.cost` stores `telnyx.billed_duration_secs` and `telnyx.total_cost` | **CONFIRMED** | `index.js:1183-1191` |
| Those fields not used for customer charge | **CONFIRMED** | Charge path uses talk duration + matrix; cost fields only set on doc |
| Twilio `dur <= 0` → `skip_no_duration` | **CONFIRMED** | `twilioCallBilling.js:122-124` |
| Twilio recording list price in code | **NOT FOUND** | UNVERIFIED / EXTERNAL |

---

## 7. Webhook auth (Phase A citations)

| Endpoint | Plan line | Status | Evidence |
|---|---|---|---|
| `GET /api/v1/sse/listen` | ~427 | **CONFIRMED** | `index.js:427`; no auth; empty `campaignId` does not filter all events (`:434-448` only filters when both ids present) |
| `POST /api/outbound-call-mapping` | ~478 | **CONFIRMED** | `index.js:478-481` auth commented |
| `POST /twilio/call-status` | ~613 | **CONFIRMED** | `index.js:613-616` auth commented; disable commit `02b8ed8` **2026-05-11** “fix issue” |
| `POST /telnyx/webhooks` | ~1442 | **CONFIRMED** | `index.js:1442` |
| `/hangup` auth active | — | **CONFIRMED** | `rejectUnauthorizedControlRequest` `index.js:1449-1454` |
| Prod Telnyx require verify | — | **CORRECTED** (nuance) | `isTelnyxSignatureRequired()` returns true in production (`telnyxWebhookParse.js:52-58`), **but** if `TELNYX_PUBLIC_KEY` empty, `verifyTelnyxWebhookSignature` returns `{ ok: true, skipped: true }` (`:91-98`) — fail-closed for missing key is **commented out** (`:94-97`). Production without key still accepts. |
| CS1 mapping sends `X-API-Key` | — | **CONFIRMED** (prior audit) | Worker `postProviderMapping`; `verifyIngressAuth` does not accept `X-API-Key` (`index.js:193+`) |

---

## 8. countryPricing copies / seed

| Claim | Status | Evidence |
|---|---|---|
| Four copies, different hashes | **CONFIRMED** | Prior Step 1 SHA prefixes differ |
| Seed countries IN US GB CA AU DE FR MY | **CONFIRMED** | Each `countryPricing.js` seed |
| Seed rates numerically aligned | **CONFIRMED** | Phase B Step 1 |
| Webhook missing `DEFAULT_FALLBACK_ORDER` export | **CONFIRMED** | `module.exports` at `countryPricing.js:562-578` has no `DEFAULT_FALLBACK_ORDER` |
| Webhook seed not `Object.freeze` | **CONFIRMED** | `DEFAULT_COUNTRY_PRICING = {` at `:49` (no freeze) |
| Canonical choice: Ondial_Webhook | **DECISION** (accepted) | Pre-filled decision #1 in implementation prompt |

---

## 9. Concurrency (Phase F)

| Claim | Status | Evidence |
|---|---|---|
| Redis Lua acquire campaign+user(+number scope) | **CONFIRMED** | `shared-lib/src/concurrency.js` `ConcurrencyGuard.acquireSlot` |
| `config.concurrency.globalMax = 5000` unused | **CONFIRMED** | Defined `config.js:64-66`; no other reads FOUND |
| Purchased cap skips non-email `createdBy` | **CONFIRMED** | `scheduler.js` filters emails with `@` (~1194-1201 in prior audit) |
| Platform VoiceLink global Redis counter | **NOT FOUND** | Only account/campaign/number keys |

---

## 10. EXTERNAL (still unverifiable from code)

- Live Mongo `systemsettings.countryPricingV1` / `callBillingBracketsV1` document contents  
- Dial API hosts behind `CALL_API_*` (conversation/mapping auth headers)  
- Twilio console StatusCallback URL and recording/storage prices  
- Telnyx Mission Control `webhook_event_url` and whether prod has `TELNYX_PUBLIC_KEY` set  
- Whether scheduler and worker share one Redis (`REDIS_HOST`) on each PM2 host  
- Whether SA `deductCallCredits` is invoked by any undeployed/dynamic path  

---

## 11. Implementation implications (locked for waves)

1. Add `isForeignBillingTarget` — does not exist yet.  
2. Rename awareness: keep storing DID ISO in existing fields for live charge; shadow log must use separate `didIso` / `destIso`.  
3. NANP island prefixes must be added **before** `"1"` and behind `NANP_ISLAND_PREFIXES`.  
4. Telnyx fail-closed when key missing requires uncommenting/fixing `telnyxWebhookParse.js:94-97` (Wave 6).  
5. Auth enforce blocked until `verifyIngressAuth` accepts CS1 `X-API-Key` (Wave 6 builds accept + log_only only).  
6. Pool regression suite is the hard gate between waves.

---

## Gate

**Step 0 complete.** Proceed to Wave 0 (golden harness outside repos) only after this file is committed on the wave branch.
