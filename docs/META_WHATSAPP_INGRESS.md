# Meta Cloud API WhatsApp ingress on Ondial_Webhook

Canonical public URL:

- Live: `https://api.ondial.ai/api/webhook/whatsapp`
- Test: `https://dev-api.ondial.ai/api/webhook/whatsapp`

Register this URL in Meta Developer App → WhatsApp → Configuration → Webhook.
Subscribe at least to:

- `messages` (inbound + delivery statuses)
- `message_template_status_update` (template APPROVED / REJECTED / PAUSED → Mongo via local worker)

Optional: `message_template_quality_update`.

## Architecture (locked)

- **Redis is local to the DigitalOcean webhook host** (`REDIS_URL=redis://127.0.0.1:6379`).
- **Producer + consumer both run in Ondial_Webhook** (same PM2 process / same Redis).
- **Hostinger Calling_system1 does not consume** `whatsapp-meta-inbound`. Set `WHATSAPP_META_CONSUMER_ENABLED=false` (or omit) on Hostinger so the scheduler does not wait on empty local Redis for Meta jobs.
- Dialer / analysis remain on Hostinger; Meta webhook processing is owned by this service.

```text
Meta Cloud API → Ondial_Webhook (DO) → Redis localhost → Meta worker → Mongo (+ Graph for AI replies)
```

## Behaviour

1. **GET** — Meta hub verification: `hub.mode=subscribe`, `hub.verify_token`, `hub.challenge`
   - Token must match `WHATSAPP_WEBHOOK_VERIFY_TOKEN` (required; no hardcoded default)
2. **POST** — Verify `X-Hub-Signature-256` with `WHATSAPP_APP_SECRET` (fail closed)
3. Reject payloads whose `object` is not `whatsapp_business_account`
4. Enqueue payload to BullMQ `whatsapp-meta-inbound`
5. Local worker (`whatsapp/metaInboundWorker.js`) drains the queue:
   - template status → `platform_whatsapp_templates` / `whatsapptemplates`
   - delivery status → `contactprocessings.whatsappStatus`
   - inbound chat → STOP list + AI relay + Meta free-text send
6. Return `200 { received: true }` **only after** `await Queue.add` succeeds (Redis has acknowledged the job write). Queue/Redis failures return `503` so Meta retries — events are never ACKed then dropped.

Webhook job IDs are derived from the full payload hash. Meta retry deliveries are therefore coalesced while
the completed job is retained (one hour), preventing duplicate AI replies.

**Ordering invariant (do not regress):** `await enqueueMetaWhatsappInbound` → on throw `503` → else `200`. Same pattern as AiSensy ingress. `/health/slo` queue counts + `metaWhatsapp.worker` detect backlog / last job after successful enqueue; they are complementary, not a substitute for this ACK contract.

## Env (DigitalOcean webhook)

| Variable | Role |
|----------|------|
| `WHATSAPP_APP_SECRET` | Meta App Secret for HMAC |
| `WHATSAPP_WEBHOOK_VERIFY_TOKEN` | hub.verify_token |
| `WHATSAPP_META_INBOUND_QUEUE_NAME` | Default `whatsapp-meta-inbound` |
| `REDIS_URL` | Local Redis on DO (`redis://127.0.0.1:6379`) |
| `WHATSAPP_META_CONSUMER_ENABLED` | Default `true` on webhook — set `false` only to pause the worker |
| `MONGODB_URI` | Same Mongo as Ondial / CS1 |
| `WHATSAPP_API_TOKEN` | Platform Meta Graph token (AI session replies) |
| `WHATSAPP_PHONE_NUMBER_ID` | Platform phone number ID |
| `WHATSAPP_API_VERSION` | Optional, default `v21.0` |
| `WHATSAPP_AI_REPLY_URL` | AI next-message endpoint |
| `WHATSAPP_AI_REPLY_SECRET` | Optional bearer for AI URL |
| `WHATSAPP_AI_RELAY_ENABLED` | Optional global force-on for AI relay |
| `TOKEN_ENCRYPTION_KEY` | Decrypt own-account Meta tokens on profiles |

## Env (Hostinger CS1)

| Variable | Role |
|----------|------|
| `WHATSAPP_META_CONSUMER_ENABLED` | Must be `false` or unset — Meta ingress is owned by Ondial_Webhook |

## Failure behaviour

| Condition | Response |
|-----------|----------|
| Missing `WHATSAPP_APP_SECRET` | `503` — fail closed |
| Invalid signature | `401` |
| `object` ≠ `whatsapp_business_account` | `400` |
| Redis / BullMQ enqueue failure | `503 Queue unavailable` — Meta retries; event is **not** ACKed with 200 |
| Success | `200 { received: true }` after enqueue |

## Verify (ops checklist)

1. Deploy Ondial_Webhook with the Meta worker; `pm2 restart ondial-webhook-dev`.
2. Confirm `.env` has Mongo + Meta token + `REDIS_URL=redis://127.0.0.1:6379` + consumer enabled.
3. Meta Test / real SMS → logs: `[MetaWhatsApp] POST enqueued` then `[MetaWhatsAppInbound] job started/completed`.
4. Template APPROVED → Mongo `status: active` without manual sync.
5. `GET /health/slo` → `metaWhatsapp.waiting` not stuck; `metaWhatsapp.worker.running: true`.
6. On Hostinger: Meta consumer off; restart scheduler; confirm skip log.

See Ondial `docs/WHATSAPP_META_HOW_IT_WORKS.md` §5.1 for production gaps (window closed template re-engagement remains out of webhook scope).

## AiSensy sibling

AiSensy remains at `/api/webhook/aisensy` → `aisensy-inbound` (still consumed by Calling_system1 when Redis is shared for that queue). Providers are exclusive per user in Ondial Omni Channel.

## Full E2E

See Ondial `docs/WHATSAPP_META_SETUP_GUIDE.md` (account + env + webhook register), `docs/WHATSAPP_META_CROSS_REPO_E2E_PLAN.md`, and `docs/AISENSY_END_TO_END.md`.
