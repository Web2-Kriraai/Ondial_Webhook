# Meta Cloud API WhatsApp ingress on Ondial_Webhook

Canonical public URL:

- Live: `https://api.ondial.ai/api/webhook/whatsapp`
- Test: `https://dev-api.ondial.ai/api/webhook/whatsapp`

Register this URL in Meta Developer App → WhatsApp → Configuration → Webhook.
Subscribe to `messages` (and optionally message status fields).

## Behaviour

1. **GET** — Meta hub verification: `hub.mode=subscribe`, `hub.verify_token`, `hub.challenge`
   - Token must match `WHATSAPP_WEBHOOK_VERIFY_TOKEN` (required; no hardcoded default)
2. **POST** — Verify `X-Hub-Signature-256` with `WHATSAPP_APP_SECRET` (fail closed)
3. Return `200 { received: true }` after enqueue
4. Enqueue payload to BullMQ `whatsapp-meta-inbound` (consumed by Calling_system1)

## Env

| Variable | Role |
|----------|------|
| `WHATSAPP_APP_SECRET` | Meta App Secret for HMAC |
| `WHATSAPP_WEBHOOK_VERIFY_TOKEN` | hub.verify_token |
| `WHATSAPP_META_INBOUND_QUEUE_NAME` | Default `whatsapp-meta-inbound` (must match CS1) |
| `REDIS_URL` | Same Redis as Calling_system1 BullMQ consumer |

Outbound Meta sends use `WHATSAPP_API_TOKEN` / phone number ID on **Calling_system1** and Ondial — not this service.

## AiSensy sibling

AiSensy remains at `/api/webhook/aisensy` → `aisensy-inbound`. Providers are exclusive per user in Ondial Omni Channel.

## Full E2E

See Ondial `docs/WHATSAPP_META_SETUP_GUIDE.md` (account + env + webhook register), `docs/WHATSAPP_META_CROSS_REPO_E2E_PLAN.md`, and `docs/AISENSY_END_TO_END.md`.
