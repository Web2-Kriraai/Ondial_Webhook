# AiSensy ingress on Ondial_Webhook

Canonical public URL:

- Live: `https://api.ondial.ai/api/webhook/aisensy`
- Test: `https://dev-api.ondial.ai/api/webhook/aisensy`

## Behaviour

1. Verify HMAC with `WEBHOOK_SECRET` (or `AISENSY_WEBHOOK_SECRET`)
2. Return `200 { received: true }` immediately
3. Enqueue payload to BullMQ `aisensy-inbound` (consumed by Calling_system1)
4. Async marketing updates: `whatsappcampaignlogs` + STOP → `whatsappunsubscribes`

## Env

See [`.env.example`](../.env.example). Queue name must match CS1: `AISENSY_INBOUND_QUEUE_NAME=aisensy-inbound`.

Shared with Calling_system1:

- `WEBHOOK_SECRET` — same value as CS1 / Ondial / AiSensy dashboard HMAC
- Redis URL used by BullMQ (same Redis CS1 consumer)
- Dashboard webhook URL (test): `https://dev-api.ondial.ai/api/webhook/aisensy`

CS1 then calls `WHATSAPP_AI_REPLY_URL` (stub or real AI) for the next session message after a follow-up template was sent.

## Full E2E docs

[Ondial `docs/AISENSY_END_TO_END.md`](../../Ondial/docs/AISENSY_END_TO_END.md)
