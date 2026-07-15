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

## Full E2E docs

[Ondial `docs/AISENSY_END_TO_END.md`](../../Ondial/docs/AISENSY_END_TO_END.md)
