# AiSensy ingress on Ondial_Webhook

Canonical public URL:

- Live: `https://api.ondial.ai/api/webhook/aisensy`
- Test: `https://dev-api.ondial.ai/api/webhook/aisensy`

## Behaviour

1. Verify HMAC with `WEBHOOK_SECRET` (or `AISENSY_WEBHOOK_SECRET`)
2. Enqueue payload to BullMQ `aisensy-inbound` (CS1 backup consumer)
3. Return `200 { received: true }`
4. Async on this service:
   - marketing updates: `whatsappcampaignlogs` + STOP → `whatsappunsubscribes`
   - inbound chat → same STOP + slim AI relay as Meta (`whatsapp/processAisensyInbound.js`)

Duplicate `messageId`s are ignored so a CS1 queue consumer cannot send a second AI reply.

## Env

See [`.env.example`](../.env.example). Queue name must match CS1: `AISENSY_INBOUND_QUEUE_NAME=aisensy-inbound`.

Shared with Calling_system1:

- `WEBHOOK_SECRET` — same value as CS1 / Ondial / AiSensy dashboard HMAC
- Redis URL used by BullMQ (same Redis CS1 consumer)
- Dashboard webhook URL (test): `https://dev-api.ondial.ai/api/webhook/aisensy`

Session AI payload is built here: `{ task, phone, message, sessionId, campaignId, contactId, callId, analysisId, history }`.

## Full E2E docs

[Ondial `docs/AISENSY_END_TO_END.md`](../../Ondial/docs/AISENSY_END_TO_END.md)
