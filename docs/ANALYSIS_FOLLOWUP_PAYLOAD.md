# Analysis payload — `features_enabled` follow-up blocks

POST `/v1/analysis/call` body includes `payload.features_enabled`:

| Block | When present |
|-------|----------------|
| `is_followup_enabled` | Always when follow-up section is evaluated |
| `callback_scheduling` | Follow-up on + call channel |
| `email_followup` | Follow-up on + email channel + templates loaded |
| `whatsapp_followup` | Follow-up on + WhatsApp channel + templates loaded |
| `demo_booking` | Appointments/demos enabled on campaign |

When `is_followup_enabled: false`, only that flag is sent (other blocks stripped before POST).

## Builder (Ondial_Webhook)

- `lib/analysis_services/shared.js` — `buildBaseEnrichedFields` + `sanitizeAnalysisPayloadForV1Api`
- `lib/triggerCallAnalysis.js` — loads `emailTemplates` + `whatsappTemplates` from Mongo before build

WhatsApp templates resolve from `whatsapptemplates` then `platform_whatsapp_templates` (same as CS1).

## Examples

See `docs/analysis-payload-examples/*.json`. Enabled WhatsApp example shape:

```json
"whatsapp_followup": {
  "status": true,
  "templates": [
    {
      "id": "...",
      "title": "followup_call_summary_en",
      "description": "...",
      "body": "Hi {{1}}, ...",
      "campaign_name": "OnDial"
    }
  ]
}
```

Disabled: `{ "status": false }`
