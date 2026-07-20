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

## Outbound WhatsApp template shape (request)

```json
"whatsapp_followup": {
  "status": true,
  "templates": [
    {
      "id": "6a5de37a299df33acca440fa",
      "title": "shopsphere_followup_en",
      "description": "Hi {{1}}, ... {{2}}% ...",
      "campaign_name": "shopsphere_followup_en_a440fa",
      "variable_count": 4,
      "sample_values": {
        "{{1}}": "Rohan Patel",
        "{{2}}": "40",
        "{{3}}": "Roshni",
        "{{4}}": "ShopSphere"
      }
    }
  ]
}
```

Notes:
- No `body` field on WhatsApp templates (description holds template text).
- `sample_values` / `variable_count` help the analysis API fill `whatsapp_draft.template_params`.

Disabled: `{ "status": false }`

## Expected analysis response (`whatsapp_draft`)

Scheduler / send path (Calling_system1) prefers these fields when present:

```json
"next_action": {
  "type": "WhatsApp",
  "whatsapp_draft": {
    "user_mobile": "+916353125194",
    "selected_whatsapp_template_id": "6a5de37a299df33acca440fa",
    "selected_whatsapp_template": "shopsphere_followup_en",
    "selected_whatsapp_campaign_name": "shopsphere_followup_en_a440fa",
    "template_variable_count": 4,
    "template_params": ["Kaushik", "40", "Roshni", "ShopSphere"],
    "sample_values": {
      "{{1}}": "Kaushik",
      "{{2}}": "40",
      "{{3}}": "Roshni",
      "{{4}}": "ShopSphere"
    },
    "message": "Hi Kaushik, ... reduce stock-outs by up to 40%. ..."
  }
}
```

`template_params` is required for reliable AiSensy send. Webhook persists the full `analysis_data` (including `whatsapp_draft`) into `call_analysis` as returned by the API.
