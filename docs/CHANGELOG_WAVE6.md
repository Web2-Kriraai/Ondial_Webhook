## Wave 6 — Phase A auth (log_only)

- `lib/authMode.js` + gates on SSE, mapping, twilio status, conversation, inbound, telnyx.
- `verifyIngressAuth` accepts CS1 `X-API-Key`.
- Telnyx: fail-closed in production when `TELNYX_PUBLIC_KEY` missing.
- Defaults: all `AUTH_MODE_*=off`. Staging example: `docs/AUTH_MODE_STAGING.example.env`.
