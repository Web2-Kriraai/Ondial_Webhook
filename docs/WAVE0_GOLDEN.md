# Wave 0 — Golden harness (outside repos)

Harness and baseline live **outside** the four GitHub repos:

- `C:\Users\ADMIN\Documents\ondial-plans\golden\pricing-golden.mjs`
- `C:\Users\ADMIN\Documents\ondial-plans\golden\golden-baseline.json`
- `C:\Users\ADMIN\Documents\ondial-plans\golden\golden-latest.json` (last run)

## Run

```bash
cd C:\Users\ADMIN\Documents\ondial-plans\golden
node pricing-golden.mjs --write-baseline
node pricing-golden.mjs --compare-baseline
```

## Wave 0 results (2026-10-01)

- Matrix cells: 1920
- Rate disagreements across 4 `countryPricing` copies: **0**
- Production charge disagreements (webhook floored vs worker floored vs Ondial 5dp): **0**
- Float duration 30.5 worker-without-floor vs floored: **240** cells differ (expected; documents Wave 3 `WORKER_FLOOR_DURATION`)
- Pool suite P1–P9: **all ok**

Gate for later waves: `--compare-baseline` must exit 0 and `poolAllOk` must stay true.
