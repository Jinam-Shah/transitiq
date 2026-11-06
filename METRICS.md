# METRICS

Every number here was **measured**, never estimated. Each entry has the date, the environment it ran in, and the exact command so anyone can reproduce it. Resume bullets quote this file only.

**Environment codes:** `LOCAL` = local Docker on the laptop, `LS` = LocalStack (not real AWS), `CI` = GitHub Actions runner, `PROD` = real hosted service (Neon, Render, Vercel).

| Date | Metric | Value | Environment | Command | Notes |
|---|---|---|---|---|---|
| _none yet_ | | | | | Phase 0 sets up the stack; measurement starts in Phase 1. |

## Sections to fill as phases complete

- Ingestion: rows and days of history in bronze, pipeline run time per day (Phase 2, 5)
- Data quality: quarantine rate, number of checks (Phase 3)
- dbt: model and test counts (Phase 4)
- Real time: ghost-trip rate, bunching findings, sampling coverage (Phase 6)
- ML: calibration per quantile, pinball loss vs schedule baseline (Phase 7)
- API: p95 latency, test counts (Phase 8, 12)
- Notifications sent, from `notification_log` (Phase 10)
- Ask TransitIQ evaluation accuracy (Phase 11)
