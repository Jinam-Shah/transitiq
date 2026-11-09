# METRICS

Every number here was **measured**, never estimated. Each entry has the date, the environment it ran in, and the exact command so anyone can reproduce it. Resume bullets quote this file only.

**Environment codes:** `LOCAL` = Jinam's Windows laptop (Python 3.12, home internet), `LS` = LocalStack 2026.3.0 in Docker Desktop on that laptop (**not real AWS**), `CI` = GitHub Actions runner, `PROD` = real hosted service (Neon, Render, Vercel).

## Ingestion (Phase 2)

Backfill of 2025-11-01 to 2026-10-31 (365 calendar days) into `data/bronze/`.
Command: `transitiq-ingest backfill --start 2025-11-01 --end 2026-10-31`

| Date | Source | Files | Rows | Size (MB) | Run time (s) | Env | Notes |
|---|---|---|---|---|---|---|---|
| 2026-11-06 | `lamp_subway_otp` (MBTA LAMP subway on-time performance) | 364 | 14,529,395 | 475.4 | 28.2 | LOCAL | LAMP's index listed 364 service dates in the 365-day range. 1 checkpoint write failed on a Windows file lock (data was stored; fixed in fsutil.atomic_replace) |
| 2026-11-06 | `lamp_alerts` (alert versions, 2019 onward) | 1 | 5,171,976 | 133.6 | 3.19 | LOCAL | Single snapshot of the full history file |
| 2026-11-06 | `gtfs_static` (all GTFS .txt files) | 1 | 3,429,594 | 22.8 | 2.34 | LOCAL | Current feed |
| 2026-11-06 | `open_meteo_hourly` (Boston) | 12 | 8,760 | 0.5 | 8.39 | LOCAL | 12 monthly files = 365 × 24 hours |
| 2026-11-06 | `mbta_v3_alerts` (live snapshot) | 1 | 183 | 0.4 | 0.36 | LOCAL | |
| 2026-11-06 | `mbta_v3_facilities` (elevators + escalators) | 1 | 420 | 1.3 | 0.55 | LOCAL | |
| | **Total** | **380** | **23,140,328** | **634.0** | **43.03** | LOCAL | Sum of the six runs above |

## AWS ingestion design on LocalStack (Phases 1–2)

Command: `python scripts/e2e_localstack.py` after `terraform -chdir=infra/terraform apply` (33 resources).

| Date | Metric | Value | Env | Notes |
|---|---|---|---|---|
| 2026-11-06 | End-to-end checks passed | 18 / 18 | LS | IAM least-privilege (static), heartbeat flow, fixture split, idempotent replay, live V3 Lambda, empty DLQ |
| 2026-11-06 | Ingest Lambda invoke time (heartbeat, first call) | 3.0 s | LS | Includes LocalStack cold start |
| 2026-11-06 | Bronze → silver latency, heartbeat file | 2.05 s | LS | S3 event → validate Lambda |
| 2026-11-06 | Bronze → silver latency, 7-line fixture | 1.0 s | LS | |
| 2026-11-06 | Live MBTA V3 alerts via Lambda | 186 alerts in 0.42 s | LS | Lambda called api-v3.mbta.com with an API key |
| 2026-11-06 | Quarantine rate on the test fixture | 0.5714 (4 of 7) | LS | **Designed test data, not a real-data quality metric.** Real quarantine rates are measured in Phase 3 |

## Sections to fill as phases complete

- Data quality: real quarantine rate and number of checks on silver (Phase 3)
- dbt: model and test counts (Phase 4)
- Pipeline run time per day, end to end (Phase 5)
- Real time: ghost-trip rate, bunching findings, sampling coverage (Phase 6)
- ML: calibration per quantile, pinball loss vs schedule baseline (Phase 7)
- API: p95 latency, test counts (Phase 8, 12)
- Notifications sent, from `notification_log` (Phase 10)
- Ask TransitIQ evaluation accuracy (Phase 11)
