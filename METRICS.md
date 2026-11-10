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

## Silver layer (Phase 3)

Command: `docker compose run --rm pipelines transitiq-silver build`, then `transitiq-silver report`. Env `LOCAL-DOCKER` = the `pipelines` container (python:3.12-slim) in Docker Desktop on Jinam's laptop. Input: the Phase 2 bronze backfill (2025-11-01 to 2026-10-31).

| Date | Table | Rows in | Valid (Iceberg) | Quarantined | Rate | Top reasons | Run time (s) | Env |
|---|---|---|---|---|---|---|---|---|
| 2026-11-09 | `subway_events` (364 daily partitions) | 14,529,395 | 14,515,547 | 13,848 | 0.095% | duplicate_event 13,704 · implausible_travel_time 81 · implausible_dwell_time 63 | 123.51 | LOCAL-DOCKER |
| 2026-11-09 | `weather_hourly` (12 months) | 8,760 | 8,760 | 0 | 0% | | 1.48 | LOCAL-DOCKER |
| 2026-11-09 | `facilities` | 420 | 420 | 0 | 0% | | 0.39 | LOCAL-DOCKER |
| 2026-11-09 | `gtfs_routes` / `stops` / `trips` | 404 / 10,291 / 112,117 | same | 0 | 0% | | 2.95 / 0.36 / 1.91 | LOCAL-DOCKER |
| 2026-11-09 | `gtfs_stop_times` | 2,858,761 | 2,858,761 | 0 | 0% | | 21.0 | LOCAL-DOCKER |
| 2026-11-09 | `gtfs_calendar` / `calendar_dates` | 146 / 133 | same | 0 | 0% | | 0.18 / 0.16 | LOCAL-DOCKER |
| 2026-11-09 | `alerts` (versions since 2018) | 5,171,976 | 5,171,906 | 70 | 0.001% | end_before_start 70 | 117.4 | LOCAL-DOCKER |
| 2026-11-09 | **All silver tables** | **22,692,403** | | **13,918** | **0.061%** | | | LOCAL-DOCKER |

Tests: 111 passed inside the container (`docker compose run --rm pipelines pytest`, 2026-11-09). First alerts run failed on an intermittent DuckDB INTERNAL error; fixed in D-013 and re-run above.

## Gold layer (Phase 4)

| Date | Metric | Value | Env | Command |
|---|---|---|---|---|
| 2026-11-09 | dbt project size | 18 models (12 gold tables, 6 staging views), 57 data tests, 1 seed, 10 sources | LOCAL-DOCKER | `dbt build` output |
| 2026-11-09 | `dbt build` on the full year (2025-11-01 to 2026-10-31) | PASS=76, WARN=0, ERROR=0 in 161.59 s | LOCAL-DOCKER | `docker compose run --rm pipelines dbt build` |
| 2026-11-09 | Slowest models | fact_headway 73.97 s, mart_stop_reliability 75.48 s, fact_travel_time 53.72 s | LOCAL-DOCKER | same run |
| 2026-11-09 | Source freshness (silver.subway_events) | PASS | LOCAL-DOCKER | `docker compose run --rm pipelines dbt source freshness` |
| 2026-11-09 | dbt docs published | https://jinam-shah.github.io/transitiq/ (built from synthetic sample data) | CI | GitHub Actions `deploy-docs` |

## Orchestration and Postgres (Phase 5)

| Date | Metric | Value | Env | Command / source |
|---|---|---|---|---|
| 2026-11-09 | `transitiq_daily` DAG, full run (11 tasks, all green) | 260.06 s | LOCAL-DOCKER (Airflow 3.1) | Airflow UI, run 2026-11-09 13:09:30 |
| 2026-11-09 | Silver rebuild from bronze onto the warehouse volume (all tables) | subway_events 14,822,712 rows in 144.74 s; alerts 5,171,976 rows in 159.83 s; total 22,985,288 rows in, 14,166 quarantined (0.06%) | LOCAL-DOCKER | `transitiq-silver build --rebuild` |
| 2026-11-09 | History after the first daily runs | 371 subway service days (2025-11-01 to 2026-11-08), 14,808,616 valid events | LOCAL-DOCKER | `transitiq-silver report` |
| 2026-11-09 | Quality checks after the green run | 7 pass, 1 warn (volume, a Sunday compared with all days; fixed in D-023) | LOCAL-DOCKER | `transitiq-gold quality` |
| 2026-11-09 | Bronze/silver reconciliation | 371 of 371 days matched exactly | LOCAL-DOCKER | same |
| 2026-11-09 | Gold segments | 13,434,100 travel-time segments from 14,808,616 silver events | LOCAL-DOCKER | same |
| 2026-11-09 | Weather coverage of the subway date range | 8,924 of 8,952 hours (99.69%) | LOCAL-DOCKER | same |
| _pending_ | Rows published to local Postgres / Neon, database size | from the `publish_gold` task log | LOCAL-DOCKER / PROD (Neon) | Airflow `publish_gold` log |

## Real time (Phase 6)

First real collection on the laptop, 2026-11-09 (UTC). The DAG run that built these tables read collector data up to 21:08 UTC.

| Date | Metric | Value | Env | Command / source |
|---|---|---|---|---|
| 2026-11-09 | Collector snapshots in silver at DAG time | 54 VehiclePositions snapshots, 39,076 vehicle rows, 81,690 trip-update rows | LOCAL-DOCKER | report query on `silver.rt_*` |
| 2026-11-09 | Collector gaps | 49 min (19:01-19:50), 64 min (20:04-21:08), 19 min (21:20-21:39) in the first 2.9 h; continuous from 21:39 to 22:41 at least. Cause not logged at the time; the collector now logs it (D-024) | LOCAL-DOCKER | bronze `.meta.json` snapshot counts |
| 2026-11-09 | Coverage windows at DAG time | 3 windows: 11.5, 14.0 and 0 minutes | LOCAL-DOCKER | `intermediate.int_rt_coverage` |
| 2026-11-09 | Trips judged for ghost status | 0: no window was long enough to cover a whole trip, which is the intended behaviour (D-022) | LOCAL-DOCKER | `gold.fact_ghost_trip` |
| 2026-11-09 | Bus arrivals estimated | 7,309 arrivals on 1,047 trips; median delay 2.1 min, 90th percentile 8.8 min | LOCAL-DOCKER | `gold.fact_bus_arrival` |
| 2026-11-09 | Bus bunching events | 214 (78 flagged after a long gap), worst routes 32 (18), 70 (16), 116 (15), 749 (14), 16 (14). Measured before D-024; some long-gap flags came from collection gaps | LOCAL-DOCKER | `gold.fact_bunching_event` |
| 2026-11-09 | Collector disk use | compact Parquet 4.57 MB for 203 snapshots (~65 MB/day at 30 s if continuous); raw `.pb.gz` 3.08 MB in 16 snapshot pairs (~18 MB/day at one pair per 15 min) | LOCAL-DOCKER | sizes of `data/bronze/source=gtfs_rt_*` |
| 2026-11-09 | `dbt_build` with real-time models | 9 min 24 s (third try; tries 1-2 failed with a DuckDB file lock, cause not found: no leftover process in the Airflow container, no other transitiq container running) | LOCAL-DOCKER (Airflow 3.1) | Airflow UI, run 2026-11-09 21:14:50 UTC |
| 2026-11-09 | Gap fix check (D-024) on real snapshots with a 6-minute hole cut in | 41 bus headways (up to 685 s) would have spanned the gap; now 0 | SANDBOX | replay of recorded snapshots + `dbt build` |

## Sections to fill as phases complete

- dbt: model and test counts (Phase 4)
- Real time: ghost-trip rate over a full covered day (Phase 6 follow-up)
- ML: calibration per quantile, pinball loss vs schedule baseline (Phase 7)
- API: p95 latency, test counts (Phase 8, 12)
- Notifications sent, from `notification_log` (Phase 10)
- Ask TransitIQ evaluation accuracy (Phase 11)
