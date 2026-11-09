# TransitIQ

**Boston transit reliability, measured, not scheduled.** TransitIQ tells MBTA riders when the train *actually* comes, gives each rider a "leave by" time they can trust, and surfaces problems the official apps don't show: ghost trips, bus bunching, and elevator outages.

> **Status:** Phase 5 (Airflow daily pipeline, quality checks, gold published to Postgres/Neon).
>
> **dbt docs:** https://jinam-shah.github.io/transitiq/ (schema, lineage and tests; row counts there come from a small synthetic sample, not real data).

## Where it runs

This table is kept honest as the project grows. "LocalStack" means an AWS emulator in Docker on the laptop or CI runner, **not real AWS**.

| Part | Runs on | Status |
|---|---|---|
| AWS ingestion design (S3, Lambda, EventBridge, SQS, IAM) | LocalStack (free Hobby plan) in Docker, locally and in CI. **Not real AWS.** | Deployed by Terraform and tested end to end (Phase 1) |
| Postgres (app DB + Airflow metadata) | Local Docker | Container starts (Phase 0) |
| Airflow orchestration | Local Docker (`airflow standalone`) | Container starts (Phase 0) |
| Historical backfill into bronze (`transitiq-ingest`) | Python on the laptop, writing to `data/bronze/` (durable). See D-009. | Built (Phase 2) |
| Silver Iceberg tables (`transitiq-silver`) | `pipelines` / Airflow containers on the laptop (Linux; D-012), stored on the `warehouse` Docker volume (D-020) | Built (Phase 3) |
| dbt gold star schema (`dbt build`) | `pipelines` Docker container, DuckDB file `data/warehouse/transitiq.duckdb` | Built (Phase 4) |
| dbt docs | GitHub Pages, built in CI from synthetic sample data | Built (Phase 4) |
| Daily orchestration | Airflow 3.1 in local Docker (`transitiq_daily` DAG) | Built (Phase 5) |
| Model training | Local Docker (same image) | Planned |
| Gold tables for the app | Local Docker Postgres always; **Neon free tier** when `NEON_DATABASE_URL` is set (`transitiq-gold publish`) | Built (Phase 5) |
| Backend API | Render free tier | Planned |
| Frontend | Vercel free tier | Planned |
| Scheduled production jobs | GitHub Actions schedules | Planned |

## Repository layout

```
infra/       Terraform for the AWS design (run on LocalStack) + Docker init files
pipelines/   Ingestion and cleaning code, Airflow DAGs
dbt/         dbt project: staging -> intermediate -> marts (gold star schema)
ml/          Quantile regression models, calibration reports, MLflow
api/         FastAPI backend
web/         Next.js frontend
docs/        DECISIONS.md, DATA_SOURCES.md
scripts/     Helper scripts (smoke test, etc.)
tests/       Repo-level tests
METRICS.md   Every measured number, with date, environment, and command
```

## Local setup (Windows PowerShell)

**Prerequisites:** Git, Docker Desktop (WSL 2 backend), Python 3.12, and a free LocalStack account for an auth token.

```powershell
Copy-Item .env.example .env          # then paste your LOCALSTACK_AUTH_TOKEN into .env
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
docker compose up -d
python scripts\smoke_test.py
```

| Service | URL |
|---|---|
| LocalStack | http://localhost:4566/_localstack/health |
| Airflow UI | http://localhost:8080 (no login locally) |
| Postgres | `localhost:5433`, user/password from `.env` |

Stop everything with `docker compose down` (add `-v` to delete data volumes).

## Ingestion (Phase 2)

`transitiq-ingest` downloads raw data into `data/bronze/` with retries, checkpoints and immutable, versioned file names. Re-running is always safe. Sources, licenses and the bronze layout are in [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md).

```powershell
pip install -r requirements-dev.txt                      # installs the transitiq-ingest command
transitiq-ingest backfill --start 2025-11-01 --end 2026-10-31
transitiq-ingest report
```

| Command | What it fetches |
|---|---|
| `transitiq-ingest lamp-subway --start … --end …` | MBTA LAMP subway on-time performance, one Parquet per day |
| `transitiq-ingest weather --start … --end …` | Open-Meteo hourly weather for Boston, one file per month |
| `transitiq-ingest lamp-alerts` | MBTA alerts history since 2019 (incl. elevator/escalator closures) |
| `transitiq-ingest gtfs-static` | Current MBTA GTFS schedule (skipped if unchanged) |
| `transitiq-ingest v3 alerts` / `v3 facilities` | Live MBTA V3 API snapshots |
| `transitiq-ingest report [--json]` | Files, rows, MB and days of history per source |

Every run writes `data/runs/<time>_<source>.json` (rows, bytes, duration, failures): the evidence behind METRICS.md.

## Silver layer (Phase 3)

`transitiq-silver` validates bronze into typed, deduplicated **Apache Iceberg** tables (PyIceberg, SQLite catalog in `data/warehouse/`). Rejected rows go to `data/quarantine/<table>/` with a reason. Every row carries lineage (`_source_file`, `_source_sha256`, `_loaded_at`). Re-runs skip unchanged inputs; replays never duplicate rows.

Runs in Docker because PyIceberg cannot yet handle Windows paths (D-012):

```powershell
docker compose build pipelines
docker compose run --rm pipelines transitiq-silver build
docker compose run --rm pipelines transitiq-silver report
docker compose run --rm pipelines pytest          # full test suite on Linux
```

| Table | From | Grain | Key rules (rejection reason) |
|---|---|---|---|
| `subway_events` | LAMP subway | one arrival/departure event; partitioned by `service_date` | duplicate_event, negative_duration, no_timestamps, implausible_travel/dwell_time, timestamp_outside_service_day, service_date_mismatch |
| `alerts` | LAMP alerts history | one alert version × informed entity | end_before_start, duplicate_row, missing ids |
| `weather_hourly` | Open-Meteo | one hour (UTC) | missing temperature, implausible values, duplicate_hour |
| `facilities` | MBTA V3 | one elevator/escalator | missing id/type, coordinates_out_of_range, duplicate_key |
| `gtfs_routes`, `gtfs_stops`, `gtfs_trips`, `gtfs_stop_times`, `gtfs_calendar`, `gtfs_calendar_dates` | GTFS static | one row per GTFS record | missing required fields, invalid types/times, duplicate_key |

Contracts for every table live in `pipelines/src/transitiq_pipelines/silver/contracts.py`; an upstream type change fails that partition as schema drift instead of silently breaking downstream models.

## Gold layer (Phase 4, dbt)

dbt (dbt-duckdb) builds the star schema from the silver views. Every gold model has an **enforced contract** (declared column types) and data tests.

```powershell
docker compose run --rm pipelines transitiq-silver publish   # silver Iceberg -> DuckDB views
docker compose run --rm pipelines dbt build                  # models + contracts + tests
docker compose run --rm pipelines dbt source freshness       # is silver up to date?
```

| Model | Grain |
|---|---|
| `dim_route`, `dim_stop`, `dim_date`, `dim_time_bucket`, `dim_weather_hour` | route, stop, date (with holidays and day type), 15-minute local bucket, weather hour |
| `fact_travel_time` | observed segment (previous stop -> stop) with scheduled time and delay |
| `fact_headway` | observed arrival with headway vs scheduled |
| `fact_bunching_event` | historical subway bunch (headway <= 25% of scheduled) |
| `fact_alert` | alert version |
| `fact_facility_outage` | elevator/escalator outage per facility |
| `mart_reliability_route_hour` | route x day type x 15-minute bucket: on-time rate, travel-time ratio p50/p90, headway CV and adherence, bunching index |
| `mart_stop_reliability` | stop x day type x 15-minute bucket: expected wait, headway p50/p90, reliability score |

Metric definitions: [D-015](docs/DECISIONS.md). Ghost trips and model predictions arrive in Phases 6 and 7.

## Daily pipeline (Phase 5, Airflow)

The `transitiq_daily` DAG (10:17 UTC) runs: 6 parallel ingests -> `silver_build` -> `silver_publish` -> `dbt_build` -> `quality_checks` -> `publish_gold`. Every step is an idempotent CLI command, so re-running a task or the whole DAG is safe. Open Airflow at http://localhost:8080 (local only, no login).

```powershell
docker compose up -d --build airflow                        # first time, or after dependency changes
# one time only, if ./data was created before Phase 5 (files were owned by root):
docker compose run --rm --user root pipelines chown -R 50000:0 /app/data /app/dbt
# one time only: prepare the warehouse volume (D-020) and rebuild silver into it from bronze
docker compose run --rm --user root pipelines chown 50000:0 /app/data/warehouse
docker compose run --rm pipelines transitiq-silver build --rebuild
docker compose run --rm pipelines transitiq-gold quality    # the checks on their own
docker compose run --rm pipelines transitiq-gold publish    # local Postgres (+ Neon if configured)
```

Quality checks (D-019): freshness, volume vs 28-day median, bronze/silver/gold reconciliation, quarantine rate, duplicates, null share, weather coverage. A failing check stops the run before publishing.

## AWS ingestion design (on LocalStack)

EventBridge schedules trigger an **ingest** Lambda that writes raw files to the **bronze** bucket: live MBTA V3 alert snapshots every 30 minutes, the facility list daily, and a daily heartbeat. Each new bronze NDJSON file triggers a **validate** Lambda that writes valid rows (with lineage columns) to **silver** and rejected rows (with a reason) to **quarantine**. Failed async events go to an **SQS dead-letter queue**. Details and diagram: [infra/terraform/README.md](infra/terraform/README.md).

Needs [Terraform](https://developer.hashicorp.com/terraform/install) 1.9 or newer.

```powershell
docker compose up -d --wait localstack
terraform -chdir=infra/terraform init
terraform -chdir=infra/terraform apply
python scripts\e2e_localstack.py
```

## CI

GitHub Actions runs Ruff, pytest (including moto-mocked Lambda tests), `terraform fmt` and `terraform validate`, and a docker-compose validation. When the `LOCALSTACK_AUTH_TOKEN` repo secret is set, it also starts LocalStack, runs `terraform apply`, runs the end-to-end check, and destroys the stack.

## Docs

- [Decisions](docs/DECISIONS.md)
- [Data sources](docs/DATA_SOURCES.md)
- [Metrics](METRICS.md)

## Known limitations (so far)

- LocalStack's free Hobby plan does **not enforce IAM policies**. Least-privilege IAM is written in Terraform and the deployed policies are checked by code, but LocalStack does not prove AWS would deny a call. See D-002 and D-007.
- Silver is NDJSON until Phase 3 switches it to Parquet + Iceberg (D-006).
- LocalStack's free plan has no persistence, so its bronze bucket is wiped on restart. Durable history lives in `data/bronze/` on the laptop (D-009).
- `transitiq-silver` must run in Docker on Windows (PyIceberg path bug, D-012).
- The DuckDB warehouse file allows one writer at a time: don't run `docker compose run pipelines ...` commands while the Airflow DAG is running.
- Only compact gold tables are published to Postgres; detailed facts stay in DuckDB (D-018).
- No public per-day bus history was found; bus reliability will come from our own GTFS-Realtime sampling (Phase 6).
- Airflow runs in single-container `standalone` mode with login disabled. Local development only.

## Credits

Transit data provided by the Massachusetts Department of Transportation (MassDOT) and the MBTA, under the MassDOT Developers License Agreement. TransitIQ is not affiliated with or endorsed by MassDOT or the MBTA. Weather data by [Open-Meteo.com](https://open-meteo.com/) (CC BY 4.0).
