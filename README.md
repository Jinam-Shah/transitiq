# TransitIQ

**Boston transit reliability, measured, not scheduled.** TransitIQ tells MBTA riders when the train *actually* comes, gives each rider a "leave by" time they can trust, and surfaces problems the official apps don't show: ghost trips, bus bunching, and elevator outages.

> **Status:** Phase 2 (ingestion: a year of MBTA history plus weather in bronze). Nothing is deployed to a hosted service yet.

## Where it runs

This table is kept honest as the project grows. "LocalStack" means an AWS emulator in Docker on the laptop or CI runner, **not real AWS**.

| Part | Runs on | Status |
|---|---|---|
| AWS ingestion design (S3, Lambda, EventBridge, SQS, IAM) | LocalStack (free Hobby plan) in Docker, locally and in CI. **Not real AWS.** | Deployed by Terraform and tested end to end (Phase 1) |
| Postgres (app DB + Airflow metadata) | Local Docker | Container starts (Phase 0) |
| Airflow orchestration | Local Docker (`airflow standalone`) | Container starts (Phase 0) |
| Historical backfill into bronze (`transitiq-ingest`) | Python on the laptop, writing to `data/bronze/` (durable). See D-009. | Built (Phase 2) |
| Iceberg, dbt, model training | Local Docker | Planned |
| Gold tables for the app | Neon PostgreSQL free tier | Planned |
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
- No public per-day bus history was found; bus reliability will come from our own GTFS-Realtime sampling (Phase 6).
- Airflow runs in single-container `standalone` mode with login disabled. Local development only.

## Credits

Transit data provided by the Massachusetts Department of Transportation (MassDOT) and the MBTA, under the MassDOT Developers License Agreement. TransitIQ is not affiliated with or endorsed by MassDOT or the MBTA. Weather data by [Open-Meteo.com](https://open-meteo.com/) (CC BY 4.0).
