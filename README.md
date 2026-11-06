# TransitIQ

**Boston transit reliability, measured, not scheduled.** TransitIQ tells MBTA riders when the train *actually* comes, gives each rider a "leave by" time they can trust, and surfaces problems the official apps don't show: ghost trips, bus bunching, and elevator outages.

> **Status:** Phase 0 (repo setup). Nothing is deployed yet. Every feature below is planned.

## Where it runs

This table is kept honest as the project grows. "LocalStack" means an AWS emulator in Docker on the laptop or CI runner, **not real AWS**.

| Part | Runs on | Status |
|---|---|---|
| AWS ingestion design (S3, Lambda, EventBridge, SQS, IAM) | LocalStack (free Hobby plan) in Docker, locally and in CI | Container starts (Phase 0); infra in Phase 1 |
| Postgres (app DB + Airflow metadata) | Local Docker | Container starts (Phase 0) |
| Airflow orchestration | Local Docker (`airflow standalone`) | Container starts (Phase 0) |
| Historical backfill, Iceberg, dbt, model training | Local Docker | Planned |
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

## CI

GitHub Actions runs Ruff, pytest, a docker-compose validation, and (when the `LOCALSTACK_AUTH_TOKEN` repo secret is set) starts LocalStack and Postgres and runs the smoke test.

## Docs

- [Decisions](docs/DECISIONS.md)
- [Data sources](docs/DATA_SOURCES.md)
- [Metrics](METRICS.md)

## Known limitations (so far)

- LocalStack's free Hobby plan does **not enforce IAM policies**, so least-privilege IAM is written and statically checked but not proven by LocalStack. See D-002.
- Airflow runs in single-container `standalone` mode with login disabled. Local development only.

## Credits

Data provided by the Massachusetts Bay Transportation Authority (MBTA). TransitIQ is not affiliated with or endorsed by the MBTA.
