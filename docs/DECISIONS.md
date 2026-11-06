# Decisions

Each entry: problem, options, choice, reason, cost impact.

---

## D-001: Monorepo layout (2026-11-04)

- **Problem:** The project spans infrastructure, pipelines, dbt, ML, an API, and a web app.
- **Options:** (a) one repo per part, (b) a single monorepo.
- **Choice:** A monorepo with `infra/`, `pipelines/`, `dbt/`, `ml/`, `api/`, `web/`, `docs/`.
- **Reason:** One CI pipeline, one place for docs and metrics, and contract tests can read schemas across parts (for example, Parquet schemas vs dbt sources).
- **Cost impact:** None. GitHub Actions is free for public repos.

## D-002: LocalStack now needs a free auth token (2026-11-04)

- **Problem:** The spec says "LocalStack Community." Since LocalStack's 2026.03.0 release there is a single image that refuses to start without an auth token. The temporary bypass ended on April 6, 2026.
- **Options:** (a) LocalStack free **Hobby** plan with an auth token, (b) pin an old pre-token image, (c) moto server only, (d) a community fork.
- **Choice:** (a) LocalStack Hobby plan, image `localstack/localstack:2026.3.0`, with the token in `.env` locally and in the `LOCALSTACK_AUTH_TOKEN` GitHub secret for CI. Lambda unit tests will still use **moto** (no token needed).
- **Reason:** Hobby is free, needs no credit card, and includes S3, Lambda, EventBridge, SQS, and IAM, plus CI use under a fair-use policy (checked on localstack.cloud/pricing on 2026-11-04). Old pinned images get no fixes, and forks are new and unproven.
- **Limits to be honest about:**
  - Hobby is for **non-commercial** use only. This is a personal portfolio project, so it fits.
  - **IAM policy enforcement is not included in Hobby.** LocalStack will accept the least-privilege IAM policies we write in Phase 1 but will **not enforce** them. We will validate policies another way (static checks in Phase 1), and the README will say so.
  - If the token is missing in CI (for example, on a fork), the stack smoke job skips itself instead of failing.
- **Cost impact:** $0.

## D-003: Airflow 3 in `standalone` mode for local development (2026-11-04)

- **Problem:** Airflow's official compose file runs 6+ containers, which is heavy on a laptop.
- **Options:** (a) the full official compose file (Celery, Redis, workers), (b) `airflow standalone` with LocalExecutor and Postgres.
- **Choice:** (b), image `apache/airflow:3.1.0-python3.12`, sharing the project's Postgres container (separate `airflow` database).
- **Reason:** One container, enough for daily batch DAGs. Login is disabled locally (`SIMPLE_AUTH_MANAGER_ALL_ADMINS`) because it binds only to the developer's machine. Not a production setup, and the README says so.
- **Cost impact:** $0, lower RAM use.

## D-004: Postgres host port 5433 (2026-11-04)

- **Problem:** Many Windows machines already have Postgres on 5432.
- **Choice:** Map the container to host port `5433` by default (configurable in `.env`).
- **Cost impact:** None.
