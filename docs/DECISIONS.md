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

## D-005: Local Terraform state (2026-11-06)

- **Problem:** Terraform needs somewhere to store state.
- **Options:** (a) local state file, (b) S3 backend with locking (in LocalStack or real AWS), (c) Terraform Cloud free tier.
- **Choice:** (a) local state, gitignored.
- **Reason:** The stack only ever targets a throwaway emulator, and CI creates and destroys it in the same job. A remote backend adds setup without adding safety here. On real AWS the right choice is (b).
- **Cost impact:** $0.

## D-006: Event-driven validation with Lambda code limited to stdlib + boto3 (2026-11-06)

- **Problem:** How bronze files reach validation, and how to package the Lambdas.
- **Options:** (a) S3 ObjectCreated → validate Lambda directly, (b) S3 → SQS → Lambda, (c) a scheduled batch job. For packaging: (i) plain zip with stdlib + boto3, (ii) zip plus a dependency layer (e.g. pyarrow), (iii) container images.
- **Choice:** (a) and (i).
- **Reason:** Direct S3 triggers are the simplest pattern that still gives retries plus a dead-letter queue (async invoke config + DLQ). Volume is small (a few files per hour), so SQS buffering isn't needed yet. Keeping to stdlib + boto3 (already in the Lambda runtime) means no build step on Windows. **Silver is NDJSON in Phase 1**; Phase 3 switches it to Parquet + Iceberg and will revisit packaging, because pyarrow is too large for a plain zip.
- **Cost impact:** On real AWS this design would sit in the Lambda, S3 and SQS free tiers at this volume. $0 here on LocalStack.

## D-007: Verify least-privilege IAM statically (2026-11-06)

- **Problem:** LocalStack's Hobby plan accepts IAM policies but does not enforce them (D-002), so "the Lambda worked" proves nothing about permissions.
- **Options:** (a) trust the Terraform code, (b) a paid LocalStack plan with IAM enforcement, (c) read the deployed policies back and check them with code.
- **Choice:** (c). `scripts/iam_check.py` fails on wildcard actions, `*` resources, NotAction/NotResource, attached managed policies, and role-specific forbidden actions (e.g. the ingest role must not read bronze). The e2e script runs it against what LocalStack actually stored; unit tests cover the checker itself.
- **Reason:** Free, automatic in CI, and catches the most common least-privilege mistakes. It does **not** prove AWS would deny a call, and the README says so.
- **Cost impact:** $0.

## D-008: Terraform and AWS provider versions (2026-11-06)

- **Choice:** Terraform 1.17.1 in CI (`>= 1.9` allowed locally), AWS provider `~> 6.0` (lock file pins 6.72.0), archive provider `~> 2.7` (2.8.1). The lock file includes Windows, Linux and macOS hashes so `terraform init` behaves the same on every machine.
- **Reason:** Current stable releases at the time of writing; the lock file makes builds reproducible.
