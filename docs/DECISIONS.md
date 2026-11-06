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

## D-009: Durable bronze on local disk; LocalStack S3 for the Lambda path (2026-11-06)

- **Problem:** Phase 2 must hold months of history in bronze. LocalStack keeps state only in memory unless persistence is enabled, and persistence is listed only for the paid Base and Ultimate plans (docs.localstack.cloud, checked 2026-11-06). A restart would wipe a year of backfill.
- **Options:** (a) bronze only in LocalStack S3, re-downloading after every restart; (b) MinIO (a free S3-compatible server) in docker-compose; (c) a local folder with the same key layout as S3, plus the S3 store for the Lambda path.
- **Choice:** (c). One `BronzeStore` interface with two implementations: `LocalBronzeStore` (`data/bronze/`, durable) and `S3BronzeStore` (LocalStack bucket). The backfill CLI defaults to local; `--store s3` writes to LocalStack. The scheduled Lambda writes live V3 snapshots to S3 to demonstrate the AWS design.
- **Reason:** No new service to run, no repeated multi-hundred-MB downloads, and DuckDB/PyIceberg read local Parquet directly in Phase 3. Keys are identical in both stores, so moving to real S3 is a configuration change. MinIO would add a second S3 emulator next to LocalStack and confuse the "where it runs" story.
- **Honest limit:** The data in LocalStack's bucket is a demo of the design, not the system of record. The README says so.
- **Cost impact:** $0. Roughly 0.7 GB of disk for a year of history (to be measured and recorded in METRICS.md).

## D-010: One pipelines package for the CLI and the Lambdas (2026-11-06)

- **Problem:** The same fetch-and-store logic is needed locally (backfill) and in Lambda (scheduled snapshots). Copying code into each Lambda folder would drift.
- **Choice:** An installable package, `transitiq_pipelines` (`pipelines/src`), with a stdlib-only core (HTTP with retries, storage, checkpoints). Terraform zips the package once and both Lambdas use different handlers. Heavy libraries (pyarrow) are imported lazily, only by local backfills; a test-style check confirms the Lambda modules import without pyarrow.
- **Reason:** One implementation, unit-tested once. Stdlib + boto3 keeps the Lambda zip small (no layer) and avoids Windows build steps.
- **Cost impact:** $0.

## D-011: Bronze immutability and idempotent re-runs (2026-11-06)

- **Problem:** Re-running a backfill must never create duplicates, and bronze must never be overwritten, yet upstream files (LAMP days, the alerts file, GTFS) are republished in place.
- **Choice:**
  - Every key includes the **upstream version** (LAMP `last_modified`, GTFS `ETag`, or our fetch timestamp for API snapshots), so a republished file becomes a **new** key and older versions are kept.
  - Writes refuse existing keys: local writes check first and rename atomically; S3 writes use a conditional put (`IfNoneMatch="*"`), which also means the ingest role needs no read permission.
  - Per-source **checkpoints** (`data/checkpoints/<source>.json`) record what was fetched and at which version; re-runs skip finished partitions, and failed partitions are retried on the next run.
  - Weather months are only marked complete once they ended at least 7 days ago (archive lag); before that they are stored as dated `_partial_` files.
- **Reason:** Replays are cheap and safe, and silver (Phase 3) can always pick the newest version per partition while bronze keeps the full history of what upstream published.
- **Cost impact:** Slightly more disk when upstream republishes. LAMP republished all historical days on 2026-10-22, so expect occasional full re-downloads of the range you keep.
