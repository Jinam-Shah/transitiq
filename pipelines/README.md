# pipelines

Python package `transitiq_pipelines` (in `src/`), installed with `pip install -r requirements-dev.txt`. `Dockerfile` builds the Linux image used by `docker compose run --rm pipelines ...`.

| Module | Purpose |
|---|---|
| `cli.py` | `transitiq-ingest` command (backfill, per-source commands, report) |
| `http.py` | Stdlib HTTP client: retries with backoff, honours Retry-After / x-ratelimit-reset, atomic downloads |
| `storage.py` | Immutable bronze stores: `LocalBronzeStore` (data/bronze) and `S3BronzeStore` (LocalStack) |
| `checkpoint.py` | Per-source resume state in data/checkpoints |
| `runlog.py` | Per-run stats in data/runs (rows, bytes, duration, failures) |
| `sources/` | One module per source: `lamp_subway`, `lamp_alerts`, `gtfs_static`, `open_meteo`, `mbta_v3` |
| `lambdas/ingest.py` | Ingest Lambda (V3 snapshots, heartbeat, supplied records) |
| `lambdas/validate.py` | Validate Lambda (bronze NDJSON → silver + quarantine) |
| `report.py` | Bronze summary for METRICS.md |
| `silver/contracts.py` | Schemas for every silver table + bronze contracts (schema-drift checks) |
| `silver/engine.py` | DuckDB validate-and-split engine with lineage columns |
| `silver/builders.py` | Per-table rules and loaders (subway_events, alerts, weather_hourly, facilities, gtfs_*) |
| `silver/warehouse.py` | PyIceberg SQLite catalog, partition overwrites, quarantine files |
| `silver/cli.py` | `transitiq-silver build / report` (runs in the Docker `pipelines` service) |

Airflow DAGs live in `airflow/dags/` (orchestration arrives in Phase 5).
