# pipelines

- `lambdas/ingest/handler.py`: writes raw NDJSON files to the bronze bucket (Phase 1: heartbeat + supplied records; real sources arrive in Phase 2).
- `lambdas/validate/handler.py`: bronze → silver (valid rows + lineage) and quarantine (rejected rows + reason). Idempotent.
- `airflow/dags/`: Airflow DAGs.
- `src/transitiq_pipelines/`: shared pipeline code (from Phase 2).
