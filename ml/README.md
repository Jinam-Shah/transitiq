# ml

Leave-By Planner journey-time models (Phase 7, D-027). The code is in
[`pipelines/src/transitiq_pipelines/ml`](../pipelines/src/transitiq_pipelines/ml) so it ships in
the same package and Docker images as the rest of the pipeline:

| File | What it does |
|---|---|
| `dataset.py` | One DuckDB query per month: simulated riders, their measured journey, and point-in-time features |
| `model.py` | Features, the time split, LightGBM quantile training and non-crossing prediction |
| `evaluate.py` | Pinball loss, calibration, the schedule-only baseline, conformal shifts (numpy only) |
| `train.py` | Train, calibrate, evaluate, save models and report, log to MLflow |
| `cli.py` | `transitiq-ml dataset / train / report` |

Outputs live on the `warehouse` Docker volume under `data/warehouse/ml/`: `dataset/`, `models/<run>/`,
`models/current.json`, and the MLflow store `mlflow.db` + `mlartifacts/`.

`mlflow-skinny` has no web UI. `transitiq-ml report` prints the current run; every run is also in MLflow and can be listed with `mlflow.search_runs()` against `sqlite:///data/warehouse/ml/mlflow.db`.
