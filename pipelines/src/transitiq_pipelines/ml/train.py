"""Train, calibrate and evaluate the Leave-By Planner models; log everything to MLflow.

    train (oldest)  ->  valid (early stopping)  ->  cal (conformal shift)  ->  test (newest)

The test days are the most recent ones and are never used to fit or tune anything, so the
numbers in the report are what a rider would have seen on days the model had never met.

Outputs (under data/ml/):
    models/<run>/q50.txt ... q95.txt     LightGBM boosters
    models/<run>/meta.json               features, category lists, conformal shifts, split
    models/<run>/report.json             calibration and pinball loss vs the schedule baseline
    models/current.json                  which run the API should load (Phase 8)
    mlflow.db, mlartifacts/              MLflow tracking (sqlite store)
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from transitiq_pipelines import fsutil
from transitiq_pipelines.ml import evaluate as ev
from transitiq_pipelines.ml import model as m

log = logging.getLogger(__name__)
EXPERIMENT = "leave-by-planner"


def load_dataset(dataset_dir: Path) -> pd.DataFrame:
    files = sorted(dataset_dir.glob("month=*.parquet"))
    if not files:
        raise FileNotFoundError(f"no dataset in {dataset_dir}; run `transitiq-ml dataset` first")
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    return m.add_derived(df)


def _score(y, pred, base) -> dict:
    model_card = ev.scorecard(y, pred)
    base_card = ev.scorecard(y, base)
    out = {}
    for key in model_card:
        out[key] = {
            "coverage": round(model_card[key]["coverage"], 4),
            "pinball_s": round(model_card[key]["pinball"], 2),
            "baseline_coverage": round(base_card[key]["coverage"], 4),
            "baseline_pinball_s": round(base_card[key]["pinball"], 2),
            "pinball_improvement": round(
                ev.improvement(model_card[key]["pinball"], base_card[key]["pinball"]), 4
            ),
        }
    return out


def evaluate_split(models, part: pd.DataFrame, categories, offsets) -> dict:
    """Raw and conformal-adjusted scores on rows where the schedule baseline is defined."""
    has_base = part["sched_ride_s"].notna() & part["sched_headway_s"].notna()
    part = part[has_base]
    y = part[m.LABEL].to_numpy(float)
    raw = m.predict(models, part, categories)
    adj = ev.apply_offsets(raw, offsets)
    base = ev.schedule_baseline(part["sched_ride_s"], part["sched_headway_s"])
    by_line = {}
    for line, idx in part.groupby("line").indices.items():
        card = _score(y[idx], adj[idx], base[idx])["q90"]
        by_line[line] = {"rows": int(len(idx)), **card}
    return {
        "rows": int(len(part)),
        "rows_without_schedule": int((~has_base).sum()),
        "raw": _score(y, raw, base),
        "calibrated": _score(y, adj, base),
        "q90_by_line": by_line,
        "median_journey_s": float(np.median(y)),
        "median_pred_p50_s": float(np.median(adj[:, 0])),
        "median_pred_p90_s": float(np.median(adj[:, 2])),
    }


def run_training(
    data_dir: Path,
    test_days: int = 60,
    cal_days: int = 30,
    valid_days: int = 14,
    max_rounds: int = 3000,
    use_mlflow: bool = True,
) -> dict:
    started = time.monotonic()
    ml_dir = data_dir / "ml"
    df = load_dataset(ml_dir / "dataset")
    split = m.time_split(df, test_days, cal_days, valid_days)
    categories = m.learn_categories(split.train)
    log.info("split %s", split.bounds)

    models = m.train_quantiles(split.train, split.valid, categories, max_rounds=max_rounds)

    cal = split.cal
    cal_pred = m.predict(models, cal, categories)
    offsets = ev.conformal_offsets(cal[m.LABEL].to_numpy(float), cal_pred)

    report = {
        "trained_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "quantiles": list(ev.QUANTILES),
        "split": split.bounds,
        "conformal_log_offsets": [round(float(o), 5) for o in offsets],
        "best_iterations": {f"q{round(q * 100)}": int(b.best_iteration) for q, b in models.items()},
        "test": evaluate_split(models, split.test, categories, offsets),
        "cal": evaluate_split(models, cal, categories, offsets),
        "feature_importance_gain_q90": _importance(models[0.9]),
    }
    report["train_seconds"] = round(time.monotonic() - started, 1)

    run_name = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out = ml_dir / "models" / run_name
    out.mkdir(parents=True, exist_ok=True)
    for q, booster in models.items():
        booster.save_model(
            str(out / f"q{round(q * 100)}.txt"), num_iteration=booster.best_iteration
        )
    meta = {
        "run": run_name,
        "features": m.FEATURES,
        "categorical": m.CATEGORICAL,
        "categories": categories,
        "quantiles": list(ev.QUANTILES),
        "label": f"log({m.LABEL})",
        "conformal_log_offsets": report["conformal_log_offsets"],
        "params": m.PARAMS,
    }
    (out / "meta.json").write_text(json.dumps(meta, indent=2), "utf-8")
    (out / "report.json").write_text(json.dumps(report, indent=2), "utf-8")
    pointer = ml_dir / "models" / "current.json"
    tmp = pointer.with_suffix(".tmp")
    tmp.write_text(json.dumps({"run": run_name}), "utf-8")
    fsutil.atomic_replace(tmp, pointer)

    if use_mlflow:
        report["mlflow_run_id"] = _log_mlflow(ml_dir, run_name, report, out)
        (out / "report.json").write_text(json.dumps(report, indent=2), "utf-8")
    return report


def _importance(booster) -> dict[str, float]:
    gain = booster.feature_importance(importance_type="gain")
    total = float(gain.sum()) or 1.0
    pairs = sorted(zip(booster.feature_name(), gain, strict=True), key=lambda p: -p[1])
    return {name: round(float(g) / total, 4) for name, g in pairs[:10]}


def _log_mlflow(ml_dir: Path, run_name: str, report: dict, model_dir: Path) -> str:
    os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")
    # The containers have no git and run as a uid with no /etc/passwd entry; without these MLflow
    # prints long warnings while collecting optional run tags.
    os.environ.setdefault("GIT_PYTHON_REFRESH", "quiet")
    os.environ.setdefault("USER", "transitiq")
    os.environ.setdefault("LOGNAME", "transitiq")
    import mlflow

    uri = os.environ.get("MLFLOW_TRACKING_URI") or f"sqlite:///{(ml_dir / 'mlflow.db').as_posix()}"
    mlflow.set_tracking_uri(uri)
    exp = mlflow.get_experiment_by_name(EXPERIMENT)
    if exp is None:
        exp_id = mlflow.create_experiment(
            EXPERIMENT, artifact_location=(ml_dir / "mlartifacts").resolve().as_uri()
        )
    else:
        exp_id = exp.experiment_id
    with mlflow.start_run(experiment_id=exp_id, run_name=run_name) as run:
        mlflow.log_params({k: v for k, v in m.PARAMS.items()})
        for part, bounds in report["split"].items():
            mlflow.log_params({f"{part}_from": bounds["from"], f"{part}_to": bounds["to"]})
            mlflow.log_metric(f"{part}_rows", bounds["rows"])
        for kind in ("raw", "calibrated"):
            for key, card in report["test"][kind].items():
                for metric, value in card.items():
                    mlflow.log_metric(f"test_{kind}_{key}_{metric}", value)
        mlflow.log_metric("train_seconds", report["train_seconds"])
        mlflow.log_artifacts(str(model_dir), artifact_path="model")
        return run.info.run_id
