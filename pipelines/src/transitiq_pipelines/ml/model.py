"""LightGBM quantile models for journey time (wait + ride), one model per quantile (D-027).

The label is log(journey seconds). A quantile of log(y) is log of the quantile of y, so nothing
is lost, and errors become relative ("20% slower than usual"), which suits trips from 2 to 60
minutes. Predictions are put back in seconds and sorted so p50 <= p80 <= p90 <= p95.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from transitiq_pipelines.ml.evaluate import QUANTILES, non_crossing

CATEGORICAL = ["line", "origin", "destination", "day_type"]
NUMERIC = [
    "direction_id",
    "sched_ride_s",
    "sched_headway_s",
    "routes_serving",
    "segments",
    "hour_local",
    "day_of_week",
    "is_holiday",
    "temperature_c",
    "precipitation_mm",
    "snowfall_cm",
    "snow_depth_m",
    "wind_gusts_kmh",
    "recent_ride_ratio",
    "recent_headway_ratio",
    "recent_segments",
    "delay_alerts",
    "disruption_alerts",
    "alert_max_severity",
]
FEATURES = CATEGORICAL + NUMERIC
LABEL = "journey_s"

PARAMS = {
    "objective": "quantile",
    "learning_rate": 0.05,
    "num_leaves": 63,
    "min_data_in_leaf": 50,
    "feature_fraction": 0.9,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "lambda_l2": 1.0,
    "cat_smooth": 10.0,
    "verbosity": -1,
    "seed": 7,
    "deterministic": True,  # same data + same thread count -> same model (METRICS reproducible)
}


def scheduled_headway(df: pd.DataFrame) -> pd.Series:
    """Branch headway when one branch serves the pair, else the trunk (combined) headway."""
    branch = df["sched_headway_branch_s"]
    use_branch = (df["routes_serving"] == 1) & branch.notna()
    return branch.where(use_branch, df["sched_headway_trunk_s"])


def add_derived(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["sched_headway_s"] = scheduled_headway(df)
    return df


def to_matrix(df: pd.DataFrame, categories: dict[str, list[str]]) -> pd.DataFrame:
    """Feature frame with fixed category lists (unknown values become missing)."""
    x = df[FEATURES].copy()
    for col in CATEGORICAL:
        values = x[col].astype("string")
        values = values.where(values.isin(categories[col]))  # unseen station etc. -> missing
        x[col] = pd.Categorical(values, categories=categories[col])
    for col in NUMERIC:
        x[col] = pd.to_numeric(x[col], errors="coerce").astype("float64")
    return x


def learn_categories(df: pd.DataFrame) -> dict[str, list[str]]:
    return {c: sorted(df[c].dropna().astype(str).unique().tolist()) for c in CATEGORICAL}


@dataclass
class Split:
    train: pd.DataFrame
    valid: pd.DataFrame  # early stopping
    cal: pd.DataFrame  # conformal calibration
    test: pd.DataFrame  # held-out most recent days; never seen while fitting anything
    bounds: dict = field(default_factory=dict)


def time_split(df: pd.DataFrame, test_days: int, cal_days: int, valid_days: int) -> Split:
    """Split by service day: train | valid | cal | test, oldest to newest, no overlap."""
    days = np.sort(pd.to_datetime(df["service_date"]).dt.date.unique())
    need = test_days + cal_days + valid_days + 1
    if len(days) < need:
        raise ValueError(
            f"{len(days)} service days in the dataset; need at least {need} "
            f"(test {test_days} + cal {cal_days} + valid {valid_days} + 1 to train)"
        )
    test0 = days[-test_days]
    cal0 = days[-(test_days + cal_days)]
    valid0 = days[-(test_days + cal_days + valid_days)]
    d = pd.to_datetime(df["service_date"]).dt.date
    split = Split(
        train=df[d < valid0],
        valid=df[(d >= valid0) & (d < cal0)],
        cal=df[(d >= cal0) & (d < test0)],
        test=df[d >= test0],
    )

    def span(part: pd.DataFrame) -> dict:
        pd_ = pd.to_datetime(part["service_date"]).dt.date
        lo, hi = pd_.min(), pd_.max()
        return {
            "from": str(lo),
            "to": str(hi),
            "days": int(pd_.nunique()),
            "rows": int(len(part)),
        }

    split.bounds = {n: span(getattr(split, n)) for n in ("train", "valid", "cal", "test")}
    return split


def train_quantiles(
    train: pd.DataFrame,
    valid: pd.DataFrame,
    categories: dict[str, list[str]],
    quantiles=QUANTILES,
    max_rounds: int = 3000,
    early_stopping: int = 100,
) -> dict[float, object]:
    import lightgbm as lgb

    threads = int(os.environ.get("TRANSITIQ_ML_THREADS", "4"))
    dtrain = lgb.Dataset(
        to_matrix(train, categories),
        label=np.log(train[LABEL].to_numpy(float)),
        categorical_feature=CATEGORICAL,
        free_raw_data=False,
    )
    dvalid = lgb.Dataset(
        to_matrix(valid, categories),
        label=np.log(valid[LABEL].to_numpy(float)),
        categorical_feature=CATEGORICAL,
        reference=dtrain,
        free_raw_data=False,
    )
    models = {}
    for q in quantiles:
        params = {**PARAMS, "alpha": q, "metric": "quantile", "num_threads": threads}
        models[q] = lgb.train(
            params,
            dtrain,
            num_boost_round=max_rounds,
            valid_sets=[dvalid],
            callbacks=[lgb.early_stopping(early_stopping, verbose=False)],
        )
    return models


def predict(models: dict[float, object], df: pd.DataFrame, categories) -> np.ndarray:
    """n x len(models) journey seconds, quantiles in increasing order, non-crossing."""
    x = to_matrix(df, categories)
    cols = [
        np.exp(models[q].predict(x, num_iteration=models[q].best_iteration)) for q in sorted(models)
    ]
    return non_crossing(np.column_stack(cols))
