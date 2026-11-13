"""Leave-By Planner model code: metrics, baseline, calibration, splits, and a small training run.

The metric tests need only numpy and pandas; the training test needs LightGBM and is skipped
where it is not installed (the Windows dev venv; it runs in CI and in the Docker image).
"""

import json

import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")

from transitiq_pipelines.ml import evaluate as ev  # noqa: E402
from transitiq_pipelines.ml import model as m  # noqa: E402


def test_pinball_known_values():
    y = np.array([10.0, 10.0])
    # under-predict by 2 costs q * 2; over-predict by 2 costs (1 - q) * 2
    assert ev.pinball(y, np.array([8.0, 8.0]), 0.9) == pytest.approx(1.8)
    assert ev.pinball(y, np.array([12.0, 12.0]), 0.9) == pytest.approx(0.2)
    assert ev.pinball(y, y, 0.5) == 0.0


def test_coverage():
    assert ev.coverage(np.array([1, 2, 3, 4]), np.array([2, 2, 2, 2])) == 0.5


def test_schedule_baseline_is_ride_plus_share_of_headway():
    base = ev.schedule_baseline(np.array([600.0]), np.array([300.0]))
    assert base.tolist() == [[750.0, 840.0, 870.0, 885.0]]  # 600 + q * 300
    assert np.isnan(ev.schedule_baseline(np.array([np.nan]), np.array([300.0]))).all()


def test_non_crossing_sorts_each_row():
    assert ev.non_crossing(np.array([[5, 4, 6, 7]])).tolist() == [[4, 5, 6, 7]]


def test_conformal_offsets_hit_the_target_on_the_calibration_set():
    rng = np.random.default_rng(0)
    y = np.exp(rng.normal(6, 0.3, 5000))
    biased = np.column_stack([np.full(5000, np.exp(6.0))] * 4)  # same guess for every quantile
    offsets = ev.conformal_offsets(y, biased)
    adjusted = ev.apply_offsets(biased, offsets)
    for j, q in enumerate(ev.QUANTILES):
        assert ev.coverage(y, adjusted[:, j]) == pytest.approx(q, abs=0.01)
    assert np.all(np.diff(offsets) > 0)


def test_improvement():
    assert ev.improvement(80.0, 100.0) == pytest.approx(0.2)


def _days(n):
    return pd.date_range("2026-01-01", periods=n, freq="D").date


def test_time_split_is_ordered_and_disjoint():
    df = pd.DataFrame({"service_date": np.repeat(_days(30), 3)})
    s = m.time_split(df, test_days=5, cal_days=4, valid_days=3)
    assert s.bounds["test"]["days"] == 5 and s.bounds["cal"]["days"] == 4
    assert s.bounds["valid"]["days"] == 3 and s.bounds["train"]["days"] == 18
    assert s.bounds["train"]["to"] < s.bounds["valid"]["from"] <= s.bounds["valid"]["to"]
    assert s.bounds["valid"]["to"] < s.bounds["cal"]["from"]
    assert s.bounds["cal"]["to"] < s.bounds["test"]["from"]
    assert sum(b["rows"] for b in s.bounds.values()) == len(df)


def test_time_split_refuses_too_few_days():
    df = pd.DataFrame({"service_date": _days(10)})
    with pytest.raises(ValueError, match="need at least"):
        m.time_split(df, test_days=5, cal_days=4, valid_days=3)


def test_scheduled_headway_uses_branch_only_when_one_branch_serves():
    df = pd.DataFrame(
        {
            "sched_headway_branch_s": [600.0, 600.0, np.nan],
            "sched_headway_trunk_s": [300.0, 300.0, 300.0],
            "routes_serving": [1, 3, 1],
        }
    )
    assert m.scheduled_headway(df).tolist() == [600.0, 300.0, 300.0]


def test_unknown_categories_become_missing():
    df = _synthetic(50, np.random.default_rng(1))
    cats = m.learn_categories(df)
    df.loc[0, "origin"] = "place-new"
    x = m.to_matrix(m.add_derived(df), cats)
    assert pd.isna(x.loc[0, "origin"]) and str(x["origin"].dtype) == "category"


def _synthetic(n, rng):
    """Journeys whose spread grows with scheduled headway: wait ~ U(0, headway) + noisy ride."""
    stations = [f"s{i}" for i in range(6)]
    headway = rng.choice([300.0, 600.0], n)
    ride = rng.choice([300.0, 900.0], n)
    journey = rng.uniform(0, headway) + ride * rng.lognormal(0, 0.15, n)
    return pd.DataFrame(
        {
            "service_date": np.repeat(_days(40), n // 40 + 1)[:n],
            "line": rng.choice(["Red", "Orange"], n),
            "origin": rng.choice(stations, n),
            "destination": rng.choice(stations, n),
            "day_type": rng.choice(["weekday", "saturday"], n),
            "direction_id": rng.integers(0, 2, n),
            "sched_ride_s": ride,
            "sched_headway_trunk_s": headway,
            "sched_headway_branch_s": np.nan,
            "routes_serving": 1,
            "segments": (ride / 120).round(),
            "hour_local": rng.uniform(5, 24, n),
            "day_of_week": rng.integers(1, 8, n),
            "is_holiday": 0,
            "temperature_c": rng.normal(10, 5, n),
            "precipitation_mm": 0.0,
            "snowfall_cm": 0.0,
            "snow_depth_m": 0.0,
            "wind_gusts_kmh": 20.0,
            "recent_ride_ratio": 1.0,
            "recent_headway_ratio": 1.0,
            "recent_segments": 100,
            "delay_alerts": 0,
            "disruption_alerts": 0,
            "alert_max_severity": 0,
            "journey_s": journey.round(),
        }
    )


def test_train_calibrate_and_beat_the_schedule_on_synthetic_journeys(tmp_path):
    pytest.importorskip("lightgbm")
    from transitiq_pipelines.ml.train import evaluate_split, run_training

    rng = np.random.default_rng(7)
    df = _synthetic(8000, rng)
    split = m.time_split(m.add_derived(df), test_days=8, cal_days=6, valid_days=4)
    cats = m.learn_categories(split.train)
    models = m.train_quantiles(split.train, split.valid, cats, max_rounds=200)
    pred = m.predict(models, split.test, cats)
    assert np.all(np.diff(pred, axis=1) >= 0)  # non-crossing
    offsets = ev.conformal_offsets(
        split.cal["journey_s"].to_numpy(float), m.predict(models, split.cal, cats)
    )
    scores = evaluate_split(models, split.test, cats, offsets)
    for key, q in zip(("q50", "q80", "q90", "q95"), ev.QUANTILES, strict=True):
        assert scores["calibrated"][key]["coverage"] == pytest.approx(q, abs=0.05)
    # trains do NOT run exactly on schedule here (noisy rides), so the model must do better
    assert scores["calibrated"]["q90"]["pinball_improvement"] > 0

    # the full pipeline writes models, a report and the current pointer (MLflow off here)
    ds = tmp_path / "ml" / "dataset"
    ds.mkdir(parents=True)
    df.to_parquet(ds / "month=2026-01.parquet")
    report = run_training(
        tmp_path, test_days=8, cal_days=6, valid_days=4, max_rounds=100, use_mlflow=False
    )
    run = json.loads((tmp_path / "ml" / "models" / "current.json").read_text())["run"]
    run_dir = tmp_path / "ml" / "models" / run
    assert {p.name for p in run_dir.iterdir()} >= {"q50.txt", "q95.txt", "meta.json", "report.json"}
    assert report["test"]["rows"] > 0 and len(report["conformal_log_offsets"]) == 4
