"""transitiq-ml: Leave-By Planner journey-time quantile models (LightGBM, MLflow).

docker compose run --rm pipelines transitiq-ml dataset          # simulated riders from gold
docker compose run --rm pipelines transitiq-ml train            # fit, calibrate, evaluate
docker compose run --rm pipelines transitiq-ml report           # print the current report

Everything lives in <data-dir>/warehouse/ml (the Docker warehouse volume, D-020): the dataset,
the models, and the MLflow sqlite store.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from transitiq_pipelines.config import Paths, load_dotenv


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="transitiq-ml", description=__doc__.split("\n")[0])
    p.add_argument("--data-dir", type=Path, default=Path("data"))
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="command", required=True)
    ds = sub.add_parser("dataset", help="build training rows (one Parquet file per month)")
    ds.add_argument("--riders-per-line-day", type=int, default=300)
    ds.add_argument("--seed", type=int, default=7)
    ds.add_argument("--rebuild", action="store_true", help="rebuild every month, not just new")
    tr = sub.add_parser("train", help="train, calibrate on recent days, test on the newest days")
    tr.add_argument("--test-days", type=int, default=60)
    tr.add_argument("--cal-days", type=int, default=30)
    tr.add_argument("--valid-days", type=int, default=14)
    tr.add_argument("--max-rounds", type=int, default=3000)
    tr.add_argument("--no-mlflow", action="store_true")
    sub.add_parser("report", help="print the report of the current model")
    return p


def _ml_dir(data_dir: Path) -> Path:
    return data_dir / "warehouse"


def _lower(x: float) -> str:
    return f"{abs(x):.1%} {'lower' if x >= 0 else 'HIGHER'}"


def print_report(report: dict) -> None:
    split = report["split"]
    print(
        "split:",
        ", ".join(f"{k} {v['from']}..{v['to']} ({v['rows']:,} rows)" for k, v in split.items()),
    )
    test = report["test"]
    print(f"held-out test riders with a schedule baseline: {test['rows']:,}")
    print("quantile  under it: model  schedule   pinball loss: model  schedule  model vs schedule")
    for key, c in test["calibrated"].items():
        print(
            f"  {key:<8}{c['coverage']:>16.1%}{c['baseline_coverage']:>10.1%}"
            f"{c['pinball_s']:>21.1f}s{c['baseline_pinball_s']:>9.1f}s"
            f"  {_lower(c['pinball_improvement'])}"
        )
    print("q90 by line:")
    for line, c in test["q90_by_line"].items():
        print(
            f"  {line:<9} {c['coverage']:.1%} under it (schedule {c['baseline_coverage']:.1%}), "
            f"pinball loss {_lower(c['pinball_improvement'])}, {c['rows']:,} riders"
        )
    print(f"train seconds: {report['train_seconds']}; mlflow run: {report.get('mlflow_run_id')}")


def main(argv: list[str] | None = None) -> int:
    load_dotenv(Path(".env"))
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    paths = Paths(args.data_dir).ensure()
    root = _ml_dir(paths.data_dir)
    ml_dir = root / "ml"

    if args.command == "dataset":
        from transitiq_pipelines.ml.dataset import build_dataset

        stats = build_dataset(
            root / "transitiq.duckdb",
            ml_dir / "dataset",
            riders_per_line_day=args.riders_per_line_day,
            seed=args.seed,
            memory_limit=os.environ.get("TRANSITIQ_DUCKDB_MEMORY", "2GB"),
            rebuild=args.rebuild,
        )
        print(json.dumps(stats))
        return 0

    if args.command == "train":
        from transitiq_pipelines.ml.train import run_training

        try:
            report = run_training(
                root,
                test_days=args.test_days,
                cal_days=args.cal_days,
                valid_days=args.valid_days,
                max_rounds=args.max_rounds,
                use_mlflow=not args.no_mlflow,
            )
        except (FileNotFoundError, ValueError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        print_report(report)
        return 0

    current = ml_dir / "models" / "current.json"
    if not current.exists():
        print("no model trained yet", file=sys.stderr)
        return 2
    run = json.loads(current.read_text("utf-8"))["run"]
    print_report(json.loads((ml_dir / "models" / run / "report.json").read_text("utf-8")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
