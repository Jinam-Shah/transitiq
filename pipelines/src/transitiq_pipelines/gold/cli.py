"""transitiq-gold: quality checks and Postgres publishing for the gold layer.

docker compose run --rm pipelines transitiq-gold quality
docker compose run --rm pipelines transitiq-gold publish               # local + Neon if set
docker compose run --rm pipelines transitiq-gold publish --target neon
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from transitiq_pipelines.config import Paths, load_dotenv


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="transitiq-gold", description=__doc__.split("\n")[0])
    p.add_argument("--data-dir", type=Path, default=Path("data"))
    p.add_argument(
        "--db", type=Path, default=None, help="default: <data-dir>/warehouse/transitiq.duckdb"
    )
    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("quality", help="cross-layer data quality checks (exit 1 on any fail)")
    pub = sub.add_parser("publish", help="publish compact gold tables to PostgreSQL")
    pub.add_argument("--target", choices=["auto", "local", "neon"], default="auto")
    pub.add_argument("--budget-mb", type=int, default=800)
    return p


def main(argv: list[str] | None = None) -> int:
    load_dotenv(Path(".env"))
    args = build_parser().parse_args(argv)
    paths = Paths(args.data_dir).ensure()
    db = args.db or paths.data_dir / "warehouse" / "transitiq.duckdb"

    if args.command == "quality":
        from transitiq_pipelines.gold.quality import FAIL, record, run_checks

        results = run_checks(db, paths.data_dir)
        report = record(results, db, paths.data_dir / "quality")
        for r in results:
            print(f"[{r.status.upper():4}] {r.check:<30} {r.value!s:<10} {r.detail}")
        print(f"report: {report}")
        return 1 if any(r.status == FAIL for r in results) else 0

    from transitiq_pipelines.gold.publish_pg import PublishError, publish, targets

    try:
        chosen = targets(args.target)
    except PublishError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    ok = True
    for label, url in chosen:
        try:
            tables, size = publish(db, url, budget_bytes=args.budget_mb * 1024 * 1024)
        except Exception as exc:  # report and continue with the next target
            print(f"[FAIL] {label}: {exc}", file=sys.stderr)
            ok = False
            continue
        for name, n in tables:
            print(f"[OK  ] {label:<5} gold.{name:<30} {n:>10,} rows")
        print(f"       {label} database size: {size / 1e6:.1f} MB")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
