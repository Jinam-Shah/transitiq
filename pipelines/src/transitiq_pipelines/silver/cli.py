"""transitiq-silver: build validated Iceberg silver tables from local bronze.

Run inside the pipelines container (PyIceberg cannot handle Windows paths yet, see D-012):
    docker compose run --rm pipelines transitiq-silver build
    docker compose run --rm pipelines transitiq-silver build --tables subway_events --force
    docker compose run --rm pipelines transitiq-silver report

Re-running is safe: unchanged inputs are skipped; changed partitions are overwritten in place.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from transitiq_pipelines.checkpoint import Checkpoint
from transitiq_pipelines.config import Paths
from transitiq_pipelines.silver.contracts import SILVER_TABLES

GROUPS = ["subway_events", "alerts", "weather_hourly", "facilities", "gtfs"]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="transitiq-silver", description=__doc__.split("\n")[0])
    p.add_argument("--data-dir", type=Path, default=Path("data"))
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument(
        "--allow-windows",
        action="store_true",
        help="try anyway on Windows (PyIceberg path bug, apache/iceberg-python#3720)",
    )
    sub = p.add_subparsers(dest="command", required=True)
    b = sub.add_parser("build", help="validate bronze into silver Iceberg tables")
    b.add_argument("--tables", default=",".join(GROUPS), help=f"comma list from {GROUPS}")
    b.add_argument("--force", action="store_true", help="reprocess even if inputs are unchanged")
    b.add_argument(
        "--rebuild", action="store_true", help="drop the selected tables first (contract change)"
    )
    r = sub.add_parser("report", help="rows, quarantine rate and reasons per silver table")
    r.add_argument("--json", action="store_true")
    return p


def quality_report(paths: Paths) -> dict:
    from transitiq_pipelines.silver.warehouse import Warehouse

    wh = Warehouse(paths.data_dir)
    tables, total_in, total_q = {}, 0, 0
    for name in SILVER_TABLES:
        parts = Checkpoint(paths.checkpoints, f"silver_{name}").partitions()
        rows_in = sum(p.get("rows_in", 0) for p in parts.values())
        quarantined = sum(p.get("quarantined", 0) for p in parts.values())
        reasons: Counter = Counter()
        for p in parts.values():
            reasons.update(p.get("reasons", {}))
        total_in += rows_in
        total_q += quarantined
        tables[name] = {
            "iceberg_rows": wh.record_count(name),
            "partitions": len(parts),
            "rows_in": rows_in,
            "rows_quarantined": quarantined,
            "quarantine_rate": round(quarantined / rows_in, 6) if rows_in else 0.0,
            "reasons": dict(reasons.most_common()),
        }
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "tables": tables,
        "totals": {
            "rows_in": total_in,
            "rows_quarantined": total_q,
            "quarantine_rate": round(total_q / total_in, 6) if total_in else 0.0,
        },
    }


def format_report(report: dict) -> str:
    header = (
        f"{'table':<22}{'iceberg rows':>14}{'rows in':>14}{'quarantined':>13}{'rate':>9}"
        "  top reasons"
    )
    lines = [header, "-" * len(header)]
    for name, t in report["tables"].items():
        top = ", ".join(f"{k}={v:,}" for k, v in list(t["reasons"].items())[:3])
        lines.append(
            f"{name:<22}{t['iceberg_rows']:>14,}{t['rows_in']:>14,}{t['rows_quarantined']:>13,}"
            f"{t['quarantine_rate']:>9.2%}  {top}"
        )
    tot = report["totals"]
    lines.append("-" * len(header))
    lines.append(
        f"{'TOTAL':<22}{'':>14}{tot['rows_in']:>14,}{tot['rows_quarantined']:>13,}"
        f"{tot['quarantine_rate']:>9.2%}"
    )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if sys.platform == "win32" and not args.allow_windows:
        print(
            "transitiq-silver uses PyIceberg, which cannot handle Windows paths yet "
            "(apache/iceberg-python#3720).\nRun it in Docker instead:\n"
            "    docker compose run --rm pipelines transitiq-silver "
            + " ".join(argv or sys.argv[1:]),
            file=sys.stderr,
        )
        return 2
    paths = Paths(args.data_dir).ensure()

    if args.command == "report":
        report = quality_report(paths)
        print(json.dumps(report, indent=2) if args.json else format_report(report))
        return 0

    from transitiq_pipelines.silver.builders import TABLE_GROUPS, SilverContext
    from transitiq_pipelines.silver.warehouse import Warehouse
    from transitiq_pipelines.storage import LocalBronzeStore

    selected = [t.strip() for t in args.tables.split(",") if t.strip()]
    unknown = [t for t in selected if t not in TABLE_GROUPS]
    if unknown:
        print(f"unknown tables {unknown}; choose from {GROUPS}", file=sys.stderr)
        return 2

    wh = Warehouse(paths.data_dir)
    if args.rebuild:
        for group in selected:
            names = (
                [n for n in SILVER_TABLES if n.startswith("gtfs_")] if group == "gtfs" else [group]
            )
            for name in names:
                wh.drop(name)
                wh.clear_quarantine(name)
                Checkpoint(paths.checkpoints, f"silver_{name}").path.unlink(missing_ok=True)

    ctx = SilverContext(
        store=LocalBronzeStore(paths.bronze),
        warehouse=wh,
        checkpoint_dir=paths.checkpoints,
        force=args.force or args.rebuild,
    )
    all_ok = True
    for group in selected:
        for stats in TABLE_GROUPS[group](ctx):
            data = stats.to_dict()
            stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
            (paths.runs / f"{stamp}_silver_{stats.table}.json").write_text(
                json.dumps(data, indent=2), "utf-8"
            )
            status = "OK  " if stats.ok else "FAIL"
            print(
                f"[{status}] {stats.table:<20} loaded={stats.partitions_loaded:<4} "
                f"skipped={stats.partitions_skipped:<4} rows_in={stats.rows_in:<11,} "
                f"quarantined={stats.rows_quarantined:<9,} ({data['quarantine_rate']:.2%}) "
                f"{stats.duration_seconds}s"
            )
            for f in stats.failures[:10]:
                print(f"        failed {f['partition']}: {f['error']}")
            all_ok = all_ok and stats.ok
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
