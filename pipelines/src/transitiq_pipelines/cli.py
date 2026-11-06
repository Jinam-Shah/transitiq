"""transitiq-ingest: load raw data into bronze.

Examples (PowerShell, from the repo root):
    transitiq-ingest backfill --start 2025-11-01 --end 2026-10-31
    transitiq-ingest lamp-subway --start 2026-10-01 --end 2026-10-31
    transitiq-ingest weather --start 2025-11-01 --end 2026-10-31
    transitiq-ingest gtfs-static
    transitiq-ingest lamp-alerts
    transitiq-ingest v3 alerts
    transitiq-ingest report

Re-running any command is safe: finished partitions are skipped (checkpoints + immutable
bronze keys). Exit code is 1 if any partition failed, so schedulers can retry.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

from transitiq_pipelines.config import Paths, load_dotenv, mbta_api_key
from transitiq_pipelines.http import HttpClient
from transitiq_pipelines.runlog import RunStats
from transitiq_pipelines.sources import Context
from transitiq_pipelines.storage import LocalBronzeStore, S3BronzeStore

log = logging.getLogger("transitiq_pipelines")


def _date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    yesterday = date.today() - timedelta(days=1)
    default_start = yesterday - timedelta(days=364)

    p = argparse.ArgumentParser(prog="transitiq-ingest", description=__doc__.split("\n")[0])
    p.add_argument("--data-dir", type=Path, default=Path("data"), help="default: ./data")
    p.add_argument(
        "--store",
        choices=["local", "s3"],
        default="local",
        help="local = data/bronze (durable); s3 = LocalStack bucket (lost on restart)",
    )
    p.add_argument("--bucket", default=os.getenv("BRONZE_BUCKET", "transitiq-local-bronze"))
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="command", required=True)

    def ranged(name: str, help_text: str):
        sp = sub.add_parser(name, help=help_text)
        sp.add_argument("--start", type=_date, default=default_start)
        sp.add_argument("--end", type=_date, default=yesterday)
        return sp

    bf = ranged("backfill", "all historical sources for a date range (default: last 365 days)")
    bf.add_argument("--workers", type=int, default=4)
    ls = ranged("lamp-subway", "LAMP subway on-time performance Parquet, one file per day")
    ls.add_argument("--workers", type=int, default=4)
    ranged("weather", "Open-Meteo hourly history for Boston, one file per month")
    la = sub.add_parser("lamp-alerts", help="LAMP alerts history (one large Parquet)")
    la.add_argument("--force", action="store_true")
    gs = sub.add_parser("gtfs-static", help="current MBTA GTFS zip (skipped if unchanged)")
    gs.add_argument("--force", action="store_true")
    v3 = sub.add_parser("v3", help="snapshot of a live MBTA V3 API endpoint")
    v3.add_argument("endpoint", choices=["alerts", "facilities"])
    rp = sub.add_parser("report", help="summarise local bronze (rows, days, bytes)")
    rp.add_argument("--json", action="store_true")
    return p


def make_context(args, paths: Paths) -> Context:
    if args.store == "s3":
        import boto3

        client = boto3.client(
            "s3",
            endpoint_url=os.getenv("LOCALSTACK_ENDPOINT", "http://localhost:4566"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        )
        store = S3BronzeStore(args.bucket, client)
    else:
        store = LocalBronzeStore(paths.bronze)
    return Context(
        store=store, http=HttpClient(), checkpoint_dir=paths.checkpoints, tmp_dir=paths.tmp
    )


def run_command(args, ctx: Context) -> list[RunStats]:
    from transitiq_pipelines.sources import (
        gtfs_static,
        lamp_alerts,
        lamp_subway,
        mbta_v3,
        open_meteo,
    )

    cmd = args.command
    if cmd == "lamp-subway":
        return [lamp_subway.run(ctx, args.start, args.end, workers=args.workers)]
    if cmd == "weather":
        return [open_meteo.run(ctx, args.start, args.end)]
    if cmd == "lamp-alerts":
        return [lamp_alerts.run(ctx, force=args.force)]
    if cmd == "gtfs-static":
        return [gtfs_static.run(ctx, force=args.force)]
    if cmd == "v3":
        return [mbta_v3.run(ctx, args.endpoint, api_key=mbta_api_key())]
    if cmd == "backfill":
        return [
            gtfs_static.run(ctx),
            open_meteo.run(ctx, args.start, args.end),
            lamp_subway.run(ctx, args.start, args.end, workers=args.workers),
            lamp_alerts.run(ctx),
            mbta_v3.run(ctx, "alerts", api_key=mbta_api_key()),
            mbta_v3.run(ctx, "facilities", api_key=mbta_api_key()),
        ]
    raise ValueError(cmd)


def main(argv: list[str] | None = None) -> int:
    load_dotenv(Path(".env"))
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    paths = Paths(args.data_dir).ensure()

    if args.command == "report":
        from transitiq_pipelines.report import format_table, latest_runs, summarise

        summary = summarise(paths.bronze)
        if args.json:
            print(json.dumps({**summary, "latest_runs": latest_runs(paths.runs)}, indent=2))
        else:
            print(format_table(summary, latest_runs(paths.runs)))
        return 0

    ctx = make_context(args, paths)
    results = run_command(args, ctx)
    for stats in results:
        stats.write(paths.runs)
        status = "OK  " if stats.ok else "FAIL"
        print(
            f"[{status}] {stats.source:<22} fetched={stats.fetched:<4} skipped={stats.skipped:<4} "
            f"rows={stats.rows:<10,} MB={stats.bytes / 1e6:<8.1f} {stats.duration_seconds}s"
        )
        for failure in stats.failures[:10]:
            print(f"        failed {failure['partition']}: {failure['error']}")
    return 0 if all(s.ok for s in results) else 1


if __name__ == "__main__":
    sys.exit(main())
