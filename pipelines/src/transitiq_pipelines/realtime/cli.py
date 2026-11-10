"""transitiq-realtime: collect or replay MBTA GTFS-Realtime snapshots into bronze.

docker compose --profile realtime up -d realtime                  # continuous collector
docker compose run --rm pipelines transitiq-realtime collect --minutes 60
docker compose run --rm pipelines transitiq-realtime replay tests/fixtures/gtfs_rt
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from transitiq_pipelines.config import Paths, load_dotenv
from transitiq_pipelines.storage import LocalBronzeStore


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="transitiq-realtime", description=__doc__.split("\n")[0])
    p.add_argument("--data-dir", type=Path, default=Path("data"))
    sub = p.add_subparsers(dest="command", required=True)
    c = sub.add_parser("collect", help="snapshot the live feeds on an interval")
    c.add_argument("--interval", type=int, default=30, help="seconds between snapshots")
    c.add_argument("--minutes", type=float, default=None, help="stop after N minutes")
    c.add_argument("--flush-minutes", type=int, default=10)
    c.add_argument("--raw-every-minutes", type=int, default=15)
    r = sub.add_parser("replay", help="load recorded vp_*/tu_* .pb(.gz) files into bronze")
    r.add_argument("folder", type=Path)
    return p


def main(argv: list[str] | None = None) -> int:
    load_dotenv(Path(".env"))
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    from transitiq_pipelines.realtime.collector import Collector, replay

    store = LocalBronzeStore(Paths(args.data_dir).ensure().bronze)
    if args.command == "collect":
        collector = Collector(
            store, flush_minutes=args.flush_minutes, raw_every_minutes=args.raw_every_minutes
        )
        stats = collector.run(interval_seconds=args.interval, minutes=args.minutes)
    else:
        files = [p for p in args.folder.iterdir() if p.is_file()]
        stats = replay(Collector(store), files)
    print(json.dumps(stats))
    return 0 if stats["snapshots"] or stats["skipped_stale"] else 1


if __name__ == "__main__":
    sys.exit(main())
