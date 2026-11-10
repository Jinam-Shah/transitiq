"""Cut recorded GTFS-Realtime snapshots down to a few routes for the test fixtures.

    python scripts/make_rt_fixture.py <recorded_dir> tests/fixtures/gtfs_rt \
        --routes 23 66 CR-Worcester

Input: vp_*.pb / tu_*.pb files saved from https://cdn.mbta.com/realtime/ (raw, unmodified).
Output: the same snapshots, filtered to the given routes, gzip-compressed. MBTA data, credited
under the MassDOT Developers License Agreement (docs/DATA_SOURCES.md).
"""

import argparse
import gzip
from pathlib import Path

from google.transit import gtfs_realtime_pb2 as pb


def route_of(entity) -> str:
    if entity.HasField("vehicle"):
        return entity.vehicle.trip.route_id
    if entity.HasField("trip_update"):
        return entity.trip_update.trip.route_id
    return ""


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("src", type=Path)
    p.add_argument("dst", type=Path)
    p.add_argument("--routes", nargs="+", required=True)
    args = p.parse_args()
    args.dst.mkdir(parents=True, exist_ok=True)
    keep = set(args.routes)
    for path in sorted(args.src.glob("*.pb")):
        feed = pb.FeedMessage()
        feed.ParseFromString(path.read_bytes())
        out = pb.FeedMessage()
        out.header.CopyFrom(feed.header)
        for entity in feed.entity:
            if route_of(entity) in keep:
                out.entity.add().CopyFrom(entity)
        (args.dst / (path.name + ".gz")).write_bytes(gzip.compress(out.SerializeToString()))
    print(f"wrote {len(list(args.dst.glob('*.gz')))} files to {args.dst}")


if __name__ == "__main__":
    main()
