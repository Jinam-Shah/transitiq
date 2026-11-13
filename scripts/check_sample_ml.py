"""Check the Leave-By Planner training rows built from the synthetic sample (CI).

The sample's trains follow a formula (transitiq_pipelines/silver/sample.py): on each line,
train n leaves its first station at 07:00 local + n * 6 min (train 5 runs 5 min early, right
behind train 4), every stop is 130 s apart, and the schedule says 120 s and a 6-minute headway.
So for every simulated rider the right answer can be computed here independently:

    wait  = (first departure from the origin at or after the rider arrives) - rider time
    ride  = 130 s * stops travelled          scheduled ride = 120 s * stops travelled

    python scripts/check_sample_ml.py data_sample
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from transitiq_pipelines.silver.sample import ROUTES


def departures(day, line: str, station: str) -> list[int]:
    r_i = list(ROUTES).index(line)
    k = ROUTES[line].index(station) + 1  # stop sequence 1..n
    base = int(datetime(day.year, day.month, day.day, 11, 0, tzinfo=UTC).timestamp())
    out = []
    for n in range(8):
        start = base + n * 360 - (300 if n == 5 else 0) + r_i * 30
        out.append(start + k * 130)  # leaves stop k when it starts moving toward stop k+1
    return sorted(out)


def main(data_dir: str) -> int:
    path = Path(data_dir) / "warehouse" / "ml" / "dataset"
    df = pd.concat([pd.read_parquet(f) for f in sorted(path.glob("month=*.parquet"))])
    problems = []
    if len(df) == 0:
        problems.append("no rows")
    for row in df.itertuples():
        t = int(row.rider_ts.timestamp())
        deps = [d for d in departures(row.service_date, row.line, row.origin) if d >= t]
        if not deps:
            problems.append(f"{row.origin}: rider at {row.rider_ts} has no later train")
            continue
        want_wait = deps[0] - t
        stops = ROUTES[row.line].index(row.destination) - ROUTES[row.line].index(row.origin)
        checks = {
            "wait_s": (row.wait_s, want_wait),
            "ride_s": (row.ride_s, 130 * stops),
            "journey_s": (row.journey_s, want_wait + 130 * stops),
            "sched_ride_s": (row.sched_ride_s, 120 * stops),
            "sched_headway_trunk_s": (row.sched_headway_trunk_s, 360),
        }
        for name, (got, want) in checks.items():
            if got != want:
                problems.append(
                    f"{row.line} {row.origin}->{row.destination} {name}: {got} != {want}"
                )
    bunched = int((df["wait_s"] <= 60).sum())
    status = "FAIL" if problems else "PASS"
    print(
        f"[{status}] {len(df)} rider rows recomputed independently; "
        f"{bunched} caught a bunched train"
    )
    for p in problems[:10]:
        print("   ", p)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "data_sample"))
