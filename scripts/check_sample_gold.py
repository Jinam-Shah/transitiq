"""Assert gold results on the synthetic sample (CI, after `dbt build` on data_sample).

The sample is built so the right answers are known (pipelines/.../silver/sample.py): six bus
trips with fates ran, ran (8 min late), ran, ghost, canceled, partial. This checks the dbt
logic end to end, not just that it runs.

    python scripts/check_sample_gold.py data_sample/warehouse/transitiq.duckdb
"""

import sys

import duckdb


def main(db: str) -> int:
    c = duckdb.connect(db, read_only=True)
    failures = []

    def expect(label, actual, expected):
        ok = actual == expected
        print(
            f"[{'PASS' if ok else 'FAIL'}] {label}: {actual!r}"
            + ("" if ok else f" (expected {expected!r})")
        )
        if not ok:
            failures.append(label)

    statuses = dict(c.sql("select trip_id, status from gold.fact_ghost_trip").fetchall())
    expect(
        "ghost-trip statuses",
        statuses,
        {
            "SB1-0": "ran",
            "SB1-1": "ran",
            "SB1-2": "ran",
            "SB1-3": "ghost",
            "SB1-4": "canceled",
            "SB1-5": "partial",
        },
    )
    arrived = {t for (t,) in c.sql("select distinct trip_id from gold.fact_bus_arrival").fetchall()}
    expect("ghost/canceled trips have no arrivals", arrived & {"SB1-3", "SB1-4"}, set())
    bunches = c.sql(
        "select count(*), bool_and(after_long_gap), min(headway_seconds) "
        "from gold.fact_bunching_event where mode = 'bus'"
    ).fetchone()
    expect(
        "bus bunches (one per stop reached, after a long gap, 120 s apart)", bunches, (3, True, 120)
    )
    rate = c.sql(
        "select sum(ghost_trips), sum(judged_trips), sum(canceled_trips) "
        "from gold.mart_reliability_route_hour where route_id = 'SB1'"
    ).fetchone()
    expect("mart ghost / judged / canceled", tuple(int(x) for x in rate), (1, 5, 1))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "data_sample/warehouse/transitiq.duckdb"))
