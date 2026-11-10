"""Data quality checks that run after every dbt build (Airflow task `quality_checks`).

dbt tests check each model in isolation (uniqueness, types, relationships). These checks look
across layers and over time, the way an on-call engineer would:

    freshness       is the newest subway service day recent enough?
    volume          is the newest day's row count in line with the previous 28 days?
    reconciliation  bronze rows == silver rows in (per day); gold facts vs silver rows
    quarantine      overall quarantine rate of subway events
    duplicates      duplicate segments in gold.fact_travel_time
    nulls           share of segments without a scheduled time
    coverage        weather hours available for the subway date range

Each check is pass / warn / fail. Results are appended to DuckDB table ops.quality_results
(published to Postgres) and written to data/quality/<timestamp>.json. Any "fail" makes the
command exit 1, which fails the Airflow task.
"""

from __future__ import annotations

import json
import re
import statistics
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

from transitiq_pipelines.checkpoint import Checkpoint
from transitiq_pipelines.storage import LocalBronzeStore

PASS, WARN, FAIL = "pass", "warn", "fail"


@dataclass
class CheckResult:
    check: str
    status: str
    value: float | None
    threshold: str
    detail: str


def _status(bad: bool, warn: bool) -> str:
    return FAIL if bad else WARN if warn else PASS


def check_freshness(conn, today: date, warn_days: int = 2, fail_days: int = 7) -> CheckResult:
    newest = conn.execute("select max(service_date) from silver.subway_events").fetchone()[0]
    if newest is None:
        return CheckResult("freshness", FAIL, None, f"<= {warn_days} days", "no subway events")
    lag = (today - newest).days
    return CheckResult(
        "freshness",
        _status(lag > fail_days, lag > warn_days),
        float(lag),
        f"warn > {warn_days} d, fail > {fail_days} d",
        f"newest service_date {newest}, {lag} day(s) before {today}",
    )


def check_volume(conn, low_warn=0.7, high_warn=1.5, low_fail=0.3) -> CheckResult:
    """Newest day vs the median of the same weekday over the previous 4 weeks.

    Comparing a Sunday with all days flagged every Sunday (fewer trains run); the first Airflow
    run did exactly that. Falls back to all recent days when fewer than 2 same-weekday days exist.
    """
    rows = conn.execute(
        "select service_date, count(*) from silver.subway_events "
        "group by 1 order by 1 desc limit 29"
    ).fetchall()
    if len(rows) < 8:
        return CheckResult("volume", WARN, None, "needs >= 8 days", f"only {len(rows)} day(s)")
    (latest_day, latest), history = rows[0], rows[1:]
    same = [n for d, n in history if d.isoweekday() == latest_day.isoweekday()]
    basis, label = (
        (same, f"{latest_day:%A}s") if len(same) >= 2 else ([n for _, n in history], "days")
    )
    median = statistics.median(basis)
    ratio = latest / median if median else 0.0
    return CheckResult(
        "volume",
        _status(ratio < low_fail, ratio < low_warn or ratio > high_warn),
        round(ratio, 3),
        f"warn outside {low_warn}-{high_warn}x, fail < {low_fail}x of recent median",
        f"{latest_day}: {latest:,} rows vs median {median:,.0f} of {len(basis)} recent {label}",
    )


def check_bronze_silver(store: LocalBronzeStore, checkpoint_dir: Path) -> CheckResult:
    """Every silver day must have exactly as many input rows as its bronze file's sidecar says."""
    parts = Checkpoint(checkpoint_dir, "silver_subway_events").partitions()
    mismatches, checked = [], 0
    for day, info in parts.items():
        key = info.get("signature")
        if not key or not store.exists(key):
            continue
        bronze_rows = store.read_meta(key).get("rows")
        if bronze_rows is None:
            continue
        checked += 1
        if int(bronze_rows) != int(info.get("rows_in", -1)):
            mismatches.append(f"{day}: bronze {bronze_rows} vs silver {info.get('rows_in')}")
    return CheckResult(
        "reconciliation_bronze_silver",
        FAIL if mismatches else (WARN if checked == 0 else PASS),
        float(len(mismatches)),
        "0 mismatched days",
        f"{checked} day(s) compared" + (f"; {mismatches[:5]}" if mismatches else ""),
    )


def check_silver_gold(conn, low=0.8, high=1.0) -> CheckResult:
    silver, segments = conn.execute(
        "select (select count(*) from silver.subway_events), "
        "(select count(*) from gold.fact_travel_time)"
    ).fetchone()
    ratio = segments / silver if silver else 0.0
    return CheckResult(
        "reconciliation_silver_gold",
        _status(ratio > high or silver == 0, ratio < low),
        round(ratio, 4),
        f"segments / events in {low}-{high}",
        f"{segments:,} gold segments from {silver:,} silver events (first stops have no segment)",
    )


def check_quarantine(checkpoint_dir: Path, warn=0.01, fail=0.05) -> CheckResult:
    parts = Checkpoint(checkpoint_dir, "silver_subway_events").partitions().values()
    rows_in = sum(p.get("rows_in", 0) for p in parts)
    bad = sum(p.get("quarantined", 0) for p in parts)
    rate = bad / rows_in if rows_in else 0.0
    return CheckResult(
        "quarantine_rate",
        _status(rate > fail, rate > warn),
        round(rate, 6),
        f"warn > {warn:.0%}, fail > {fail:.0%}",
        f"{bad:,} of {rows_in:,} subway rows quarantined",
    )


def check_duplicates(conn) -> CheckResult:
    dups = conn.execute(
        "select count(*) from (select 1 from gold.fact_travel_time "
        "group by service_date, trip_id, vehicle_id, to_stop_id, departure_ts having count(*) > 1)"
    ).fetchone()[0]
    return CheckResult(
        "duplicates", _status(dups > 0, False), float(dups), "0", "duplicate segments in gold"
    )


def check_nulls(conn, warn=0.15, fail=0.5) -> CheckResult:
    share = conn.execute(
        "select avg(case when scheduled_seconds is null then 1.0 else 0.0 end) "
        "from gold.fact_travel_time"
    ).fetchone()[0]
    share = float(share or 0.0)
    return CheckResult(
        "nulls_scheduled_time",
        _status(share > fail, share > warn),
        round(share, 4),
        f"warn > {warn:.0%}, fail > {fail:.0%}",
        "share of segments without a scheduled travel time",
    )


def check_weather_coverage(conn, warn=0.95) -> CheckResult:
    covered, needed = conn.execute(
        """
        with span as (
            select min(service_date) as lo, max(service_date) as hi from silver.subway_events
        )
        select
            (select count(*) from gold.dim_weather_hour, span
              where local_date between span.lo and span.hi),
            (select (date_diff('day', lo, hi) + 1) * 24 from span)
        """
    ).fetchone()
    share = covered / needed if needed else 0.0
    return CheckResult(
        "weather_coverage",
        _status(False, share < warn),
        round(share, 4),
        f"warn < {warn:.0%}",
        f"{covered:,} of {needed or 0:,} hours have weather",
    )


def run_checks(db_path: Path, data_dir: Path, today: date | None = None) -> list[CheckResult]:
    today = today or datetime.now(UTC).date()
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        results = [
            check_freshness(conn, today),
            check_volume(conn),
            check_bronze_silver(LocalBronzeStore(data_dir / "bronze"), data_dir / "checkpoints"),
            check_silver_gold(conn),
            check_quarantine(data_dir / "checkpoints"),
            check_duplicates(conn),
            check_nulls(conn),
            check_weather_coverage(conn),
        ]
    finally:
        conn.close()
    return results


def record(results: list[CheckResult], db_path: Path, out_dir: Path) -> Path:
    """Append results to ops.quality_results (DuckDB) and write a JSON report."""
    run_at = datetime.now(UTC)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("create schema if not exists ops")
        conn.execute(
            """
            create table if not exists ops.quality_results (
                run_at timestamptz, check_name varchar, status varchar, value double,
                threshold varchar, detail varchar
            )
            """
        )
        conn.executemany(
            "insert into ops.quality_results values (?, ?, ?, ?, ?, ?)",
            [(run_at, r.check, r.status, r.value, r.threshold, r.detail) for r in results],
        )
    finally:
        conn.close()
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = re.sub(r"[^0-9TZ]", "", run_at.strftime("%Y%m%dT%H%M%SZ"))
    path = out_dir / f"quality_{stamp}.json"
    payload = {"run_at": run_at.isoformat(), "results": [asdict(r) for r in results]}
    path.write_text(json.dumps(payload, indent=2), "utf-8")
    return path
