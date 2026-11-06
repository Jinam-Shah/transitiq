"""Summarise what is in local bronze: files, rows, bytes and days of history per source.

transitiq-ingest report          # table
transitiq-ingest report --json   # machine-readable, for METRICS.md
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path

from transitiq_pipelines.storage import LocalBronzeStore

DATE_PART = re.compile(
    r"/(?:service_date|snapshot_date|ingest_date|fetched_date)=(\d{4}-\d{2}-\d{2})/"
)
MONTH_PART = re.compile(r"/month=(\d{4}-\d{2})/")


def summarise(bronze_root: Path) -> dict:
    store = LocalBronzeStore(bronze_root)
    by_source: dict[str, dict] = defaultdict(
        lambda: {"files": 0, "rows": 0, "bytes": 0, "dates": set(), "months": set()}
    )
    for meta in store.iter_meta():
        key = meta["key"]
        source = key.split("/", 1)[0].removeprefix("source=")
        s = by_source[source]
        s["files"] += 1
        s["rows"] += int(meta.get("rows") or 0)
        s["bytes"] += int(meta.get("bytes") or 0)
        if m := DATE_PART.search("/" + key):
            s["dates"].add(m.group(1))
        if m := MONTH_PART.search("/" + key):
            s["months"].add(m.group(1))

    out = {}
    for source, s in sorted(by_source.items()):
        dates = sorted(s["dates"])
        out[source] = {
            "files": s["files"],
            "rows": s["rows"],
            "bytes": s["bytes"],
            "distinct_days": len(dates),
            "first_day": dates[0] if dates else None,
            "last_day": dates[-1] if dates else None,
            "months": sorted(s["months"]),
        }
    totals = {
        "files": sum(v["files"] for v in out.values()),
        "rows": sum(v["rows"] for v in out.values()),
        "bytes": sum(v["bytes"] for v in out.values()),
    }
    return {"sources": out, "totals": totals}


def latest_runs(runs_dir: Path) -> dict[str, dict]:
    latest: dict[str, dict] = {}
    if not runs_dir.exists():
        return latest
    for path in sorted(runs_dir.glob("*.json")):
        run = json.loads(path.read_text("utf-8"))
        latest[run["source"]] = run
    return latest


def format_table(summary: dict, runs: dict[str, dict]) -> str:
    header = f"{'source':<22}{'files':>7}{'rows':>14}{'MB':>10}{'days':>7}  range / last run"
    lines = [header, "-" * len(header)]
    for source, s in summary["sources"].items():
        span = (
            f"{s['first_day']}..{s['last_day']}" if s["first_day"] else ",".join(s["months"][-2:])
        )
        run = runs.get(source)
        run_txt = f"  (last run {run['duration_seconds']}s)" if run else ""
        lines.append(
            f"{source:<22}{s['files']:>7}{s['rows']:>14,}{s['bytes'] / 1e6:>10.1f}"
            f"{s['distinct_days']:>7}  {span}{run_txt}"
        )
    t = summary["totals"]
    lines.append("-" * len(header))
    lines.append(f"{'TOTAL':<22}{t['files']:>7}{t['rows']:>14,}{t['bytes'] / 1e6:>10.1f}")
    return "\n".join(lines)
