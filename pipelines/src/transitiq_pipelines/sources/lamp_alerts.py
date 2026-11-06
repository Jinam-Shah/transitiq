"""MBTA LAMP real-time alerts history: one Parquet file with every alert version since 2019.

https://performancedata.mbta.com/lamp/tableau/alerts/LAMP_RT_ALERTS.parquet (~130 MB)

Includes effect_detail (e.g. ELEVATOR_CLOSURE, ESCALATOR_CLOSURE) and informed_entity
facility_id, so it is the history behind Accessibility Watch and fact_alert.

The file is replaced upstream in place, so we HEAD it first and download only when
Last-Modified changed and the previous snapshot is older than min_interval_days.
    source=lamp_alerts/snapshot_date=YYYY-MM-DD/LAMP_RT_ALERTS_v<last_modified>.parquet
"""

from __future__ import annotations

from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime

from transitiq_pipelines.runlog import RunStats
from transitiq_pipelines.sources import Context, parquet_rows, version_tag
from transitiq_pipelines.storage import BronzeExists

SOURCE = "lamp_alerts"
URL = "https://performancedata.mbta.com/lamp/tableau/alerts/LAMP_RT_ALERTS.parquet"


def run(ctx: Context, min_interval_days: int = 7, force: bool = False, url: str = URL) -> RunStats:
    stats = RunStats(SOURCE)
    ckpt = ctx.checkpoint(SOURCE)
    try:
        headers = ctx.http.head(url)
        last_modified = parsedate_to_datetime(headers["last-modified"])
        prev = ckpt.get("latest")
        if prev and not force:
            same = prev["last_modified"] == last_modified.isoformat()
            recent = ctx.now - datetime.fromisoformat(prev["fetched_at"]) < timedelta(
                days=min_interval_days
            )
            if (same or recent) and ctx.store.exists(prev["key"]):
                stats.add_skipped()
                return stats.finish()

        key = (
            f"source={SOURCE}/snapshot_date={ctx.today}/"
            f"LAMP_RT_ALERTS_v{version_tag(last_modified)}.parquet"
        )
        tmp = ctx.tmp_dir / f"{SOURCE}.parquet"
        try:
            dl = ctx.http.download(url, tmp)
            rows = parquet_rows(tmp)
            meta = {
                "source": SOURCE,
                "source_url": url,
                "upstream_last_modified": last_modified.isoformat(),
                "fetched_at": ctx.now.isoformat(),
                "sha256": dl["sha256"],
                "bytes": dl["bytes"],
                "rows": rows,
            }
            try:
                ctx.store.put_file(key, tmp, meta)
                stats.add_fetched(rows, dl["bytes"])
            except BronzeExists:
                stats.add_skipped()
            ckpt.mark(
                "latest",
                key=key,
                last_modified=last_modified.isoformat(),
                fetched_at=ctx.now.isoformat(),
                rows=rows,
            )
        finally:
            tmp.unlink(missing_ok=True)
    except Exception as exc:
        stats.add_failure("latest", exc)
    return stats.finish()
