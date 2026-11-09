"""Open-Meteo historical hourly weather for Boston (free, non-commercial, CC BY 4.0).

https://archive-api.open-meteo.com/v1/archive  (Historical Weather API)

Fetched one whole calendar month per request (a year = 12 calls; the free limit is
10,000/day). Ranges are widened to whole months.
Times are requested in UTC (timezone=GMT) to avoid daylight-saving ambiguity.

The archive lags real time by several days, so a month is only "complete" once it ended at
least ARCHIVE_LAG_DAYS ago. Complete months are stored once:
    source=open_meteo_hourly/month=YYYY-MM/open_meteo_boston_YYYY-MM.json
Incomplete months get a dated partial file that a later run supersedes:
    source=open_meteo_hourly/month=YYYY-MM/open_meteo_boston_YYYY-MM_partial_YYYYMMDD.json
"""

from __future__ import annotations

import calendar
import json
import logging
from datetime import date, timedelta

from transitiq_pipelines.runlog import RunStats
from transitiq_pipelines.sources import Context
from transitiq_pipelines.storage import BronzeExists

log = logging.getLogger(__name__)

SOURCE = "open_meteo_hourly"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
BOSTON = (42.3601, -71.0589)  # downtown Boston
HOURLY_VARS = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "snowfall",
    "snow_depth",
    "weather_code",
    "wind_speed_10m",
    "wind_gusts_10m",
)
ARCHIVE_LAG_DAYS = 7


def month_chunks(start: date, end: date) -> list[tuple[str, date, date, date]]:
    """[(YYYY-MM, chunk_start, chunk_end, month_end)] covering start..end."""
    chunks = []
    cur = start.replace(day=1)
    while cur <= end:
        month_end = cur.replace(day=calendar.monthrange(cur.year, cur.month)[1])
        chunks.append((f"{cur:%Y-%m}", max(cur, start), min(month_end, end), month_end))
        cur = month_end + timedelta(days=1)
    return chunks


def run(ctx: Context, start: date, end: date, url: str = ARCHIVE_URL) -> RunStats:
    stats = RunStats(SOURCE)
    ckpt = ctx.checkpoint(SOURCE)
    latest_available = ctx.today - timedelta(days=2)
    end = min(end, latest_available)
    # Always fetch whole calendar months: a window like "last 14 days" would otherwise store a
    # mid-month slice, and a month could never get its "complete" file.
    start = start.replace(day=1)
    for month, c_start, c_end, month_end in month_chunks(start, end):
        complete = c_start.day == 1 and c_end == month_end
        complete = complete and month_end <= ctx.today - timedelta(days=ARCHIVE_LAG_DAYS)
        prev = ckpt.get(month)
        if prev and prev.get("complete") and ctx.store.exists(prev["key"]):
            stats.add_skipped()
            continue
        suffix = "" if complete else f"_partial_{ctx.today:%Y%m%d}"
        key = f"source={SOURCE}/month={month}/open_meteo_boston_{month}{suffix}.json"
        if ctx.store.exists(key):
            stats.add_skipped()
            continue
        params = {
            "latitude": BOSTON[0],
            "longitude": BOSTON[1],
            "start_date": c_start.isoformat(),
            "end_date": c_end.isoformat(),
            "hourly": ",".join(HOURLY_VARS),
            "timezone": "GMT",
        }
        try:
            resp = ctx.http.get(url, params=params)
            payload = json.loads(resp.body)
            hours = payload.get("hourly", {}).get("time")
            if not isinstance(hours, list) or not hours:
                raise ValueError("response has no hourly.time array")
            meta = {
                "source": SOURCE,
                "source_url": url,
                "params": params,
                "month": month,
                "complete": complete,
                "fetched_at": ctx.now.isoformat(),
                "bytes": len(resp.body),
                "rows": len(hours),
            }
            try:
                ctx.store.put_bytes(key, resp.body, meta)
                stats.add_fetched(len(hours), len(resp.body))
            except BronzeExists:
                stats.add_skipped()
            ckpt.mark(month, key=key, complete=complete, rows=len(hours))
        except Exception as exc:
            log.error("failed %s: %s", month, exc)
            stats.add_failure(month, exc)
    return stats.finish()
