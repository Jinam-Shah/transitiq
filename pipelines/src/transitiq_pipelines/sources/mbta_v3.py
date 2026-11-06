"""MBTA V3 API snapshots: current alerts and the elevator/escalator facility list.

https://api-v3.mbta.com  (free key: 1,000 requests/minute; without a key the limit is lower,
20/minute at the time of writing per the x-ratelimit-limit header)

Each call stores the raw JSON:API response unchanged:
    source=mbta_v3_alerts/snapshot_date=YYYY-MM-DD/alerts_<YYYYmmddTHHMMSSZ>.json
    source=mbta_v3_facilities/snapshot_date=YYYY-MM-DD/facilities_<...>.json

Used by both the local CLI and the ingest Lambda (stdlib + boto3 only).
"""

from __future__ import annotations

import json

from transitiq_pipelines.http import HttpClient
from transitiq_pipelines.runlog import RunStats
from transitiq_pipelines.sources import Context, version_tag
from transitiq_pipelines.storage import BronzeExists, BronzeStore

BASE_URL = "https://api-v3.mbta.com"
ENDPOINTS = {
    "alerts": ("/alerts", {}),
    "facilities": ("/facilities", {"filter[type]": "ELEVATOR,ESCALATOR"}),
}


def source_name(name: str) -> str:
    return f"mbta_v3_{name}"


def snapshot(
    store: BronzeStore,
    http: HttpClient,
    name: str,
    now,
    api_key: str | None = None,
    base_url: str = BASE_URL,
) -> dict:
    if name not in ENDPOINTS:
        raise ValueError(f"unknown V3 endpoint {name!r}; choose from {sorted(ENDPOINTS)}")
    path, params = ENDPOINTS[name]
    headers = {"Accept": "application/vnd.api+json"}
    if api_key:
        headers["x-api-key"] = api_key
    resp = http.get(base_url + path, params=params, headers=headers)
    data = json.loads(resp.body).get("data")
    if not isinstance(data, list):
        raise ValueError(f"V3 {name} response has no data array")
    key = f"source={source_name(name)}/snapshot_date={now:%Y-%m-%d}/{name}_{version_tag(now)}.json"
    meta = {
        "source": source_name(name),
        "source_url": base_url + path,
        "params": params,
        "fetched_at": now.isoformat(),
        "bytes": len(resp.body),
        "rows": len(data),
        "ratelimit_limit": resp.headers.get("x-ratelimit-limit"),
    }
    uri = store.put_bytes(key, resp.body, meta)
    return {"key": key, "uri": uri, "rows": len(data), "bytes": len(resp.body)}


def run(ctx: Context, name: str, api_key: str | None = None, base_url: str = BASE_URL) -> RunStats:
    stats = RunStats(source_name(name))
    try:
        result = snapshot(ctx.store, ctx.http, name, ctx.now, api_key, base_url)
        stats.add_fetched(result["rows"], result["bytes"])
    except BronzeExists:
        stats.add_skipped()
    except Exception as exc:
        stats.add_failure(name, exc)
    return stats.finish()
