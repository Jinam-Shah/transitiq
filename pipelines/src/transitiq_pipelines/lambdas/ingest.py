"""Ingest Lambda: writes raw files to the bronze bucket.

Triggered by EventBridge schedules (or invoked manually). Event shapes:
    {"source": "mbta_v3_alerts"}        live alerts snapshot from the MBTA V3 API
    {"source": "mbta_v3_facilities"}    elevator/escalator list from the MBTA V3 API
    {"source": "heartbeat"}             one synthetic NDJSON record (pipeline self-test)
    {"source": "e2e_sample", "records": [{...}, ...]}   caller-supplied NDJSON records

Bronze is immutable: every call writes a NEW key and the S3 write is conditional
(IfNoneMatch="*"), so nothing is ever overwritten.
NDJSON keys: source=<source>/ingest_date=<YYYY-MM-DD>/<source>_<YYYYmmddTHHMMSSZ>_<id>.jsonl
"""

from __future__ import annotations

import json
import logging
import os
import re
import uuid
from datetime import UTC, datetime

from transitiq_pipelines.http import HttpClient, RetryPolicy
from transitiq_pipelines.sources import mbta_v3
from transitiq_pipelines.storage import S3BronzeStore

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

SOURCE_PATTERN = re.compile(r"^[a-z0-9_]{1,64}$")
V3_SOURCES = {mbta_v3.source_name(n): n for n in mbta_v3.ENDPOINTS}


def build_key(source: str, now: datetime, unique: str) -> str:
    if not SOURCE_PATTERN.match(source):
        raise ValueError(f"invalid source name: {source!r} (use lowercase letters, digits, _)")
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    return f"source={source}/ingest_date={now:%Y-%m-%d}/{source}_{stamp}_{unique}.jsonl"


def heartbeat_records(now: datetime) -> list[dict]:
    return [
        {
            "record_id": f"heartbeat-{now:%Y%m%dT%H%M%SZ}",
            "source": "heartbeat",
            "observed_at": now.isoformat(),
            "payload": {"status": "ok"},
        }
    ]


def to_ndjson(records: list[dict]) -> bytes:
    return (
        "\n".join(json.dumps(r, separators=(",", ":"), sort_keys=True) for r in records) + "\n"
    ).encode("utf-8")


def handler(event, context):
    now = datetime.now(UTC)
    event = event or {}
    source = event.get("source", "heartbeat")
    store = S3BronzeStore(os.environ["BRONZE_BUCKET"])

    if source in V3_SOURCES:
        http = HttpClient(policy=RetryPolicy(max_attempts=3, base_delay=2.0, max_delay=20.0))
        result = mbta_v3.snapshot(
            store,
            http,
            V3_SOURCES[source],
            now,
            api_key=os.getenv("MBTA_API_KEY") or None,
            base_url=os.getenv("MBTA_V3_BASE_URL", mbta_v3.BASE_URL),
        )
        out = {"bucket": store.bucket, "key": result["key"], "records": result["rows"]}
        logger.info(json.dumps({"event": "bronze_written", "source": source, **out}))
        return out

    records = event.get("records")
    if records is None:
        if source != "heartbeat":
            raise ValueError(f"no fetcher for source {source!r} and no records supplied")
        records = heartbeat_records(now)
    if not isinstance(records, list) or not records:
        raise ValueError("records must be a non-empty list")

    key = build_key(source, now, uuid.uuid4().hex[:8])
    store.put_bytes(
        key,
        to_ndjson(records),
        {"source": source, "fetched_at": now.isoformat(), "rows": len(records)},
    )
    out = {"bucket": store.bucket, "key": key, "records": len(records)}
    logger.info(json.dumps({"event": "bronze_written", **out}))
    return out
