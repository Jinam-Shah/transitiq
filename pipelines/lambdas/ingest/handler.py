"""Ingest Lambda: writes one raw NDJSON file to the bronze bucket.

Triggered by an EventBridge schedule (or invoked manually). Phase 1 only knows the
"heartbeat" source plus caller-supplied records; real MBTA/weather fetchers arrive in Phase 2.

Event shapes:
    {"source": "heartbeat"}
    {"source": "e2e_sample", "records": [{...}, {...}]}

Bronze is immutable: every call writes a NEW key (timestamp + random suffix), never overwrites.
Key layout: source=<source>/ingest_date=<YYYY-MM-DD>/<source>_<YYYYmmddTHHMMSSZ>_<id>.jsonl
"""

from __future__ import annotations

import json
import logging
import os
import re
import uuid
from datetime import UTC, datetime

import boto3

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

SOURCE_PATTERN = re.compile(r"^[a-z0-9_]{1,64}$")


def _s3():
    return boto3.client("s3")


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
    source = (event or {}).get("source", "heartbeat")
    records = event.get("records") if event else None
    if records is None:
        if source != "heartbeat":
            raise ValueError(f"no fetcher for source {source!r} yet and no records supplied")
        records = heartbeat_records(now)
    if not isinstance(records, list) or not records:
        raise ValueError("records must be a non-empty list")

    bucket = os.environ["BRONZE_BUCKET"]
    key = build_key(source, now, uuid.uuid4().hex[:8])
    _s3().put_object(
        Bucket=bucket,
        Key=key,
        Body=to_ndjson(records),
        ContentType="application/x-ndjson",
        Metadata={"ingested-at": now.isoformat(), "source": source},
    )
    result = {"bucket": bucket, "key": key, "records": len(records)}
    logger.info(json.dumps({"event": "bronze_written", **result}))
    return result
