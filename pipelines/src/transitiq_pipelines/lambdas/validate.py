"""Validate Lambda: bronze NDJSON -> silver (valid rows) + quarantine (rejected rows).

Triggered by S3 ObjectCreated events on the bronze bucket.

Rules (Phase 1 generic envelope; per-source schemas arrive in Phase 3), with the
rejection reason in brackets:
    - line is a JSON object                 [invalid_json]
    - record_id, source: non-empty strings  [missing_field:<f> / invalid_type:<f>]
    - observed_at: ISO-8601 with timezone   [missing_field:observed_at / invalid_timestamp]
    - payload: JSON object                  [missing_field:payload / invalid_type:payload]
    - record_id unique within the file      [duplicate_record_id]
Blank lines are ignored.

Idempotency: the silver and quarantine keys are derived from the bronze key, so reprocessing
the same bronze file overwrites the same outputs and never creates duplicates.

Lineage: every silver row gets _source_file, _line_number and _loaded_at.
Files that cannot be read at all (e.g. not UTF-8) raise, so Lambda retries and then sends the
event to the SQS dead-letter queue.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from urllib.parse import unquote_plus

import boto3

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

REQUIRED_STRINGS = ("record_id", "source")


def _s3():
    return boto3.client("s3")


@dataclass
class ValidationResult:
    valid: list[dict] = field(default_factory=list)
    rejected: list[dict] = field(default_factory=list)


def _parse_ts(value) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None


def check_record(record) -> str | None:
    """Return a rejection reason, or None if the record is valid."""
    if not isinstance(record, dict):
        return "invalid_json"
    for name in REQUIRED_STRINGS:
        if name not in record or record[name] in (None, ""):
            return f"missing_field:{name}"
        if not isinstance(record[name], str):
            return f"invalid_type:{name}"
    if "observed_at" not in record or record["observed_at"] in (None, ""):
        return "missing_field:observed_at"
    if not _parse_ts(record["observed_at"]):
        return "invalid_timestamp"
    if "payload" not in record or record["payload"] is None:
        return "missing_field:payload"
    if not isinstance(record["payload"], dict):
        return "invalid_type:payload"
    return None


def validate_lines(text: str, source_file: str, loaded_at: str) -> ValidationResult:
    result = ValidationResult()
    seen: set[str] = set()
    for line_number, raw in enumerate(text.splitlines(), start=1):
        if not raw.strip():
            continue
        try:
            record = json.loads(raw)
        except json.JSONDecodeError:
            record, reason = None, "invalid_json"
        else:
            reason = check_record(record)
        if reason is None and record["record_id"] in seen:
            reason = "duplicate_record_id"
        lineage = {
            "_source_file": source_file,
            "_line_number": line_number,
            "_loaded_at": loaded_at,
        }
        if reason is None:
            seen.add(record["record_id"])
            result.valid.append({**record, **lineage})
        else:
            result.rejected.append({"_reason": reason, "_raw": raw, **lineage})
    return result


def _ndjson(rows: list[dict]) -> bytes:
    return "".join(
        json.dumps(r, separators=(",", ":"), sort_keys=True) + "\n" for r in rows
    ).encode("utf-8")


def process_object(bucket: str, key: str) -> dict:
    s3 = _s3()
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    text = body.decode("utf-8")  # UnicodeDecodeError -> retry -> DLQ
    loaded_at = datetime.now(UTC).isoformat()
    result = validate_lines(text, f"s3://{bucket}/{key}", loaded_at)

    silver_bucket = os.environ["SILVER_BUCKET"]
    quarantine_bucket = os.environ["QUARANTINE_BUCKET"]
    s3.put_object(
        Bucket=silver_bucket,
        Key=key,
        Body=_ndjson(result.valid),
        ContentType="application/x-ndjson",
    )
    if result.rejected:
        s3.put_object(
            Bucket=quarantine_bucket,
            Key=key,
            Body=_ndjson(result.rejected),
            ContentType="application/x-ndjson",
        )
    else:
        # A previous run of an identical key may have quarantined rows; keep outputs consistent.
        s3.delete_object(Bucket=quarantine_bucket, Key=key)

    total = len(result.valid) + len(result.rejected)
    summary = {
        "source_file": f"s3://{bucket}/{key}",
        "valid": len(result.valid),
        "quarantined": len(result.rejected),
        "quarantine_rate": round(len(result.rejected) / total, 4) if total else 0.0,
    }
    logger.info(json.dumps({"event": "validated", **summary}))
    return summary


def handler(event, context):
    summaries = []
    for rec in event.get("Records", []):
        bucket = rec["s3"]["bucket"]["name"]
        key = unquote_plus(rec["s3"]["object"]["key"])
        summaries.append(process_object(bucket, key))
    return {"files": summaries}
