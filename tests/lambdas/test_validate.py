import json
from pathlib import Path

import boto3
import pytest
from conftest import load_module
from moto import mock_aws

validate = load_module("pipelines/lambdas/validate/handler.py", "validate_handler")

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "sample_bronze.jsonl"
GOOD = {"record_id": "r1", "source": "s", "observed_at": "2026-11-01T08:00:00Z", "payload": {}}


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({}, None),
        ({"record_id": ""}, "missing_field:record_id"),
        ({"record_id": 7}, "invalid_type:record_id"),
        ({"source": None}, "missing_field:source"),
        ({"observed_at": "2026-11-01T08:00:00"}, "invalid_timestamp"),  # no timezone
        ({"observed_at": "not a date"}, "invalid_timestamp"),
        ({"observed_at": 1730448000}, "invalid_timestamp"),
        ({"payload": None}, "missing_field:payload"),
        ({"payload": [1, 2]}, "invalid_type:payload"),
    ],
)
def test_check_record_rules(change, reason):
    assert validate.check_record({**GOOD, **change}) == reason


def test_missing_observed_at():
    record = {k: v for k, v in GOOD.items() if k != "observed_at"}
    assert validate.check_record(record) == "missing_field:observed_at"


def test_non_object_json_is_invalid():
    result = validate.validate_lines("[1,2,3]\n42\n", "s3://b/k", "t")
    assert [r["_reason"] for r in result.rejected] == ["invalid_json", "invalid_json"]


def test_fixture_counts_and_reasons():
    result = validate.validate_lines(
        FIXTURE.read_text(), "s3://bronze/k", "2026-11-06T00:00:00+00:00"
    )
    assert [r["record_id"] for r in result.valid] == ["r1", "r2", "r6"]
    assert sorted(r["_reason"] for r in result.rejected) == sorted(
        ["invalid_json", "missing_field:observed_at", "invalid_timestamp", "duplicate_record_id"]
    )


def test_lineage_columns():
    result = validate.validate_lines(json.dumps(GOOD), "s3://bronze/k", "LOADED")
    row = result.valid[0]
    assert row["_source_file"] == "s3://bronze/k"
    assert row["_line_number"] == 1
    assert row["_loaded_at"] == "LOADED"


def test_blank_lines_ignored():
    text = "\n" + json.dumps(GOOD) + "\n\n   \n"
    result = validate.validate_lines(text, "f", "t")
    assert len(result.valid) == 1 and not result.rejected


def _s3_event(bucket, key):
    return {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": key}}}]}


@pytest.fixture
def buckets(aws_env, monkeypatch):
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        for b in ("bronze", "silver", "quarantine"):
            s3.create_bucket(Bucket=b)
        monkeypatch.setenv("SILVER_BUCKET", "silver")
        monkeypatch.setenv("QUARANTINE_BUCKET", "quarantine")
        yield s3


def _rows(s3, bucket, key):
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode()
    return [json.loads(line) for line in body.splitlines()]


def test_handler_splits_silver_and_quarantine(buckets):
    key = "source=e2e_sample/ingest_date=2026-11-01/f.jsonl"
    buckets.put_object(Bucket="bronze", Key=key, Body=FIXTURE.read_bytes())

    out = validate.handler(_s3_event("bronze", key), None)

    assert out["files"][0]["valid"] == 3
    assert out["files"][0]["quarantined"] == 4
    assert out["files"][0]["quarantine_rate"] == round(4 / 7, 4)
    assert len(_rows(buckets, "silver", key)) == 3
    assert len(_rows(buckets, "quarantine", key)) == 4


def test_handler_is_idempotent(buckets):
    key = "source=x/ingest_date=2026-11-01/f.jsonl"
    buckets.put_object(Bucket="bronze", Key=key, Body=FIXTURE.read_bytes())
    for _ in range(3):
        validate.handler(_s3_event("bronze", key), None)
    assert len(_rows(buckets, "silver", key)) == 3
    assert len(_rows(buckets, "quarantine", key)) == 4
    assert buckets.list_objects_v2(Bucket="silver")["KeyCount"] == 1


def test_handler_decodes_url_encoded_keys(buckets):
    key = "source=x/ingest_date=2026-11-01/with space.jsonl"
    buckets.put_object(Bucket="bronze", Key=key, Body=json.dumps(GOOD).encode())
    validate.handler(_s3_event("bronze", key.replace(" ", "+")), None)
    assert len(_rows(buckets, "silver", key)) == 1


def test_clean_file_has_no_quarantine_object(buckets):
    key = "source=x/ingest_date=2026-11-01/clean.jsonl"
    buckets.put_object(Bucket="bronze", Key=key, Body=json.dumps(GOOD).encode())
    validate.handler(_s3_event("bronze", key), None)
    assert buckets.list_objects_v2(Bucket="quarantine")["KeyCount"] == 0


def test_unreadable_file_raises_for_dlq(buckets):
    key = "source=x/ingest_date=2026-11-01/binary.jsonl"
    buckets.put_object(Bucket="bronze", Key=key, Body=b"\xff\xfe\x00bad")
    with pytest.raises(UnicodeDecodeError):
        validate.handler(_s3_event("bronze", key), None)
