import json
from datetime import UTC, datetime

import boto3
import pytest
from conftest import Reply
from moto import mock_aws
from transitiq_pipelines.lambdas import ingest

NOW = datetime(2026, 11, 6, 13, 5, 9, tzinfo=UTC)


def test_build_key_layout():
    key = ingest.build_key("heartbeat", NOW, "abcd1234")
    assert (
        key == "source=heartbeat/ingest_date=2026-11-06/heartbeat_20261106T130509Z_abcd1234.jsonl"
    )


@pytest.mark.parametrize("bad", ["", "Heartbeat", "../etc", "a b", "x" * 65])
def test_build_key_rejects_bad_source(bad):
    with pytest.raises(ValueError):
        ingest.build_key(bad, NOW, "abcd1234")


def test_ndjson_one_line_per_record():
    data = ingest.to_ndjson([{"b": 1, "a": 2}, {"c": 3}]).decode()
    assert data.splitlines() == ['{"a":2,"b":1}', '{"c":3}']
    assert data.endswith("\n")


@mock_aws
def test_handler_writes_heartbeat_to_bronze(aws_env, monkeypatch):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="bronze-test")
    monkeypatch.setenv("BRONZE_BUCKET", "bronze-test")

    result = ingest.handler({"source": "heartbeat"}, None)

    assert result["records"] == 1
    obj = s3.get_object(Bucket="bronze-test", Key=result["key"])
    rows = [json.loads(line) for line in obj["Body"].read().decode().splitlines()]
    assert rows[0]["source"] == "heartbeat"
    meta = json.loads(
        s3.get_object(Bucket="bronze-test", Key=result["key"] + ".meta.json")["Body"].read()
    )
    assert meta["source"] == "heartbeat" and meta["rows"] == 1


@mock_aws
def test_handler_never_overwrites(aws_env, monkeypatch):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="bronze-test")
    monkeypatch.setenv("BRONZE_BUCKET", "bronze-test")

    keys = {ingest.handler({"source": "heartbeat"}, None)["key"] for _ in range(3)}

    assert len(keys) == 3
    assert s3.list_objects_v2(Bucket="bronze-test")["KeyCount"] == 6  # 3 files + 3 sidecars


@mock_aws
def test_handler_with_supplied_records(aws_env, monkeypatch):
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="bronze-test")
    monkeypatch.setenv("BRONZE_BUCKET", "bronze-test")
    records = [{"record_id": "a"}, {"record_id": "b"}]
    assert ingest.handler({"source": "e2e_sample", "records": records}, None)["records"] == 2


def test_unknown_source_without_records_fails(monkeypatch):
    monkeypatch.setenv("BRONZE_BUCKET", "bronze-test")
    with pytest.raises(ValueError, match="no fetcher"):
        ingest.handler({"source": "mbta_alerts"}, None)


@mock_aws
def test_handler_v3_alerts_snapshot(aws_env, monkeypatch, fake_server):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="bronze-test")
    monkeypatch.setenv("BRONZE_BUCKET", "bronze-test")
    monkeypatch.setenv("MBTA_V3_BASE_URL", fake_server.base_url)
    monkeypatch.setenv("MBTA_API_KEY", "k")
    fake_server.add("/alerts", Reply(200, b'{"data": [{"id": "a1"}]}'))

    result = ingest.handler({"source": "mbta_v3_alerts"}, None)

    assert result["records"] == 1
    assert result["key"].startswith("source=mbta_v3_alerts/snapshot_date=")
    assert s3.get_object(Bucket="bronze-test", Key=result["key"])["Body"].read() == (
        b'{"data": [{"id": "a1"}]}'
    )
    assert fake_server.hits("/alerts")[0]["headers"]["x-api-key"] == "k"
