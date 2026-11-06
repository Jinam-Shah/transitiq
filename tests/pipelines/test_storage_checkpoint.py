import json
import threading

import boto3
import pytest
from moto import mock_aws
from transitiq_pipelines.checkpoint import Checkpoint
from transitiq_pipelines.runlog import RunStats
from transitiq_pipelines.storage import (
    BronzeExists,
    LocalBronzeStore,
    S3BronzeStore,
    validate_key,
)

KEY = "source=x/service_date=2026-11-04/file_v20261105T000000Z.parquet"


@pytest.mark.parametrize(
    "bad",
    ["", "/abs", "a/../b", "a//b", "a\\b", "C:/x", "a b", "a/./b", "x/"],
)
def test_invalid_keys_rejected(bad):
    with pytest.raises(ValueError):
        validate_key(bad)


def test_local_put_and_metadata(tmp_path):
    store = LocalBronzeStore(tmp_path)
    store.put_bytes(KEY, b"data", {"rows": 3})
    assert store.exists(KEY)
    assert store.path(KEY).read_bytes() == b"data"
    assert store.read_meta(KEY) == {"key": KEY, "rows": 3}


def test_local_never_overwrites(tmp_path):
    store = LocalBronzeStore(tmp_path)
    store.put_bytes(KEY, b"first", {})
    with pytest.raises(BronzeExists):
        store.put_bytes(KEY, b"second", {})
    assert store.path(KEY).read_bytes() == b"first"


def test_local_put_file_and_iter_meta(tmp_path):
    src = tmp_path / "src.bin"
    src.write_bytes(b"xyz")
    store = LocalBronzeStore(tmp_path / "bronze")
    store.put_file(KEY, src, {"rows": 1})
    metas = list(store.iter_meta())
    assert [m["key"] for m in metas] == [KEY]
    assert not list((tmp_path / "bronze").rglob("*.tmp"))


@mock_aws
def test_s3_store_conditional_write(aws_env):
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="bronze-test")
    store = S3BronzeStore("bronze-test", client)
    assert not store.exists(KEY)
    store.put_bytes(KEY, b"data", {"rows": 2})
    assert store.exists(KEY)
    with pytest.raises(BronzeExists):
        store.put_bytes(KEY, b"other", {})
    body = client.get_object(Bucket="bronze-test", Key=KEY)["Body"].read()
    assert body == b"data"
    meta = json.loads(
        client.get_object(Bucket="bronze-test", Key=KEY + ".meta.json")["Body"].read()
    )
    assert meta["rows"] == 2


def test_checkpoint_persists_and_resumes(tmp_path):
    ck = Checkpoint(tmp_path, "src")
    ck.mark("2026-11-04", key="k", rows=5)
    again = Checkpoint(tmp_path, "src")
    assert again.get("2026-11-04") == {"key": "k", "rows": 5}
    assert again.get("2026-11-05") is None
    assert not (tmp_path / "src.json.tmp").exists()


def test_checkpoint_is_thread_safe(tmp_path):
    ck = Checkpoint(tmp_path, "src")
    threads = [
        threading.Thread(target=ck.mark, args=(f"p{i}",), kwargs={"i": i}) for i in range(50)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(Checkpoint(tmp_path, "src").partitions()) == 50


def test_runstats_write(tmp_path):
    stats = RunStats("src")
    stats.add_fetched(rows=10, nbytes=100)
    stats.add_skipped()
    stats.add_failure("2026-11-01", ValueError("boom"))
    path = stats.finish().write(tmp_path)
    data = json.loads(path.read_text())
    assert data["fetched"] == 1 and data["skipped"] == 1 and data["rows"] == 10
    assert data["failures"] == [{"partition": "2026-11-01", "error": "boom"}]
    assert data["duration_seconds"] is not None
    assert not stats.ok
