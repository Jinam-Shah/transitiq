"""Bronze storage: immutable raw files with the same key layout on disk and in S3.

    LocalBronzeStore  data/bronze/<key>              durable history on the laptop
    S3BronzeStore     s3://<bucket>/<key>            Lambda on LocalStack (or real AWS)

Every object gets a sidecar "<key>.meta.json" with lineage: source URL, fetch time, sha256,
byte count and row count. Writing a key that already exists raises BronzeExists, so bronze is
never overwritten (callers treat that as "already ingested").
"""

from __future__ import annotations

import json
import os
import re
import shutil
from pathlib import Path
from typing import Protocol

KEY_PATTERN = re.compile(r"^[A-Za-z0-9=._\-]+(/[A-Za-z0-9=._\-]+)*$")
META_SUFFIX = ".meta.json"


class BronzeExists(Exception):
    pass


def validate_key(key: str) -> str:
    """Keys must be portable to S3 and Windows paths: no '..', ':', backslashes, or spaces."""
    if not KEY_PATTERN.match(key) or any(part in ("", ".", "..") for part in key.split("/")):
        raise ValueError(f"invalid bronze key: {key!r}")
    return key


class BronzeStore(Protocol):
    def exists(self, key: str) -> bool: ...
    def put_file(self, key: str, src: Path, metadata: dict) -> str: ...
    def put_bytes(self, key: str, data: bytes, metadata: dict) -> str: ...


class LocalBronzeStore:
    def __init__(self, root: Path):
        self.root = Path(root)

    def path(self, key: str) -> Path:
        return self.root.joinpath(*validate_key(key).split("/"))

    def uri(self, key: str) -> str:
        return self.path(key).as_posix()

    def exists(self, key: str) -> bool:
        return self.path(key).exists()

    def _write_meta(self, key: str, metadata: dict) -> None:
        meta = self.path(key + META_SUFFIX)
        tmp = meta.with_name(meta.name + ".tmp")
        tmp.write_text(json.dumps({"key": key, **metadata}, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, meta)

    def put_file(self, key: str, src: Path, metadata: dict) -> str:
        dest = self.path(key)
        if dest.exists():
            raise BronzeExists(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".tmp")
        shutil.copyfile(src, tmp)
        self._write_meta(key, metadata)
        os.replace(tmp, dest)  # data last: a key is only "present" once fully written
        return self.uri(key)

    def put_bytes(self, key: str, data: bytes, metadata: dict) -> str:
        dest = self.path(key)
        if dest.exists():
            raise BronzeExists(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".tmp")
        tmp.write_bytes(data)
        self._write_meta(key, metadata)
        os.replace(tmp, dest)
        return self.uri(key)

    def read_meta(self, key: str) -> dict:
        return json.loads(self.path(key + META_SUFFIX).read_text("utf-8"))

    def iter_meta(self, prefix: str = ""):
        base = self.root / prefix if prefix else self.root
        if not base.exists():
            return
        for meta in sorted(base.rglob("*" + META_SUFFIX)):
            data_file = meta.with_name(meta.name[: -len(META_SUFFIX)])
            if data_file.exists():
                yield json.loads(meta.read_text("utf-8"))


class S3BronzeStore:
    def __init__(self, bucket: str, client=None):
        self.bucket = bucket
        if client is None:
            import boto3  # lazy: not needed for local runs

            client = boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT_URL") or None)
        self.client = client

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=validate_key(key))
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def _put(self, key: str, body, metadata: dict) -> str:
        from botocore.exceptions import ClientError

        validate_key(key)
        try:
            # IfNoneMatch="*" makes the write fail if the key exists: atomic immutability,
            # and the writer needs no read permission on the bucket.
            self.client.put_object(Bucket=self.bucket, Key=key, Body=body, IfNoneMatch="*")
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("PreconditionFailed", "412"):
                raise BronzeExists(key) from exc
            raise
        meta_body = json.dumps({"key": key, **metadata}, sort_keys=True).encode("utf-8")
        self.client.put_object(Bucket=self.bucket, Key=key + META_SUFFIX, Body=meta_body)
        return self.uri(key)

    def put_file(self, key: str, src: Path, metadata: dict) -> str:
        with Path(src).open("rb") as fh:
            return self._put(key, fh, metadata)

    def put_bytes(self, key: str, data: bytes, metadata: dict) -> str:
        return self._put(key, data, metadata)
