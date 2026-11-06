"""Unit tests for scripts/smoke_test.py (no Docker needed)."""

import importlib.util
import socket
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "smoke_test.py"
spec = importlib.util.spec_from_file_location("smoke_test", SCRIPT)
smoke = importlib.util.module_from_spec(spec)
sys.modules["smoke_test"] = smoke
spec.loader.exec_module(smoke)

REQUIRED = ("s3", "lambda", "sqs", "events", "iam")


def test_localstack_all_services_available(monkeypatch):
    payload = {"edition": "pro", "services": {s: "available" for s in REQUIRED}}
    monkeypatch.setattr(smoke, "http_json", lambda url, timeout=5.0: payload)
    result = smoke.check_localstack("http://localhost:4566", REQUIRED)
    assert result.ok


def test_localstack_missing_service_fails(monkeypatch):
    services = {s: "available" for s in REQUIRED}
    services["lambda"] = "error"
    monkeypatch.setattr(smoke, "http_json", lambda url, timeout=5.0: {"services": services})
    result = smoke.check_localstack("http://localhost:4566", REQUIRED)
    assert not result.ok
    assert "lambda" in result.detail


def test_localstack_unreachable(monkeypatch):
    def boom(url, timeout=5.0):
        raise OSError("connection refused")

    monkeypatch.setattr(smoke, "http_json", boom)
    assert not smoke.check_localstack("http://localhost:4566", REQUIRED).ok


@pytest.mark.parametrize(
    ("db", "sched", "expected"),
    [("healthy", "healthy", True), ("healthy", "unhealthy", False), (None, None, False)],
)
def test_airflow_health(monkeypatch, db, sched, expected):
    payload = {"metadatabase": {"status": db}, "scheduler": {"status": sched}}
    monkeypatch.setattr(smoke, "http_json", lambda url, timeout=5.0: payload)
    assert smoke.check_airflow("http://localhost:8080").ok is expected


def test_tcp_check_open_and_closed_port():
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]
    try:
        assert smoke.check_tcp("pg", "127.0.0.1", port).ok
    finally:
        server.close()
    assert not smoke.check_tcp("pg", "127.0.0.1", port, timeout=0.5).ok


def test_retries_stop_on_success():
    calls = []

    def flaky():
        calls.append(1)
        return smoke.CheckResult("x", len(calls) >= 2, "")

    assert smoke.with_retries(flaky, attempts=5, delay=0).ok
    assert len(calls) == 2


def test_repo_layout_exists():
    root = SCRIPT.parents[1]
    for folder in ("infra", "pipelines", "dbt", "ml", "api", "web", "docs"):
        assert (root / folder).is_dir(), folder
    for doc in ("README.md", "METRICS.md", "docs/DECISIONS.md", "docs/DATA_SOURCES.md"):
        assert (root / doc).is_file(), doc
