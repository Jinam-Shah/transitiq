"""Phase 0 smoke test: checks that the local Docker services are up.

Usage:
    python scripts/smoke_test.py                 # checks localstack, postgres, airflow
    python scripts/smoke_test.py --skip airflow  # e.g. in CI

Uses only the Python standard library so it runs before any dependencies are installed.
Exit code 0 means every checked service is healthy.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str


def http_json(url: str, timeout: float = 5.0) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310 (local URLs only)
        return json.loads(resp.read().decode("utf-8"))


def check_localstack(endpoint: str, required: tuple[str, ...]) -> CheckResult:
    try:
        health = http_json(f"{endpoint.rstrip('/')}/_localstack/health")
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
        return CheckResult("localstack", False, f"unreachable: {exc}")
    services = health.get("services", {})
    bad = [s for s in required if services.get(s) not in ("available", "running")]
    if bad:
        states = {s: services.get(s, "missing") for s in bad}
        return CheckResult("localstack", False, f"services not ready: {states}")
    return CheckResult("localstack", True, f"edition={health.get('edition')}, services ok")


def check_tcp(name: str, host: str, port: int, timeout: float = 3.0) -> CheckResult:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return CheckResult(name, True, f"accepting connections on {host}:{port}")
    except OSError as exc:
        return CheckResult(name, False, f"{host}:{port} not reachable: {exc}")


def check_airflow(base_url: str) -> CheckResult:
    try:
        health = http_json(f"{base_url.rstrip('/')}/api/v2/monitor/health")
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
        return CheckResult("airflow", False, f"unreachable: {exc}")
    db = health.get("metadatabase", {}).get("status")
    sched = health.get("scheduler", {}).get("status")
    ok = db == "healthy" and sched == "healthy"
    return CheckResult("airflow", ok, f"metadatabase={db}, scheduler={sched}")


def with_retries(fn: Callable[[], CheckResult], attempts: int, delay: float) -> CheckResult:
    result = fn()
    for _ in range(attempts - 1):
        if result.ok:
            break
        time.sleep(delay)
        result = fn()
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip", action="append", default=[], help="service to skip")
    parser.add_argument("--attempts", type=int, default=10)
    parser.add_argument("--delay", type=float, default=6.0)
    args = parser.parse_args(argv)

    checks: dict[str, Callable[[], CheckResult]] = {
        "localstack": lambda: check_localstack(
            os.getenv("LOCALSTACK_ENDPOINT", "http://localhost:4566"),
            ("s3", "lambda", "sqs", "events", "iam"),
        ),
        "postgres": lambda: check_tcp(
            "postgres", "localhost", int(os.getenv("POSTGRES_HOST_PORT", "5433"))
        ),
        "airflow": lambda: check_airflow(
            f"http://localhost:{os.getenv('AIRFLOW_HOST_PORT', '8080')}"
        ),
    }

    failed = False
    for name, fn in checks.items():
        if name in args.skip:
            print(f"[SKIP] {name}")
            continue
        result = with_retries(fn, args.attempts, args.delay)
        print(f"[{'PASS' if result.ok else 'FAIL'}] {result.name}: {result.detail}")
        failed = failed or not result.ok
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
