"""End-to-end check of the Phase 1 stack on LocalStack (NOT real AWS).

Prerequisites: LocalStack running (docker compose up -d localstack) and
`terraform -chdir=infra/terraform apply` done.

Checks:
  1. IAM: deployed role policies pass the least-privilege static checks.
  2. Ingest: invoking the ingest Lambda writes a bronze file, which the S3 event
     turns into a silver file automatically.
  3. Sample file: tests/fixtures/sample_bronze.jsonl (3 valid + 4 bad lines) uploaded to
     bronze produces exactly 3 silver rows and 4 quarantined rows with reasons.
  4. Idempotency: uploading the same file to the same key again leaves 3 + 4 rows.
  5. DLQ: the dead-letter queue exists and is empty.

Prints a JSON summary with measured timings; copy real values into METRICS.md.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from iam_check import allowed_actions, policy_violations  # noqa: E402

TF_DIR = ROOT / "infra" / "terraform"
FIXTURE = ROOT / "tests" / "fixtures" / "sample_bronze.jsonl"
EXPECTED_VALID = 3
EXPECTED_REASONS = sorted(
    ["invalid_json", "missing_field:observed_at", "invalid_timestamp", "duplicate_record_id"]
)


def tf_outputs() -> dict:
    exe = shutil.which("terraform") or "terraform"
    out = subprocess.run(  # noqa: S603 (fixed args)
        [exe, f"-chdir={TF_DIR}", "output", "-json"], check=True, capture_output=True, text=True
    ).stdout
    return {k: v["value"] for k, v in json.loads(out).items()}


def client(service: str):
    return boto3.client(
        service,
        endpoint_url=os.getenv("LOCALSTACK_ENDPOINT", "http://localhost:4566"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id="test",
        aws_secret_access_key="test",  # noqa: S106 (LocalStack dummy)
        config=Config(read_timeout=300, retries={"max_attempts": 2}),
    )


def wait_for_object(s3, bucket: str, key: str, timeout: float = 180.0) -> float:
    """Poll until the object exists. Returns seconds waited."""
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            s3.head_object(Bucket=bucket, Key=key)
            return time.monotonic() - start
        except s3.exceptions.ClientError:
            time.sleep(1)
    raise TimeoutError(f"s3://{bucket}/{key} did not appear within {timeout}s")


def read_ndjson(s3, bucket: str, key: str) -> list[dict]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def check(condition: bool, message: str, failures: list[str]) -> None:
    print(f"[{'PASS' if condition else 'FAIL'}] {message}")
    if not condition:
        failures.append(message)


def main() -> int:
    out = tf_outputs()
    s3, lam, iam, sqs = client("s3"), client("lambda"), client("iam"), client("sqs")
    failures: list[str] = []
    summary: dict = {"run_at": datetime.now(UTC).isoformat(), "environment": "LocalStack"}

    # 1. IAM least privilege (static: LocalStack free plan does not enforce IAM)
    forbidden = {
        out["ingest_role"]: {"s3:GetObject", "s3:DeleteObject"},
        out["validate_role"]: set(),
    }
    for role, extra_forbidden in forbidden.items():
        for name in iam.list_role_policies(RoleName=role)["PolicyNames"]:
            doc = iam.get_role_policy(RoleName=role, PolicyName=name)["PolicyDocument"]
            if isinstance(doc, str):
                doc = json.loads(doc)
            problems = policy_violations(doc, extra_forbidden)
            check(not problems, f"IAM {role}/{name} is least-privilege {problems or ''}", failures)
        attached = iam.list_attached_role_policies(RoleName=role)["AttachedPolicies"]
        check(not attached, f"IAM {role} has no attached managed policies", failures)
    ingest_doc = iam.get_role_policy(
        RoleName=out["ingest_role"], PolicyName="ingest-least-privilege"
    )["PolicyDocument"]
    if isinstance(ingest_doc, str):
        ingest_doc = json.loads(ingest_doc)
    check(
        "s3:getobject" not in allowed_actions(ingest_doc),
        "ingest role cannot read bronze",
        failures,
    )

    # 2. Ingest Lambda -> bronze -> (S3 event) -> validate Lambda -> silver
    t0 = time.monotonic()
    resp = lam.invoke(
        FunctionName=out["ingest_function"], Payload=json.dumps({"source": "heartbeat"})
    )
    payload = json.loads(resp["Payload"].read())
    check("FunctionError" not in resp, f"ingest Lambda ran ({payload})", failures)
    if "FunctionError" not in resp:
        summary["ingest_invoke_seconds"] = round(time.monotonic() - t0, 2)
        waited = wait_for_object(s3, out["silver_bucket"], payload["key"])
        summary["heartbeat_bronze_to_silver_seconds"] = round(waited, 2)
        rows = read_ndjson(s3, out["silver_bucket"], payload["key"])
        check(
            len(rows) == 1 and rows[0]["_source_file"].endswith(payload["key"]),
            "heartbeat row reached silver with lineage",
            failures,
        )

    # 3. Sample file with known good and bad lines
    key = "source=e2e_sample/ingest_date=2026-11-01/e2e_sample_fixture.jsonl"
    for attempt in (1, 2):  # attempt 2 = idempotency replay
        s3.delete_object(Bucket=out["silver_bucket"], Key=key)
        s3.put_object(Bucket=out["bronze_bucket"], Key=key, Body=FIXTURE.read_bytes())
        waited = wait_for_object(s3, out["silver_bucket"], key)
        wait_for_object(s3, out["quarantine_bucket"], key, timeout=30)
        silver = read_ndjson(s3, out["silver_bucket"], key)
        quarantined = read_ndjson(s3, out["quarantine_bucket"], key)
        label = "first load" if attempt == 1 else "replay (idempotency)"
        check(
            len(silver) == EXPECTED_VALID,
            f"{label}: {len(silver)} silver rows (expect 3)",
            failures,
        )
        reasons = sorted(r["_reason"] for r in quarantined)
        check(reasons == EXPECTED_REASONS, f"{label}: quarantine reasons {reasons}", failures)
        check(
            len({r["record_id"] for r in silver}) == len(silver),
            f"{label}: no duplicate ids",
            failures,
        )
        check(
            all({"_source_file", "_line_number", "_loaded_at"} <= r.keys() for r in silver),
            f"{label}: lineage columns on every silver row",
            failures,
        )
        if attempt == 1:
            summary["fixture_bronze_to_silver_seconds"] = round(waited, 2)
            total = len(silver) + len(quarantined)
            summary["fixture_quarantine_rate"] = (
                round(len(quarantined) / total, 4) if total else None
            )

    # 5. DLQ exists and is empty
    attrs = sqs.get_queue_attributes(
        QueueUrl=out["dlq_url"], AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]
    check(attrs.get("ApproximateNumberOfMessages") == "0", "dead-letter queue is empty", failures)

    summary["checks_failed"] = len(failures)
    print(json.dumps(summary, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
