"""Static least-privilege checks for IAM policy documents.

LocalStack's free plan does not enforce IAM, so instead of trusting enforcement we inspect the
deployed policies and fail if any statement is broader than intended.
"""

from __future__ import annotations


def _as_list(value) -> list:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def policy_violations(doc: dict, forbidden_actions: set[str] | None = None) -> list[str]:
    """Return human-readable problems with a policy document (empty list = OK).

    Rules:
      - no Effect other than Allow (Deny statements would hint at an over-broad Allow elsewhere)
      - no NotAction / NotResource
      - no action containing a wildcard ("*", "s3:*", "s3:Get*")
      - no resource that is exactly "*"
      - none of the explicitly forbidden actions for this role
    """
    forbidden = {a.lower() for a in (forbidden_actions or set())}
    problems: list[str] = []
    for i, stmt in enumerate(_as_list(doc.get("Statement"))):
        sid = stmt.get("Sid", f"#{i}")
        if stmt.get("Effect") != "Allow":
            problems.append(f"{sid}: unexpected Effect {stmt.get('Effect')!r}")
        for key in ("NotAction", "NotResource"):
            if key in stmt:
                problems.append(f"{sid}: uses {key}")
        for action in _as_list(stmt.get("Action")):
            if "*" in action:
                problems.append(f"{sid}: wildcard action {action!r}")
            if action.lower() in forbidden:
                problems.append(f"{sid}: forbidden action {action!r}")
        for resource in _as_list(stmt.get("Resource")):
            if resource == "*":
                problems.append(f"{sid}: resource '*'")
    return problems


def allowed_actions(doc: dict) -> set[str]:
    return {
        a.lower()
        for stmt in _as_list(doc.get("Statement"))
        if stmt.get("Effect") == "Allow"
        for a in _as_list(stmt.get("Action"))
    }
