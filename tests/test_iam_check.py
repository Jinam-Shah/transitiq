import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from iam_check import allowed_actions, policy_violations  # noqa: E402

GOOD = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Write",
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::b/*",
        },
        {
            "Sid": "Logs",
            "Effect": "Allow",
            "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
            "Resource": ["arn:aws:logs:us-east-1:000000000000:log-group:/aws/lambda/x:*"],
        },
    ],
}


def test_good_policy_passes():
    assert policy_violations(GOOD) == []


def _with(stmt):
    return {"Statement": [stmt]}


def test_wildcard_action_fails():
    doc = _with({"Effect": "Allow", "Action": "s3:*", "Resource": "arn:aws:s3:::b/*"})
    assert "wildcard action" in policy_violations(doc)[0]


def test_prefix_wildcard_action_fails():
    doc = _with({"Effect": "Allow", "Action": ["s3:Get*"], "Resource": "arn:aws:s3:::b/*"})
    assert policy_violations(doc)


def test_star_resource_fails():
    doc = _with({"Effect": "Allow", "Action": "s3:PutObject", "Resource": "*"})
    assert "resource '*'" in policy_violations(doc)[0]


def test_not_action_fails():
    doc = _with({"Effect": "Allow", "NotAction": "iam:*", "Resource": "arn:aws:s3:::b"})
    assert any("NotAction" in p for p in policy_violations(doc))


def test_forbidden_action_is_case_insensitive():
    problems = policy_violations(GOOD, {"S3:PUTOBJECT"})
    assert any("forbidden" in p for p in problems)


def test_allowed_actions_lowercased():
    assert allowed_actions(GOOD) == {"s3:putobject", "logs:createlogstream", "logs:putlogevents"}
