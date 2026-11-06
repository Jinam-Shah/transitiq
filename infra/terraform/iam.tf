# Least-privilege roles: one per Lambda, each allowed only the exact actions on the exact
# resources it needs. No wildcards in actions; no "*" resources.
# NOTE: LocalStack's free plan stores but does not ENFORCE IAM. scripts/e2e_localstack.py
# statically checks the deployed policies instead (docs/DECISIONS.md D-007).

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# ---------- ingest ----------
resource "aws_iam_role" "ingest" {
  name               = "${local.name_prefix}-ingest-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "ingest" {
  statement {
    sid       = "WriteBronzeOnly"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.lake["bronze"].arn}/*"]
  }
  statement {
    sid       = "OwnLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.ingest.arn}:*"]
  }
  statement {
    sid       = "DeadLetter"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.dlq.arn]
  }
}

resource "aws_iam_role_policy" "ingest" {
  name   = "ingest-least-privilege"
  role   = aws_iam_role.ingest.id
  policy = data.aws_iam_policy_document.ingest.json
}

# ---------- validate ----------
resource "aws_iam_role" "validate" {
  name               = "${local.name_prefix}-validate-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "validate" {
  statement {
    sid       = "ReadBronze"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake["bronze"].arn}/*"]
  }
  statement {
    sid       = "WriteSilver"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.lake["silver"].arn}/*"]
  }
  statement {
    sid       = "WriteQuarantine"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake["quarantine"].arn}/*"]
  }
  statement {
    sid       = "OwnLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.validate.arn}:*"]
  }
  statement {
    sid       = "DeadLetter"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.dlq.arn]
  }
}

resource "aws_iam_role_policy" "validate" {
  name   = "validate-least-privilege"
  role   = aws_iam_role.validate.id
  policy = data.aws_iam_policy_document.validate.json
}
