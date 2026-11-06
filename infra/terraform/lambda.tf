locals {
  lambda_src = "${path.module}/../../pipelines/lambdas"
  functions = {
    ingest   = "${local.name_prefix}-ingest"
    validate = "${local.name_prefix}-validate"
  }
}

data "aws_caller_identity" "current" {}

# ---------- packaging (stdlib + boto3 only, so no dependency layer is needed) ----------
data "archive_file" "ingest" {
  type        = "zip"
  source_dir  = "${local.lambda_src}/ingest"
  output_path = "${path.module}/build/ingest.zip"
  excludes    = ["__pycache__"]
}

data "archive_file" "validate" {
  type        = "zip"
  source_dir  = "${local.lambda_src}/validate"
  output_path = "${path.module}/build/validate.zip"
  excludes    = ["__pycache__"]
}

# ---------- log groups (created first so Lambda never needs logs:CreateLogGroup) ----------
resource "aws_cloudwatch_log_group" "ingest" {
  name              = "/aws/lambda/${local.functions.ingest}"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "validate" {
  name              = "/aws/lambda/${local.functions.validate}"
  retention_in_days = var.log_retention_days
}

# ---------- functions ----------
resource "aws_lambda_function" "ingest" {
  function_name    = local.functions.ingest
  role             = aws_iam_role.ingest.arn
  runtime          = var.lambda_runtime
  handler          = "handler.handler"
  filename         = data.archive_file.ingest.output_path
  source_code_hash = data.archive_file.ingest.output_base64sha256
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      BRONZE_BUCKET = aws_s3_bucket.lake["bronze"].bucket
      LOG_LEVEL     = "INFO"
    }
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.dlq.arn
  }

  depends_on = [aws_cloudwatch_log_group.ingest, aws_iam_role_policy.ingest]
}

resource "aws_lambda_function" "validate" {
  function_name    = local.functions.validate
  role             = aws_iam_role.validate.arn
  runtime          = var.lambda_runtime
  handler          = "handler.handler"
  filename         = data.archive_file.validate.output_path
  source_code_hash = data.archive_file.validate.output_base64sha256
  timeout          = 120
  memory_size      = 512

  environment {
    variables = {
      SILVER_BUCKET     = aws_s3_bucket.lake["silver"].bucket
      QUARANTINE_BUCKET = aws_s3_bucket.lake["quarantine"].bucket
      LOG_LEVEL         = "INFO"
    }
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.dlq.arn
  }

  depends_on = [aws_cloudwatch_log_group.validate, aws_iam_role_policy.validate]
}

# Async invocations (S3 events, EventBridge) retry twice, then go to the DLQ.
resource "aws_lambda_function_event_invoke_config" "ingest" {
  function_name                = aws_lambda_function.ingest.function_name
  maximum_retry_attempts       = 2
  maximum_event_age_in_seconds = 3600
}

resource "aws_lambda_function_event_invoke_config" "validate" {
  function_name                = aws_lambda_function.validate.function_name
  maximum_retry_attempts       = 2
  maximum_event_age_in_seconds = 3600
}

resource "aws_lambda_permission" "s3_invoke_validate" {
  statement_id   = "AllowBronzeBucketInvoke"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.validate.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.lake["bronze"].arn
  source_account = data.aws_caller_identity.current.account_id
}
