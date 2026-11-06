locals {
  pipelines_src = "${path.module}/../../pipelines/src"
  functions = {
    ingest   = "${local.name_prefix}-ingest"
    validate = "${local.name_prefix}-validate"
  }
}

data "aws_caller_identity" "current" {}

# ---------- packaging ----------
# One zip of the transitiq_pipelines package serves both functions (different handlers).
# The code Lambda imports is stdlib + boto3 only, so no dependency layer is needed;
# pyarrow is imported lazily and only by local backfills.
data "archive_file" "pipelines" {
  type        = "zip"
  source_dir  = local.pipelines_src
  output_path = "${path.module}/build/pipelines.zip"
  excludes    = ["**/__pycache__/**", "**/*.pyc", "*.egg-info/**"]
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
  handler          = "transitiq_pipelines.lambdas.ingest.handler"
  filename         = data.archive_file.pipelines.output_path
  source_code_hash = data.archive_file.pipelines.output_base64sha256
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      BRONZE_BUCKET = aws_s3_bucket.lake["bronze"].bucket
      MBTA_API_KEY  = var.mbta_api_key
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
  handler          = "transitiq_pipelines.lambdas.validate.handler"
  filename         = data.archive_file.pipelines.output_path
  source_code_hash = data.archive_file.pipelines.output_base64sha256
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
