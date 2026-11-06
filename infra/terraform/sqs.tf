# Dead-letter queue: events that still fail after Lambda's async retries land here,
# so no bronze file is silently skipped.
resource "aws_sqs_queue" "dlq" {
  name                      = "${local.name_prefix}-pipeline-dlq"
  message_retention_seconds = 1209600 # 14 days, the SQS maximum
  sqs_managed_sse_enabled   = true
}
