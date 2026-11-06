output "bronze_bucket" {
  value = aws_s3_bucket.lake["bronze"].bucket
}

output "silver_bucket" {
  value = aws_s3_bucket.lake["silver"].bucket
}

output "quarantine_bucket" {
  value = aws_s3_bucket.lake["quarantine"].bucket
}

output "dlq_url" {
  value = aws_sqs_queue.dlq.url
}

output "ingest_function" {
  value = aws_lambda_function.ingest.function_name
}

output "validate_function" {
  value = aws_lambda_function.validate.function_name
}

output "ingest_role" {
  value = aws_iam_role.ingest.name
}

output "validate_role" {
  value = aws_iam_role.validate.name
}

output "ingest_schedule_rules" {
  value = { for k, r in aws_cloudwatch_event_rule.ingest : k => r.name }
}
