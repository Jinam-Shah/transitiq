resource "aws_cloudwatch_event_rule" "ingest_schedule" {
  name                = "${local.name_prefix}-ingest-schedule"
  description         = "Runs the ingest Lambda on a schedule."
  schedule_expression = var.ingest_schedule
}

resource "aws_cloudwatch_event_target" "ingest" {
  rule  = aws_cloudwatch_event_rule.ingest_schedule.name
  arn   = aws_lambda_function.ingest.arn
  input = jsonencode({ source = "heartbeat" })
}

resource "aws_lambda_permission" "eventbridge_invoke_ingest" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.ingest_schedule.arn
}
