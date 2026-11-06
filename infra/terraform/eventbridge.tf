# One schedule per ingest source; each sends {"source": "<name>"} to the ingest Lambda.
resource "aws_cloudwatch_event_rule" "ingest" {
  for_each = var.ingest_schedules

  name                = "${local.name_prefix}-ingest-${replace(each.key, "_", "-")}"
  description         = "Runs the ingest Lambda for source ${each.key}."
  schedule_expression = each.value
}

resource "aws_cloudwatch_event_target" "ingest" {
  for_each = var.ingest_schedules

  rule  = aws_cloudwatch_event_rule.ingest[each.key].name
  arn   = aws_lambda_function.ingest.arn
  input = jsonencode({ source = each.key })
}

resource "aws_lambda_permission" "eventbridge_invoke_ingest" {
  for_each = var.ingest_schedules

  statement_id  = "AllowEventBridgeInvoke-${replace(each.key, "_", "-")}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.ingest[each.key].arn
}
