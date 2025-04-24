# Give EventBridge permission to invoke the Lambda
resource "aws_lambda_permission" "allow_eventbridge_invoke" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.aggregate_kpi_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_aggregation_schedule.arn
}

# Scheduled EventBridge Rule (runs every day at midnight UTC)
resource "aws_cloudwatch_event_rule" "daily_aggregation_schedule" {
  name                = "daily-aggregation-metrics"
  schedule_expression = "cron(0 0 * * ? *)" # Midnight UTC every day
  state               = "DISABLED"          # Turn it on after testing
  tags = {
    "project" = "project-7"
  }
}

# Connect the rule to your Lambda
resource "aws_cloudwatch_event_target" "trigger_lambda" {
  rule      = aws_cloudwatch_event_rule.daily_aggregation_schedule.name
  target_id = "SendToLambda"
  arn       = aws_lambda_function.aggregate_kpi_lambda.arn
}
