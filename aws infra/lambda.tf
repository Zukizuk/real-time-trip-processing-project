resource "aws_lambda_function" "trip_start_lambda" {
  function_name = "trip-start-lambda"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.9"
  role          = aws_iam_role.lambda_role.arn
  filename      = "lambda/trip_start/lambda_function.zip"
  tags = {
    "project" = "project-7"
  }
}

resource "aws_lambda_function" "trip_end_lambda" {
  function_name = "trip-end-lambda"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.9"
  role          = aws_iam_role.lambda_role.arn
  filename      = "lambda/trip_end/lambda_function.zip"
  tags = {
    "project" = "project-7"
  }
}

resource "aws_lambda_event_source_mapping" "trip_start_event_source_mapping" {
  event_source_arn                   = aws_kinesis_stream.trip_start_stream.arn
  function_name                      = aws_lambda_function.trip_start_lambda.arn
  starting_position                  = "LATEST"
  batch_size                         = 50
  maximum_batching_window_in_seconds = 50
  tags = {
    "project" = "project-7"
  }
}

resource "aws_lambda_event_source_mapping" "trip_end_event_source_mapping" {
  event_source_arn                   = aws_kinesis_stream.trip_end_stream.arn
  function_name                      = aws_lambda_function.trip_end_lambda.arn
  starting_position                  = "LATEST"
  batch_size                         = 50
  maximum_batching_window_in_seconds = 50
  tags = {
    "project" = "project-7"
  }
}

resource "aws_lambda_function" "aggregate_kpi_lambda" {
  function_name = "aggregate-kpi-lambda"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.9"
  role          = aws_iam_role.lambda_role.arn
  filename      = "lambda/aggregate_kpi/lambda_function.zip"
  tags = {
    "project" = "project-7"
  }
}

