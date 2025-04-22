resource "aws_lambda_function" "trip_start_lambda" {
  function_name = "trip-start-lambda"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.9"
  role          = aws_iam_role.lambda_role.arn
  filename      = "lambda/lambda_function.zip"
}

resource "aws_lambda_event_source_mapping" "trip_start_event_source_mapping" {
  event_source_arn                   = aws_kinesis_stream.trip_start_stream.arn
  function_name                      = aws_lambda_function.trip_start_lambda.arn
  starting_position                  = "LATEST"
  batch_size                         = 100
  maximum_batching_window_in_seconds = 100
}
