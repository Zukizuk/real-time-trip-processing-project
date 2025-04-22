# S3
output "bucket_name" {
  value = aws_s3_bucket.my_bucket.bucket
}

# Kinesis Streams
output "trip_start_stream_name" {
  value = aws_kinesis_stream.trip_start_stream.name
}

output "trip_end_stream_name" {
  value = aws_kinesis_stream.trip_end_stream.name
}

output "trip_start_stream_arn" {
  value = aws_kinesis_stream.trip_start_stream.arn
}

output "trip_end_stream_arn" {
  value = aws_kinesis_stream.trip_end_stream.arn
}

# DynamoDB
output "trip_table_name" {
  value = aws_dynamodb_table.trip_table.name
}

# IAM
output "lambda_role_arn" {
  value = aws_iam_role.lambda_role.arn
}

# Lambda
output "trip_start_lambda_arn" {
  value = aws_lambda_function.trip_start_lambda.arn
}

