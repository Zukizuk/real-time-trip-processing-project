resource "aws_dynamodb_table" "trip_table" {
  name     = "trip-table"
  hash_key = "trip_id"
  attribute {
    name = "trip_id"
    type = "S"
  }
  attribute {
    name = "trip_id"
    type = "S"
  }

  attribute {
    name = "completion_date"
    type = "S"
  }

  attribute {
    name = "trip_status"
    type = "S"
  }

  global_secondary_index {
    name            = "CompletionDateStatusIndex"
    hash_key        = "completion_date"
    range_key       = "trip_status"
    projection_type = "ALL"
  }

  billing_mode = "PAY_PER_REQUEST"
  tags = {
    "project" = "project-7"
  }
}
