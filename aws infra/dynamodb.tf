resource "aws_dynamodb_table" "trip_table" {
  name     = "trip-table"
  hash_key = "trip_id"
  attribute {
    name = "trip_id"
    type = "S"
  }
  billing_mode = "PAY_PER_REQUEST"
  tags = {
    "project" = "project-7"
  }
}
