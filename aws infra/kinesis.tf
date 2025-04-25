resource "aws_kinesis_stream" "trip_start_stream" {
  name                = "trip-start-stream"
  shard_count         = 1
  retention_period    = 24
  shard_level_metrics = ["IncomingBytes", "OutgoingBytes"]
  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
  tags = {
    "project" = "project-7"
  }
}

resource "aws_kinesis_stream" "trip_end_stream" {
  name                = "trip-end-stream"
  shard_count         = 1
  retention_period    = 24
  shard_level_metrics = ["IncomingBytes", "OutgoingBytes"]
  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
  tags = {
    "project" = "project-7"
  }
}
