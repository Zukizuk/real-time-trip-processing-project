resource "aws_s3_bucket" "my_bucket" {
  bucket = "your-bucket"

  tags = {
    "project" = "project-7"
  }
}



