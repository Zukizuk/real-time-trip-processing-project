resource "aws_s3_bucket" "my_bucket" {
  bucket = "nsp-bolt-metrics-zuki"

  tags = {
    "project" = "project-7"
  }
}



