resource "aws_s3_bucket" "data_lake" {
  bucket = "my-ecommerce-mlops-bucket"
  # In production, enable versioning, logging, etc.
}

resource "aws_s3_bucket_public_access_block" "data_lake_access_block" {
  bucket = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
