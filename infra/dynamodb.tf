resource "aws_dynamodb_table" "summary_cache" {
  name           = "ProductSummaryCache"
  billing_mode   = "PAY_PER_REQUEST" # Best for spiky, infrequent workloads
  hash_key       = "product_id"

  attribute {
    name = "product_id"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = {
    Project = "ReviewSummarization"
  }
}