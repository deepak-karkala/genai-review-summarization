resource "aws_iam_role" "airflow_worker_role" {
  name = "airflow-worker-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "ec2.amazonaws.com" # Assuming MWAA workers run on EC2
        },
        Action = "sts:AssumeRole",
      },
    ]
  })
}

resource "aws_iam_policy" "airflow_pipeline_policy" {
  name        = "AirflowDataIngestionPolicy"
  description = "Policy for the data ingestion pipeline"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow",
        Action   = ["rds-data:ExecuteStatement", "rds:DescribeDBInstances"],
        Resource = "arn:aws:rds:eu-west-1:123456789012:db:source-db-identifier"
      },
      {
        Effect   = "Allow",
        Action   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "airflow_attach" {
  role       = aws_iam_role.airflow_worker_role.name
  policy_arn = aws_iam_policy.airflow_pipeline_policy.arn
}