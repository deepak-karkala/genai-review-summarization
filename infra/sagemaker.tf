resource "aws_iam_role" "sagemaker_execution_role" {
  name = "SageMakerExecutionRole"
  # Assume role policy for SageMaker service
}

resource "aws_iam_policy" "sagemaker_policy" {
  name = "SageMakerPolicy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow",
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
        Resource = ["arn:aws:s3:::my-ecommerce-mlops-bucket/*"]
      },
      {
        Effect   = "Allow",
        Action   = ["ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage", "ecr:BatchCheckLayerAvailability"],
        Resource = aws_ecr_repository.training_repo.arn
      }
      # Plus CloudWatch logs permissions, etc.
    ]
  })
}

resource "aws_iam_role_policy_attachment" "sagemaker_attach" {
  role       = aws_iam_role.sagemaker_execution_role.name
  policy_arn = aws_iam_policy.sagemaker_policy.arn
}

resource "aws_ecr_repository" "training_repo" {
  name = "llm-finetuning-image"
}