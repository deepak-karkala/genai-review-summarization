resource "aws_db_subnet_group" "vector_db_subnet" {
  name       = "vector-db-subnet-group"
  subnet_ids = ["subnet-xxxx", "subnet-yyyy"] # Replace with your private subnet IDs
}

resource "aws_secretsmanager_secret" "db_credentials" {
  name = "aurora/vector_db/credentials"
}

resource "aws_rds_cluster" "vector_db" {
  cluster_identifier      = "vector-db-cluster"
  engine                  = "aurora-postgresql"
  engine_mode             = "provisioned" # Serverless v2 is configured within the instance
  database_name           = "vectordb"
  master_username         = "postgres"
  master_password         = aws_secretsmanager_secret.db_credentials.secret_string
  db_subnet_group_name    = aws_db_subnet_group.vector_db_subnet.name
  vpc_security_group_ids  = ["sg-zzzz"] # Security group allowing access from Airflow workers
  skip_final_snapshot     = true

  serverlessv2_scaling_configuration {
    min_capacity = 0.5
    max_capacity = 4.0
  }
}

# Note: The pgvector extension must be enabled on the database itself.
# This can be done with a one-time SQL command: CREATE EXTENSION IF NOT EXISTS vector;