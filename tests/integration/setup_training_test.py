import logging
import pandas as pd
import boto3
import os

# --- Test Configuration ---
STAGING_BUCKET = os.environ["STAGING_S3_BUCKET"]
EXECUTION_DATE = "2025-01-01" # Matches the test execution date
SAMPLE_SIZE = 50 # Small sample for a quick test run

logging.basicConfig(level=logging.INFO)

def create_finetuning_test_data():
    """Creates a sample DataFrame for the fine-tuning test."""
    # This format matches what the SFTTrainer expects in our train.py script
    data = {
        "text": [
            f"###Instruction: Summarize these reviews. ###Input: Test review text {i}. ###Response: Ideal test summary {i}."
            for i in range(SAMPLE_SIZE)
        ]
    }
    return pd.DataFrame(data)

def upload_data_to_s3(df: pd.DataFrame):
    """Saves data locally and uploads it to the correct S3 path for the DAG."""
    s3_key = f"data/training/{EXECUTION_DATE}/train.parquet"
    local_path = "/tmp/train.parquet"
    
    df.to_parquet(local_path, index=False)
    
    logging.info(f"Uploading test training data to s3://{STAGING_BUCKET}/{s3_key}")
    s3_client = boto3.client("s3")
    s3_client.upload_file(local_path, STAGING_BUCKET, s3_key)
    logging.info("Upload complete.")

def create_mock_evaluation_data():
    """
    Creates a mock evaluation dataset. Our evaluation script needs this
    to run, even though the results are mocked for the integration test.
    """
    eval_data = { "review_text": ["This is a test review for evaluation."] }
    df = pd.DataFrame(eval_data)
    s3_key = "data/evaluation/golden_dataset.parquet"
    local_path = "/tmp/golden_dataset.parquet"
    df.to_parquet(local_path, index=False)
    
    logging.info(f"Uploading mock evaluation data to s3://{STAGING_BUCKET}/{s3_key}")
    s3_client = boto3.client("s3")
    s3_client.upload_file(local_path, STAGING_BUCKET, s3_key)
    logging.info("Upload complete.")


if __name__ == "__main__":
    training_df = create_finetuning_test_data()
    upload_data_to_s3(training_df)
    create_mock_evaluation_data()
    logging.info("Setup for training pipeline integration test is complete.")