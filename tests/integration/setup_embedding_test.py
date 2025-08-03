import logging
import pandas as pd
import boto3
import subprocess
import os
import psycopg2

# --- Test Configuration ---
TEST_EXECUTION_DATE = "2025-01-01T00:00:00+00:00"
TEST_REVIEW_ID = "test_review_001"
STAGING_BUCKET = os.environ["STAGING_S3_BUCKET"]
LOCAL_DATA_PATH = "/tmp/staging_data"

# Database connection params from environment variables
DB_PARAMS = {
    "host": os.environ["STAGING_DB_HOST"],
    "port": os.environ["STAGING_DB_PORT"],
    "dbname": os.environ["STAGING_DB_NAME"],
    "user": os.environ["STAGING_DB_USER"],
    "password": os.environ["STAGING_DB_PASSWORD"],
}

logging.basicConfig(level=logging.INFO)

def create_test_data():
    """Creates a sample DataFrame for the test."""
    # This text is designed to be split into two chunks by our splitter configuration
    long_text = (
        "This is the first sentence of a moderately long review. "
        "It provides some initial positive feedback on the product's build quality. "
        "The reviewer seems generally happy with their purchase so far. "
        "Now we move on to the second part of the review which discusses the battery life. "
        "Unfortunately, the battery does not last as long as advertised, which is a significant drawback."
    )
    data = {
        'review_id': [TEST_REVIEW_ID],
        'product_id': ['product_abc'],
        'user_id': [999],
        'star_rating': [3],
        'cleaned_text': [long_text],
        'language': ['en'],
        'toxicity_score': [0.1],
        'created_at': [pd.to_datetime(TEST_EXECUTION_DATE)]
    }
    return pd.DataFrame(data)

def upload_and_version_data(df: pd.DataFrame):
    """Saves data locally, uploads to S3, and creates DVC file."""
    os.makedirs(LOCAL_DATA_PATH, exist_ok=True)
    
    # Path names must match what the Airflow DAG expects
    execution_date_str = pd.to_datetime(TEST_EXECUTION_DATE).strftime('%Y-%m-%dT%H:%M:%S%z')
    file_name = f"cleaned_reviews_{execution_date_str}.parquet"
    local_file_path = os.path.join(LOCAL_DATA_PATH, file_name)
    
    logging.info(f"Saving test data to {local_file_path}")
    df.to_parquet(local_file_path, index=False)
    
    # Upload to S3 (simulating DVC remote)
    s3_client = boto3.client("s3")
    s3_key = f"data/{file_name}" # DVC would use a hash, but this is simpler for a test
    s3_client.upload_file(local_file_path, STAGING_BUCKET, s3_key)
    logging.info(f"Uploaded test data to s3://{STAGING_BUCKET}/{s3_key}")
    
    # For a real DVC setup, we would run `dvc add` and `dvc push` here.
    # For this test, placing the file is sufficient.

def clean_staging_db():
    """Ensures the staging DB is clean before the test run."""
    logging.info("Cleaning staging database for a fresh test run.")
    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            # Truncate the table to remove any data from previous runs
            cur.execute(f"DELETE FROM review_embeddings WHERE review_id = '{TEST_REVIEW_ID}';")
            conn.commit()
    logging.info("Staging database cleaned.")

if __name__ == "__main__":
    clean_staging_db()
    test_df = create_test_data()
    upload_and_version_data(test_df)
    logging.info("Setup for embedding pipeline integration test is complete.")
