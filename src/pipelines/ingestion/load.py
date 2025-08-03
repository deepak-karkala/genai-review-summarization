import logging
import pandas as pd
import subprocess

logging.basicConfig(level=logging.INFO)

def save_and_version_data(df: pd.DataFrame, local_path: str, s3_bucket: str, execution_date: str) -> None:
    """
    Saves the DataFrame to a local Parquet file and uses DVC to version and push to S3.
    """
    try:
        # Save to local filesystem (accessible by Airflow worker)
        file_path = f"{local_path}/cleaned_reviews_{execution_date}.parquet"
        logging.info(f"Saving cleaned data to {file_path}")
        df.to_parquet(file_path, index=False)
        
        # DVC commands to version and push the data
        # Assumes DVC is initialized and remote is configured
        logging.info("Versioning data with DVC...")
        subprocess.run(["dvc", "add", file_path], check=True)
        
        logging.info("Pushing data to S3 remote with DVC...")
        subprocess.run(["dvc", "push", f"{file_path}.dvc"], check=True)

        logging.info("Data successfully saved and versioned.")
    except Exception as e:
        logging.error(f"Failed to save and version data: {e}")
        raise