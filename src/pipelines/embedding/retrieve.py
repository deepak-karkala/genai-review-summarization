import logging
import pandas as pd
import subprocess

logging.basicConfig(level=logging.INFO)

def get_latest_cleaned_data(local_path: str, execution_date: str) -> pd.DataFrame:
    """
    Uses DVC to pull the latest version of the cleaned data corresponding to the execution date.
    """
    file_path = f"{local_path}/cleaned_reviews_{execution_date}.parquet"
    dvc_file_path = f"{file_path}.dvc"
    try:
        logging.info(f"Using DVC to pull data for {dvc_file_path}...")
        # Ensure the .dvc file itself is present before pulling
        # In a real Airflow setup, the repo would be synced.
        subprocess.run(["dvc", "pull", dvc_file_path], check=True, capture_output=True)
        
        logging.info(f"Loading data from {file_path} into pandas DataFrame.")
        df = pd.read_parquet(file_path)
        logging.info(f"Successfully loaded {len(df)} records.")
        return df
    except FileNotFoundError:
        logging.error(f"DVC file not found: {dvc_file_path}. Did the ingestion pipeline run successfully?")
        raise
    except Exception as e:
        logging.error(f"Failed to retrieve data with DVC: {e}")
        logging.error(f"DVC output: {e.stdout.decode() if hasattr(e, 'stdout') else ''}")
        raise