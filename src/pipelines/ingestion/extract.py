import logging
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)

def get_new_reviews(db_connection_string: str, execution_date: str) -> pd.DataFrame:
    """
    Extracts new reviews from the source database created in the last 24 hours.
    
    Args:
        db_connection_string: The database connection string.
        execution_date: The date of the DAG run (for reproducibility).

    Returns:
        A pandas DataFrame with new reviews.
    """
    try:
        logging.info("Connecting to the source database...")
        engine = create_engine(db_connection_string)
        
        # Calculate the time window for the query
        end_date = datetime.fromisoformat(execution_date)
        start_date = end_date - timedelta(days=1)
        
        query = f"""
        SELECT review_id, product_id, user_id, star_rating, review_text, created_at
        FROM public.reviews
        WHERE created_at >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
        AND created_at < '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        
        logging.info(f"Executing query for reviews between {start_date} and {end_date}.")
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
        
        logging.info(f"Successfully extracted {len(df)} new reviews.")
        return df
    except Exception as e:
        logging.error(f"Failed to extract reviews: {e}")
        raise