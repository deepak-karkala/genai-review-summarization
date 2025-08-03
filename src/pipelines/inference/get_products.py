import logging
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)

def get_products_to_update(db_connection_string: str, interval_hours: int = 1) -> list[str]:
    """
    Gets a list of product_ids that have received new reviews in the last interval.
    """
    try:
        logging.info("Connecting to the application database to find products with new reviews...")
        engine = create_engine(db_connection_string)
        
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(hours=interval_hours)
        
        query = f"""
        SELECT DISTINCT product_id
        FROM public.reviews
        WHERE created_at >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
        
        product_ids = df['product_id'].tolist()
        logging.info(f"Found {len(product_ids)} products to update.")
        return product_ids
    except Exception as e:
        logging.error(f"Failed to get products to update: {e}")
        raise