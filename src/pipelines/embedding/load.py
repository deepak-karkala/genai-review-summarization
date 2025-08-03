import logging
import psycopg2
import psycopg2.extras
from pgvector.psycopg2 import register_vector

logging.basicConfig(level=logging.INFO)

def index_embeddings_in_db(embedding_data: list, db_params: dict) -> None:
    """
    Indexes the generated embeddings and metadata into the Aurora PostgreSQL DB with pgvector.
    """
    try:
        logging.info(f"Connecting to the vector database...")
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                register_vector(cur)
                
                insert_query = """
                INSERT INTO review_embeddings (review_id, product_id, star_rating, language, chunk_text, embedding)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (review_id, chunk_text) DO NOTHING; 
                """ # Using a simple ON CONFLICT to ensure idempotency

                # Prepare data for batch insert
                data_to_insert = [
                    (
                        item["review_id"],
                        item["product_id"],
                        item["star_rating"],
                        item["language"],
                        item["chunk_text"],
                        item["embedding"],
                    )
                    for item in embedding_data
                ]
                
                logging.info(f"Indexing {len(data_to_insert)} embeddings in batches...")
                psycopg2.extras.execute_batch(cur, insert_query, data_to_insert)
                conn.commit()
                logging.info("Indexing complete.")
    except Exception as e:
        logging.error(f"Failed to index embeddings: {e}")
        raise