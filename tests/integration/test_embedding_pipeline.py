import pytest
import os
import psycopg2
from pgvector.psycopg2 import register_vector

# --- Test Configuration ---
TEST_REVIEW_ID = "test_review_001"
EXPECTED_CHUNKS = 2
EXPECTED_EMBEDDING_DIM = 1024

# Database connection params from environment variables
DB_PARAMS = {
    "host": os.environ["STAGING_DB_HOST"],
    "port": os.environ["STAGING_DB_PORT"],
    "dbname": os.environ["STAGING_DB_NAME"],
    "user": os.environ["STAGING_DB_USER"],
    "password": os.environ["STAGING_DB_PASSWORD"],
}

@pytest.fixture(scope="module")
def db_connection():
    """Provides a reusable database connection for the test module."""
    conn = psycopg2.connect(**DB_PARAMS)
    register_vector(conn)
    yield conn
    conn.close()

def test_embedding_generation_end_to_end(db_connection):
    """
    Verifies that the embedding generation pipeline correctly processed
    and indexed the test data into the staging database.
    """
    # Arrange
    query = f"SELECT chunk_text, embedding FROM review_embeddings WHERE review_id = '{TEST_REVIEW_ID}';"
    
    # Act
    with db_connection.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()

    # Assert
    assert results is not None, "No results found for the test review ID."
    
    # 1. Verify the number of chunks
    assert len(results) == EXPECTED_CHUNKS, \
        f"Expected {EXPECTED_CHUNKS} chunks, but found {len(results)}."

    # 2. Verify the embedding vectors
    for i, (chunk_text, embedding) in enumerate(results):
        assert isinstance(embedding, list) or hasattr(embedding, 'shape'), \
            f"Embedding for chunk {i} is not a list or array."
        assert len(embedding) == EXPECTED_EMBEDDING_DIM, \
            f"Embedding for chunk {i} has dimension {len(embedding)}, expected {EXPECTED_EMBEDDING_DIM}."

    print(f"\nIntegration test passed: Found {len(results)} chunks with correct embedding dimensions.")
