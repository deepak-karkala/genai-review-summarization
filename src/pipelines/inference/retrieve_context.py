import logging
import psycopg2
from pgvector.psycopg2 import register_vector
from langchain.prompts import PromptTemplate

logging.basicConfig(level=logging.INFO)

PROMPT_TEMPLATE = """
###Instruction: Based ONLY on the following customer reviews, provide a balanced summary of the product's pros and cons. Do not invent information.

###Reviews:
{reviews_context}

###Response:
"""

def retrieve_rag_context(product_ids: list[str], db_params: dict) -> list[dict]:
    """
    For each product, retrieves the RAG context from the Vector DB and constructs a prompt.
    """
    prompts = []
    try:
        with psycopg2.connect(**db_params) as conn:
            register_vector(conn)
            with conn.cursor() as cur:
                for product_id in product_ids:
                    # This query implements our advanced RAG strategy
                    # Note: This is a simplified example. A production query might be more complex.
                    query = """
                    (SELECT chunk_text FROM review_embeddings WHERE product_id = %s AND star_rating >= 4 ORDER BY review_id DESC LIMIT 5)
                    UNION ALL
                    (SELECT chunk_text FROM review_embeddings WHERE product_id = %s AND star_rating <= 2 ORDER BY review_id DESC LIMIT 5)
                    """
                    cur.execute(query, (product_id, product_id))
                    results = cur.fetchall()
                    
                    if not results:
                        continue
                        
                    context_str = "\n".join([f"- {res[0]}" for res in results])
                    prompt_formatter = PromptTemplate.from_template(PROMPT_TEMPLATE)
                    formatted_prompt = prompt_formatter.format(reviews_context=context_str)
                    
                    prompts.append({"product_id": product_id, "prompt": formatted_prompt})
        
        logging.info(f"Successfully constructed {len(prompts)} prompts.")
        return prompts
    except Exception as e:
        logging.error(f"Failed to retrieve RAG context: {e}")
        raise