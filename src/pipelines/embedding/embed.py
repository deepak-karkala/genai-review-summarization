import logging
import pandas as pd
import boto3
import json
from langchain.text_splitter import RecursiveCharacterTextSplitter

logging.basicConfig(level=logging.INFO)

def generate_embeddings(reviews_df: pd.DataFrame, bedrock_client) -> list:
    """
    Chunks review text and generates embeddings using Amazon Bedrock.
    """
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=256,
        chunk_overlap=32,
        length_function=len,
    )
    
    all_embeddings_data = []
    
    logging.info(f"Starting embedding generation for {len(reviews_df)} reviews.")
    for index, row in reviews_df.iterrows():
        chunks = text_splitter.split_text(row['cleaned_text'])
        
        for chunk in chunks:
            body = json.dumps({"inputText": chunk})
            response = bedrock_client.invoke_model(
                body=body,
                modelId="amazon.titan-embed-text-v2:0",
                accept="application/json",
                contentType="application/json",
            )
            response_body = json.loads(response.get("body").read())
            embedding = response_body.get("embedding")
            
            all_embeddings_data.append({
                "review_id": row['review_id'],
                "product_id": row['product_id'],
                "star_rating": row['star_rating'],
                "language": row['language'],
                "chunk_text": chunk,
                "embedding": embedding,
            })
    
    logging.info(f"Successfully generated {len(all_embeddings_data)} embeddings.")
    return all_embeddings_data