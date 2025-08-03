import logging
import boto3
from datetime import datetime
from decimal import Decimal

logging.basicConfig(level=logging.INFO)

def cache_summaries_in_dynamodb(summaries: list[dict], table_name: str, ttl_days: int = 30):
    """
    Writes the generated summaries to the DynamoDB cache table in a batch.
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    ttl_timestamp = int((datetime.utcnow() + timedelta(days=ttl_days)).timestamp())

    try:
        with table.batch_writer() as batch:
            for item in summaries:
                batch.put_item(
                    Item={
                        'product_id': item['product_id'],
                        'summary_json': json.dumps(item['summary']), # Store as JSON string
                        'last_updated': datetime.utcnow().isoformat(),
                        'ttl': ttl_timestamp
                    }
                )
        logging.info(f"Successfully cached {len(summaries)} summaries in DynamoDB.")
    except Exception as e:
        logging.error(f"Failed to cache summaries: {e}")
        raise