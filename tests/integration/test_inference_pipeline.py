import pytest
import os
import boto3
import time

# --- Test Configuration ---
STAGING_DYNAMODB_TABLE = "StagingProductSummaryCache"
TEST_PRODUCT_ID = "product_integration_test_001"

@pytest.fixture(scope="module")
def dynamodb_client():
    return boto3.client("dynamodb")

def test_inference_pipeline_caches_summary(dynamodb_client):
    """
    Verifies that after the batch inference DAG runs, a summary for the
    test product exists in the staging DynamoDB cache.
    """
    # Arrange (Setup would have populated source DBs and run the DAG)
    time.sleep(10) # Give a moment for potential eventual consistency

    # Act
    try:
        response = dynamodb_client.get_item(
            TableName=STAGING_DYNAMODB_TABLE,
            Key={'product_id': {'S': TEST_PRODUCT_ID}}
        )
    except ClientError as e:
        pytest.fail(f"Failed to query DynamoDB: {e}")

    # Assert
    assert "Item" in response, f"No summary found in cache for product {TEST_PRODUCT_ID}"
    
    item = response["Item"]
    assert "summary_json" in item, "Cached item is missing the 'summary_json' attribute."
    
    # Check if the summary is valid JSON
    summary = json.loads(item["summary_json"]["S"])
    assert "pros" in summary
    assert "cons" in summary
    
    print(f"\nIntegration test passed: Found a valid cached summary for product {TEST_PRODUCT_ID}.")