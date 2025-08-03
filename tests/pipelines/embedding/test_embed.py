import pandas as pd
from unittest.mock import MagicMock
from src.pipelines.embedding.embed import generate_embeddings

def test_generate_embeddings_batching(mocker):
    # Arrange
    mock_bedrock_client = MagicMock()
    # Mock the return value of invoke_model
    mock_response_body = json.dumps({"embedding": [0.1] * 1024})
    mock_stream = MagicMock()
    mock_stream.read.return_value = mock_response_body.encode('utf-8')
    mock_bedrock_client.invoke_model.return_value = {"body": mock_stream}
    
    mocker.patch('boto3.client', return_value=mock_bedrock_client)

    test_data = {
        'review_id': [1],
        'product_id': ['A'],
        'star_rating': [5],
        'language': ['en'],
        'cleaned_text': ['This is the first sentence. This is the second sentence.']
    }
    test_df = pd.DataFrame(test_data)

    # Act
    embedding_data = generate_embeddings(test_df, mock_bedrock_client)

    # Assert
    assert len(embedding_data) == 2 # The text should be split into two chunks
    assert mock_bedrock_client.invoke_model.call_count == 2
    assert embedding_data[0]['review_id'] == 1
    assert "embedding" in embedding_data[0]
    assert len(embedding_data[0]['embedding']) == 1024