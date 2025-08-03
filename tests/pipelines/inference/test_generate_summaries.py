from unittest.mock import patch
from src.pipelines.inference.generate_summaries import invoke_llm_endpoint

@patch('requests.post')
def test_invoke_llm_endpoint_success(mock_post):
    # Arrange
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"summary": {"pros": "Good", "cons": "Bad"}}
    
    test_prompts = [{"product_id": "A", "prompt": "Test prompt"}]
    
    # Act
    summaries = invoke_llm_endpoint(test_prompts, "http://fake-url", "fake-key")

    # Assert
    assert len(summaries) == 1
    assert summaries[0]["product_id"] == "A"
    assert summaries[0]["summary"]["pros"] == "Good"

@patch('requests.post')
def test_invoke_llm_endpoint_handles_error(mock_post):
    # Arrange
    mock_post.side_effect = requests.exceptions.RequestException("API Error")
    
    test_prompts = [{"product_id": "A", "prompt": "Test prompt"}]
    
    # Act
    summaries = invoke_llm_endpoint(test_prompts, "http://fake-url", "fake-key")

    # Assert
    assert len(summaries) == 0 # The failed request should be skipped