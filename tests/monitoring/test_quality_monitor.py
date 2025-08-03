from unittest.mock import patch, MagicMock
import pandas as pd
from src.monitoring import quality_monitor

@patch('boto3.client')
def test_publish_metrics_to_cloudwatch(mock_boto_client):
    # Arrange
    mock_cloudwatch = MagicMock()
    mock_boto_client.return_value = mock_cloudwatch
    
    test_data = {
        'faithfulness_score': [1.0, 0.9], # Avg = 0.95
        'coherence_score': [5.0, 4.0]     # Avg = 4.5
    }
    test_df = pd.DataFrame(test_data)
    
    # Act
    quality_monitor.publish_metrics_to_cloudwatch(test_df)
    
    # Assert
    mock_cloudwatch.put_metric_data.assert_called_once()
    
    # Get the arguments passed to the mock
    call_args = mock_cloudwatch.put_metric_data.call_args[1]
    
    assert call_args['Namespace'] == "LLMReviewSummarizer"
    
    metric_data = call_args['MetricData']
    faithfulness_metric = next(m for m in metric_data if m['MetricName'] == 'AverageFaithfulness')
    coherence_metric = next(m for m in metric_data if m['MetricName'] == 'AverageCoherence')

    assert faithfulness_metric['Value'] == 0.95
    assert coherence_metric['Value'] == 4.5