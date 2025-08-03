import pytest
import os
import mlflow
from mlflow.tracking import MlflowClient

# --- Test Configuration ---
STAGING_MLFLOW_TRACKING_URI = os.environ["STAGING_MLFLOW_TRACKING_URI"]
MODEL_NAME = "review-summarizer"
TEST_RUN_TAG = "integration_test"
EXECUTION_DATE = "2025-01-01"

@pytest.fixture(scope="module")
def mlflow_client():
    """Provides a reusable MLflow client for the test module."""
    mlflow.set_tracking_uri(STAGING_MLFLOW_TRACKING_URI)
    return MlflowClient()

def test_finetuning_pipeline_registers_new_model_version(mlflow_client):
    """
    Verifies that a new version of the summarizer model was registered by the
    pipeline run, tagged appropriately for this integration test.
    """
    # Arrange: Find the experiment and the specific run for our test
    # In the real DAG, we would add a tag to the MLflow run to identify it.
    experiment = mlflow_client.get_experiment_by_name("llm-finetuning")
    assert experiment is not None, "MLflow experiment 'llm-finetuning' not found."
    
    # Filter runs by tag to find our specific integration test run
    runs = mlflow_client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string=f"tags.airflow_run_id LIKE 'scheduled__{EXECUTION_DATE}%' AND tags.dag_id = 'llm_continuous_training'"
    )
    
    assert len(runs) > 0, f"No MLflow run found for DAG 'llm_continuous_training' on {EXECUTION_DATE}"
    
    test_run = runs[0]
    test_run_id = test_run.info.run_id

    # Act: Get all registered versions for our model
    registered_versions = mlflow_client.get_latest_versions(MODEL_NAME, stages=["None", "Staging"])
    
    # Assert: Check if any of the registered versions came from our test run
    found_match = any(version.run_id == test_run_id for version in registered_versions)
    
    assert found_match, \
        f"Integration test failed: No model version was registered in MLflow for the test run ID {test_run_id}."

    print(f"\nIntegration test passed: Found a newly registered model version from run ID {test_run_id}.")