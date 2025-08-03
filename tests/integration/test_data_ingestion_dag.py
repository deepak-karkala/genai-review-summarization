import pytest
from airflow.models.dagbag import DagBag

# This test checks the structural integrity of the DAG
def test_dag_loaded():
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    assert dagbag.get_dag(dag_id='data_ingestion_and_cleaning') is not None
    assert 'data_ingestion_and_cleaning' in dagbag.dags

# A more complex integration test would use the Airflow API
# to trigger a run in a staging environment and check the output in S3.
# This requires a running Airflow and is often done in a separate CI/CD stage.
#
# Example using pytest-airflow:
# from pytest_airflow import clirunner
#
# def test_dag_run_successfully(clirunner):
#     result = clirunner("dags", "test", "data_ingestion_and_cleaning", "2024-01-01")
#     assert result.return_code == 0, "DAG run failed"
#
#     # Add assertions here to check for output artifacts in a mock S3 bucket