from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# Assuming custom Python modules are in a package installed in the Airflow environment
from src.pipelines.ingestion import extract, transform, validate, load

S3_BUCKET = "my-ecommerce-mlops-bucket"
LOCAL_DATA_PATH = "/tmp/data" # Path on the Airflow worker

def extract_task(ti):
    hook = PostgresHook(postgres_conn_id="source_db_conn")
    conn_string = hook.get_uri()
    reviews_df = extract.get_new_reviews(conn_string, ti.execution_date.to_iso8601_string())
    # Push to XComs for the next task
    ti.xcom_push(key="raw_reviews_df", value=reviews_df.to_json())

def transform_task(ti):
    raw_reviews_json = ti.xcom_pull(task_ids="extract_new_reviews", key="raw_reviews_df")
    raw_df = pd.read_json(raw_reviews_json)
    transformed_df = transform.transform_reviews(raw_df)
    ti.xcom_push(key="transformed_reviews_df", value=transformed_df.to_json())

def validate_task(ti):
    transformed_reviews_json = ti.xcom_pull(task_ids="transform_raw_reviews", key="transformed_reviews_df")
    transformed_df = pd.read_json(transformed_reviews_json)
    if not validate.validate_cleaned_data(transformed_df):
        raise ValueError("Data validation failed, stopping pipeline.")
    # If validation succeeds, the original df is passed through
    ti.xcom_push(key="validated_reviews_df", value=transformed_df.to_json())


def load_task(ti):
    validated_reviews_json = ti.xcom_pull(task_ids="validate_transformed_reviews", key="validated_reviews_df")
    validated_df = pd.read_json(validated_reviews_json)
    load.save_and_version_data(
        df=validated_df, 
        local_path=LOCAL_DATA_PATH, 
        s3_bucket=S3_BUCKET,
        execution_date=ti.execution_date.to_iso8601_string()
    )


with DAG(
    dag_id="data_ingestion_and_cleaning",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 1 * * *",  # Run daily at 1 AM UTC
    catchup=False,
    tags=["data-eng", "ingestion"],
) as dag:
    extract_new_reviews = PythonOperator(
        task_id="extract_new_reviews",
        python_callable=extract_task,
    )
    transform_raw_reviews = PythonOperator(
        task_id="transform_raw_reviews",
        python_callable=transform_task,
    )
    validate_transformed_reviews = PythonOperator(
        task_id="validate_transformed_reviews",
        python_callable=validate_task,
    )
    load_and_version_data = PythonOperator(
        task_id="load_and_version_data",
        python_callable=load_task,
    )

    extract_new_reviews >> transform_raw_reviews >> validate_transformed_reviews >> load_and_version_data