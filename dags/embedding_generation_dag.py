from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.amazon.aws.hooks.bedrock import BedrockHook
from airflow.providers.amazon.aws.hooks.secrets_manager import SecretsManagerHook
# Assuming custom Python modules are installed
from src.pipelines.embedding import retrieve, embed, load

LOCAL_DATA_PATH = "/tmp/data"

def retrieve_data_task(ti):
    # This task gets the output from the ingestion DAG
    # For simplicity, we assume the execution date matches.
    execution_date = ti.execution_date.to_iso8601_string()
    reviews_df = retrieve.get_latest_cleaned_data(LOCAL_DATA_PATH, execution_date)
    ti.xcom_push(key="reviews_df_json", value=reviews_df.to_json())

def embed_task(ti):
    reviews_json = ti.xcom_pull(task_ids="retrieve_cleaned_data", key="reviews_df_json")
    reviews_df = pd.read_json(reviews_json)
    
    bedrock_hook = BedrockHook(aws_conn_id='aws_default')
    bedrock_client = bedrock_hook.get_conn()
    
    embedding_data = embed.generate_embeddings(reviews_df, bedrock_client)
    ti.xcom_push(key="embedding_data", value=embedding_data)

def load_task(ti):
    embedding_data = ti.xcom_pull(task_ids="generate_review_embeddings", key="embedding_data")
    
    secrets_hook = SecretsManagerHook(aws_conn_id='aws_default')
    db_secret = secrets_hook.get_secret_value("aurora/vector_db/credentials")
    db_params = json.loads(db_secret)
    
    load.index_embeddings_in_db(embedding_data, db_params)

with DAG(
    dag_id="embedding_generation",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,  # Triggered by the ingestion DAG
    catchup=False,
    tags=["data-eng", "embedding", "rag"],
) as dag:
    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion_dag",
        external_dag_id="data_ingestion_and_cleaning",
        external_task_id="load_and_version_data",
        allowed_states=["success"],
        execution_delta=pendulum.duration(hours=0),
    )
    
    retrieve_cleaned_data = PythonOperator(task_id="retrieve_cleaned_data", python_callable=retrieve_data_task)
    generate_review_embeddings = PythonOperator(task_id="generate_review_embeddings", python_callable=embed_task)
    index_embeddings = PythonOperator(task_id="index_embeddings", python_callable=load_task)

    wait_for_ingestion >> retrieve_cleaned_data >> generate_review_embeddings >> index_embeddings