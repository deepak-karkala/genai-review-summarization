from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.docker_operator import DockerOperator

# Assumes the monitoring script is containerized in an image in ECR
ECR_IMAGE = "123456789012.dkr.ecr.eu-west-1.amazonaws.com/quality-monitor:latest"

with DAG(
    dag_id="model_quality_monitoring",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 3 * * *",  # Run daily at 3 AM UTC
    catchup=False,
    tags=["monitoring", "quality", "llm"],
) as dag:
    run_quality_monitor = DockerOperator(
        task_id="run_quality_monitor",
        image=ECR_IMAGE,
        api_version="auto",
        auto_remove=True,
        # Pass environment variables needed by the script
        environment={
            "SUMMARY_CACHE_TABLE": "ProductionProductSummaryCache",
            "AWS_ACCESS_KEY_ID": "{{ conn.aws_default.login }}",
            "AWS_SECRET_ACCESS_KEY": "{{ conn.aws_default.password }}",
            "AWS_SESSION_TOKEN": "{{ conn.aws_default.extra_dejson.aws_session_token }}",
            "AWS_REGION": "eu-west-1",
        },
        command="/usr/bin/python3 quality_monitor.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )