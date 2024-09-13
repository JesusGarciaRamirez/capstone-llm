from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
import os 

default_args = {
    "owner": "airflow",
    "description": "Use of the DockerOperator",
    "depend_on_past": False,
    "start_date": datetime(2024, 9, 11),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "docker_operator_dag_2",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    ingestion_task = DockerOperator(
        task_id="ingestion_task",
        image="data_ingestion",
        container_name="task__data_ingestion",
        api_version="auto",
        auto_remove=True,
        network_mode="bridge",
        environment={
            "AWS_SECRET_ACCESS_KEY": os.getenv('AWS_SECRET_ACCESS_KEY', 'default_value'),
            "AWS_ACCESS_KEY_ID": os.getenv('AWS_ACCESS_KEY_ID', 'default_value'),
            "AWS_SESSION_TOKEN": os.getenv('AWS_SESSION_TOKEN', 'default_value'),
        }
    )