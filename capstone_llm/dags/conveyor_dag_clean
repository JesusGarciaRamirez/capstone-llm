from datetime import datetime, timedelta
from airflow import DAG
from conveyor.operators import ConveyorSparkSubmitOperatorV2

role = "capstone_conveyor_llm"

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
    "clean_data_mj",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    ingestion_task = ConveyorSparkSubmitOperatorV2(
    task_id="cleaning_task",
    num_executors=1,
    driver_instance_type="mx.small",
    executor_instance_type="mx.small",
    aws_role=role,
    application="/opt/spark/work-dir/src/capstonellm/tasks/clean.py",
    application_args=[
        "--env", "{{ macros.conveyor.env() }}",
    ]
)
