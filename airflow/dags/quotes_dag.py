from airflow import DAG
from pendulum import datetime
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from airflow.operators.docker_operator import DockerOperator


def send_email():
    #send email if fail
    pass


# Define the DAG
with DAG(
    dag_id="plata_main",
    start_date=datetime(2025, 2, 6),
    schedule_interval="@daily",  # Adjust based on your requirements
    catchup=False,
    default_args={"on_failure_callback": send_email()}
) as dag:
    
    ingest_daily_file_docker = DockerOperator(
        task_id = 'quotes_files',
        image = 'plata_quotes_images:latest',
        container_name = 'plata_quotes_container',
        auto_remove = True,
        command = """python ingest_file.py"""
    )

    # Define the DBT Task Group
    dbt_task_group = DbtTaskGroup(
        group_id="plata_main",
        project_config = ProjectConfig("/opt/airflow/dbt/"),
        render_confi = RenderConfig(
            select = ["config.materialized:table","config.materialized:incremental"]
        ),
        execution_config = ExecutionConfig(dbt_executable_path="/opt/airflow/dbt_venv/dbt"),
    )

    # Add other tasks if needed
    ingest_daily_file_docker >> dbt_task_group
