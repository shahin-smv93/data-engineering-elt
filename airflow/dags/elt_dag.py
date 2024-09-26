from datetime import datetime
from airflow import DAG
from docker.types import Mount
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.docker.operators.docker import DockerOperator
import subprocess

CONN_ID = '7867dd66-7e07-430f-883c-8590855bf3f6'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}


dag = DAG(
    'elt_and_dbt',
    default_args=default_args,
    description='elt workflow with dbt',
    start_date=datetime(2024, 9, 26),
    catchup=False
)

# writing tasks

# the first task is the elt_script, which is a python script
t1 = AirbyteTriggerSyncOperator(
    task_id="airbyte_postgres_postgres",
    airbyte_conn_id='airbyte',
    connection_id=CONN_ID,
    asynchronous=False,
    timeout=3600,
    wait_seconds=3,
    dag=dag
)

# the second task is related to dbt
# we have to attach dbt image to it.
t2 = DockerOperator(
    task_id="dbt_run",
    image='ghcr.io/dbt-labs/dbt-postgres:1.7.2',
    command=[
        "run",
        "--profiles-dir",
        "/root",
        "--project-dir",
        "/opt/dbt"
    ],
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    mounts=[
        Mount(source='/home/shahin/elt_project/custom_postgres',
              target='/opt/dbt', type='bind'),
        Mount(source='/home/shahin/.dbt',
              target='/root', type='bind')
    ],
    dag=dag
)

t1 >> t2
