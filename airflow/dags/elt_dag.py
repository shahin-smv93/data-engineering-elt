from datetime import datetime, timedelta
from airflow import DAG
from docker.types import Mount
from airflow.operators.python_operators import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.docker import DockerOperator
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

def run_elt_script():
    # this is going to point to the path we have
    # set inside of the docker container
    script_path = "/opt/airflow/elt/elt_script.py"
    result = subprocess.run(
        ["python", script_path],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"script failed with error: {result.stderr}")
    else:
        print(result.stdout)
    

dag = DAG(
    'elt_and_dbt',
    default_args=default_args,
    description='elt workflow with dbt',
    start_date=datetime(2024, 9, 22),
    catchup=False
)

# writing tasks

# the first task is the elt_script, which is a python script
t1 = PythonOperator(
    task_id="run_elt_script",
    python_callable=run_elt_script,
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
        "/dbt"
    ],
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    mounts=[
        Mount(source='/home/shahin/elt_project/custom_postgres',
              target='/dbt', type='bind'),
        Mount(source='/home/shahin/.dbt',
              target='/root', type='bind')
    ],
    dag=dag
)

t1 >> t2
