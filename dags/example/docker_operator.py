from __future__ import annotations

from datetime import datetime, timedelta

import docker
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from dag_id import ExampleDags
from plugins.operators.CustomDockerOperator import CustomDockerOperator

import dags.utils.models as models

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "schedule+interval": None,
    "start_date": datetime(2024, 6, 21),
    "retries": 0,
}

task_args = {
    "retries": 5,
    "retry_delay": timedelta(seconds=5),
    "backoff": True,
}
example_dags = ExampleDags()


def remove_existing_container():
    client = docker.from_env()
    container_name = "synfutures-gateway"
    try:
        container = client.containers.get(container_name)
        container.remove(force=True)  # Force removal of the container
        print(f"Removed existing container {container_name}.")
    except docker.errors.NotFound:
        print(f"Container {container_name} does not exist. No need to remove.")
    except docker.errors.APIError as e:
        print(f"Failed to remove container {container_name}: {e}")


def get_instruments():
    response = requests.get("http://synfutures-gateway:3000/instruments")
    print(f"Response: {response.text}")


with DAG(
    dag_id=example_dags.DOCKEROPERATOR_SHOWCASE,
    default_args=default_args,
    tags=[models.Major.example.value],
    catchup=False,
) as dag:

    remove_container = PythonOperator(
        task_id="remove_container",
        python_callable=remove_existing_container,
    )

    run_synfutures_api_server = CustomDockerOperator(
        task_id="run_synfutures_api_server",
        image="965013327980.dkr.ecr.ap-northeast-1.amazonaws.com/data/portfolio-manager-extensions-synfutures:latest",
        network="data-platform_default",
        detach=True,
        ports={"3000/tcp": 3000},
        container_name="synfutures-gateway",
        retries=0,
    )

    get_synfutures_instruments = PythonOperator(
        task_id="get_synfutures_instruments",
        python_callable=get_instruments,
        default_args=task_args,
    )

    run_synfutures_api_server >> get_synfutures_instruments >> remove_container
