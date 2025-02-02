import os
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dag_id import ExampleDags
from kubernetes.client import models as k8s

import dags.utils.models as models

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "queue": "kubernetes",  # add queue "kubernetes"
}

# https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/kubernetes_executor.html#pod-override
k8s_exec_config_resource_requirements = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        requests={"cpu": "500m", "memory": "512Mi"}, limits={"cpu": "500m", "memory": "512Mi"}
                    ),
                )
            ],
            node_selector={"high-mem-pool": "true"},
        )
    )
}

example_dags = ExampleDags()


def list_top_level_folders(bucket_name):
    s3 = boto3.client("s3")

    # Use the delimiter '/' to list only top-level folders
    response = s3.list_objects_v2(Bucket=bucket_name, Delimiter="/")

    if "CommonPrefixes" in response:
        for folder in response["CommonPrefixes"]:
            print(folder["Prefix"])
    else:
        print("No folders found.")


def check_s3():
    env = os.environ.get("ENV")
    if env == "dev":
        list_top_level_folders("dev")
    elif env == "prod":
        list_top_level_folders("prod")
    else:
        print(f"Error account_id:{env}")


with DAG(
    dag_id=example_dags.K8S,
    description=example_dags.K8S,
    doc_md=__doc__,
    default_args=default_args,
    start_date=datetime(2024, 8, 22),
    schedule=None,
    catchup=False,
    tags=[models.Major.example.value, "k8s_executor"],
) as dag:
    start = BashOperator(task_id="start", bash_command="echo start!!")

    sleep = BashOperator(
        task_id="sleep",
        executor_config=k8s_exec_config_resource_requirements,  # override k8s pod yaml config
        bash_command="for i in {1..20}; do sleep 5s; echo 'waiting'; done",
    )

    s3 = PythonOperator(task_id="s3", python_callable=check_s3)

    marketdata = BashOperator(task_id="marketdata", bash_command="ls /marketdata")  # default mount /marketdata

    end = BashOperator(task_id="end", bash_command="echo finish!!")

    start >> [sleep, s3, marketdata] >> end
