from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from dag_id import ExampleDags
from kubernetes.client import models as k8s

import dags.utils.models as models

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "queue": "kubernetes",  # add queue "kubernetes"
}

example_dags = ExampleDags()

with DAG(
    dag_id=example_dags.K8S_POD_OPERATOR,
    description=example_dags.K8S_POD_OPERATOR,
    doc_md=__doc__,
    default_args=default_args,
    start_date=datetime(2024, 8, 22),
    schedule=None,
    catchup=False,
    tags=[models.Major.example.value, "k8s_pod_operator"],
) as dag:
    start = BashOperator(task_id="start", bash_command="echo start!!")

    # use KubernetesPodOperator
    # https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html#kubernetespodoperator
    kpo_demo = KubernetesPodOperator(
        task_id="kpo_demo",
        namespace="airflow",
        name="airflow-kpo-task",
        image="python:3.8-slim",
        cmds=["python", "-c"],
        arguments=["import time; [print(f'Message {i+1}') or time.sleep(5) for i in range(5)]"],
        node_selector={"high-mem-pool": "true"},
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "512Mi"}, limits={"cpu": "500m", "memory": "512Mi"}
        )
        # need mount volume and set service account by yourself
    )

    end = BashOperator(task_id="end", bash_command="echo finish!!")

    start >> kpo_demo >> end
