from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

import dags.utils.models as models
from dags.dag_id import ExampleDags

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

example_dags = ExampleDags()

with DAG(
    dag_id=example_dags.SLEEP,
    description="dag shows 2 different way to connect dags",
    doc_md=__doc__,
    default_args=default_args,
    start_date=datetime(2024, 8, 22),
    schedule=None,
    catchup=False,
    tags=[models.Major.example.value],
) as dag:
    task1 = BashOperator(task_id="task1", bash_command="echo start!!")

    task2 = BashOperator(task_id="task2", bash_command="for i in {1..20}; do sleep 10s; echo 'waiting'; done")

    task3 = BashOperator(task_id="task3", bash_command="echo finish!!")

    task1 >> task2 >> task3
