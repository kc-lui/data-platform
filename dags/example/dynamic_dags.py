"""Dynamic DAG demonstrating.

This dag show how to write one dag python file to generate multiple DAGs
"""
from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from dag_id import ExampleDags

import dags.utils.models as models
from dags.utils.common import get_context_params

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
example_dags = ExampleDags()


def create_dag(dag_id, schedule, dag_number):
    dag = DAG(
        dag_id,
        description="dynamic generate multiple dags example",
        doc_md=__doc__,
        default_args=default_args,
        schedule=schedule,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=[models.Major.example.value, "dynamic"],
    )
    with dag:

        @task()
        def hello_world(**kwargs):
            logging.info("Hello World")
            logging.info(f"This is DAG: {dag_number}")
            logging.info(f"kwargs: {kwargs}")
            params = get_context_params()
            logging.info(params)

        logging.info(dag.params)
        hello_world()

    return dag


globals()["dynamic_dag_1"] = create_dag(example_dags.DYNAMIC_DAG + "_1", "@daily", 1)
globals()["dynamic_dag_2"] = create_dag(example_dags.DYNAMIC_DAG + "_2", "@hourly", 2)
