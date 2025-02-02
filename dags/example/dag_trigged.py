"""DAG demonstrating.

This dag demostrate 2 ways to cross dag connection, trigger and sensor

TriggerDagRunOperator explain how to pass parameters from a DAG to a triggered DAG.

Additionally, if 2 independent task chains are established in a dag, a failed status is always displayed.
"""


from __future__ import annotations

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from dag_id import ExampleDags

import dags.utils.models as models

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
example_dags = ExampleDags()


# CAUTION, The subdag's dag_id should have the form '{parent_dag_id}.{this_task_id}'
# subdag will been deprecated, replace with TaskGroup
def subdag(parent_dag_id, child_dag_id, args) -> DAG:
    dag_subdag = DAG(
        dag_id=f"{parent_dag_id}.{child_dag_id}",
        default_args=args,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        schedule="@daily",
    )

    for i in range(5):
        EmptyOperator(
            task_id=f"{child_dag_id}-task-{i + 1}",
            default_args=args,
            dag=dag_subdag,
        )

    return dag_subdag


with DAG(
    dag_id=example_dags.DAG_TRIGGED,
    description="dag shows 2 different way to connect dags",
    doc_md=__doc__,
    default_args=default_args,
    schedule="30 */8 * * *",
    start_date=datetime(2022, 1, 1),
    params={
        "my_easy_int_param": 3,
        "my_easy_flag_param": False,
        "my_int_param": Param(
            default=10,
            description="a integer value",
            type="integer",
            minimum=0,
            maximum=20,
            section="my series",
        ),
        "my_str_param": Param(
            default="hi",
            description="a string value",
            type="string",
            section="my series",
        ),
    },
    catchup=False,
    tags=[models.Major.example.value, "dag_trigger"],
) as dag:
    # example 1
    wait_for_marketdata = ExternalTaskSensor(
        task_id="wait_for_sub_folder_dag",
        external_dag_id=example_dags.DYNAMIC_DAG + "_1",
        external_task_id="task_start",
        start_date=datetime(2020, 4, 29),
        # execution_delta=timedelta(hours=24),
        timeout=3600,
    )

    external_start = EmptyOperator(
        task_id="external_start",
    )

    external_end = EmptyOperator(
        task_id="external_end",
    )

    wait_for_marketdata >> external_start >> external_end

    # example 2
    trigger_start = EmptyOperator(
        task_id="trigger_start",
    )

    trigger_end = EmptyOperator(
        task_id="trigger_end",
    )

    trigger_op = TriggerDagRunOperator(
        trigger_dag_id=example_dags.DAG_TRIGGED + "_2",
        task_id="task_start",
        start_date=datetime(2020, 4, 29),
        # dynamic input by jinja, else it is static config, ex. my_easy_int_param always is default value 3.
        conf={"configuration": "{{ params }}"},
    )

    trigger_start >> trigger_op >> trigger_end
