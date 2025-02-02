"""
This dag call airflow command to clean metadata in database.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

import dags.utils.models as models

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

default_args = {
    "owner": "data",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


@task.bash
def clean_task(**kwargs) -> str:
    params = kwargs["params"]
    start_time = params["start_time"]
    dry_run = params["dry_run"]
    cmd = f"""airflow db clean --clean-before-timestamp {start_time} {"--dry-run" if dry_run else ""} -y"""
    logging.info(cmd)
    return cmd


with DAG(
    DAG_ID,
    doc_md=__doc__,
    default_args=default_args,
    schedule="@daily",
    start_date=datetime.today() - timedelta(days=1),
    params={
        "start_time": Param(
            default=f"{date.today() - timedelta(days=30)}",
            description="Please select a date for --clean-before-timestamp",
            type="string",
            format="date",
        ),
        "dry_run": Param(
            default=False,
            description="dry run mode",
            type="boolean",
        ),
    },  # https://json-schema.org/draft/2020-12/json-schema-validation
    dagrun_timeout=timedelta(minutes=3),
    catchup=False,
    tags=[models.Team.operator.value, models.Major.maintenance.value],
) as dag:
    clean_task()
