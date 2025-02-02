"""
A DAG to grap twse symbols
"""
import logging
from datetime import date, timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.hooks.base_hook import BaseHook
from airflow.models.baseoperator import chain
from airflow.utils.trigger_rule import TriggerRule

import dags.utils.models as models
from dag_id import MarketDataDags
from dags.utils.common import init_task
from dags.utils.common_dag_parameters import (
    callback,
    DATE_PARAMS,
    DRY_RUN,
    LOG_LEVEL,
    default_args,
)
from src.twstock.manager import Manager
from src.twstock.db import get_session

marketdata_dags = MarketDataDags()

@task
def grab_symbols():
    params = init_task()
    mgr = Manager()
    ret = mgr.get_symbols()
    logging.debug(ret)

    uri = BaseHook.get_connection(models.Connection.prod.value).get_uri()
    uri = uri.replace("postgres:", "postgresql:", 1)
    logging.debug(uri)

    s = get_session(uri)
    for r in ret:
        s.merge(r)
    s.commit()
    s.close()

    return params

def create_twse_symbols_dag(dag_id, schedule):
    dag = DAG(
        dag_id=dag_id,
        description="grab twse symbols",
        doc_md=__doc__,
        default_args=default_args,
        schedule=schedule,
        params={
            "dry_run": DRY_RUN(False),
            "log_level": LOG_LEVEL("info"),
        },  # https://json-schema.org/draft/2020-12/json-schema-validation
        render_template_as_native_obj=True,
        dagrun_timeout=timedelta(hours=1),
        catchup=False,
        on_failure_callback=callback,
        on_success_callback=callback,
        tags=[models.Team.data.value, models.Major.marketdata.value, models.Marketdata.symbols.value],
    )
    with dag:
        grab_symbols()

    return dag

globals()[marketdata_dags.GRAG_TWSE_SYMBOLS] = create_twse_symbols_dag(marketdata_dags.GRAG_TWSE_SYMBOLS, "0 11 * * 1-5")
