"""DAG demonstrating.

This message will show in UI, to illustrate this dag when trigger.

This example show some feature:

    a. dynamic task generation, shows 3 ways to dynamic generate tasks.
    b. notifier when status callback.
    c. diversity parameters.
    d. trigger rule, the status of tail tasks determine the status of dag.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import task, task_group
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.notifications.basenotifier import BaseNotifier
from airflow.providers.slack.notifications.slack import SlackNotifier
from airflow.utils.trigger_rule import TriggerRule
from dag_id import ExampleDags
from pendulum import duration

import dags.utils.models as models

SYMBOL_COUNT = 2000


SLACK_CONNECTION_ID = "slack-workflow-notification"
SLACK_CHANNEL = "#workflow-notification-dev"
SLACK_MESSAGE = """
Hello!
The {{ ti.task_id }} task is saying hi :wave:
Today is the {{ ds }} and this task finished with the state: {{ ti.state }} :tada:.
"""

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
example_dags = ExampleDags()


class MyNotifier(BaseNotifier):
    def __init__(self):
        super().__init__()
        pass

    def notify(self, context):
        t_id = context["ti"].task_id
        t_state = context["ti"].state
        logging.info(f"{t_id}, {t_state}")
        logging.info(context)


def sla_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    logging.warn(
        f"""The callback arguments are: \n
            dag: {dag},
            task_list: {task_list},
            blocking_task_list: {blocking_task_list},
            slas: slas,
            blocking_tis: {blocking_tis}
        """
    )


def callback(context):
    logging.info(context)


# Creating Tasks
@task
def task_start(**kwargs) -> str:
    """Empty Task which is First Task of Dag"""
    logging.info("Task_Start")
    logging.info(f"kwargs: {kwargs}")
    return "Task_Start"


@task
def check_orderbook(exchange: str = "", symbol: str = "", **kwargs) -> str:
    """Check symbol order book"""
    ti = kwargs["ti"]
    logging.info(f"{exchange} {symbol}")
    logging.info(f"kwargs: {kwargs}")
    logging.info(f"""Previous Reture Value: {ti.xcom_pull("task_start", "return_value")}""")
    return f"[ Task {exchange} {symbol} ]"


@task
def check_spot_orderbook(exchange: str = "", **kwargs) -> str:
    """Expend each spot symbol of the exchange"""
    ti = kwargs["ti"]
    logging.info(exchange)
    logging.info(f"kwargs: {kwargs}")
    logging.info(f"""Previous Reture Value: {ti.xcom_pull("task_start", "return_value")}""")
    ret = (
        check_orderbook.override(task_id=f"check_{exchange}_spot_orderbook")
        .partial(exchange=exchange)
        .expand(symbol=[f"symbol_{i}" for i in range(SYMBOL_COUNT)])
    )
    logging.info(ret)
    return ""


@task
def check_future_orderbook(exchange: str = "", **kwargs) -> str:
    """Expend each future symbol of the exchange"""
    ti = kwargs["ti"]
    logging.debug(exchange)
    logging.info(f"kwargs: {kwargs}")
    logging.info(f"""Previous Reture Value: {ti.xcom_pull("task_start", "return_value")}""")
    ret = check_orderbook.partial(exchange=exchange).expand(symbol=[f"symbol_{i}" for i in range(SYMBOL_COUNT)])
    logging.info(ret)
    return ""


@task(
    sla=timedelta(hours=2),
    on_failure_callback=MyNotifier(),
    on_success_callback=SlackNotifier(
        slack_conn_id=SLACK_CONNECTION_ID,
        text=SLACK_MESSAGE,
        channel=SLACK_CHANNEL,
    ),
)
def task_end(**kwargs) -> str:
    """Empty Task which is Last Task of Dag"""
    ti = kwargs["ti"]
    time.sleep(2)
    logging.info("Task_End")
    logging.info(f"kwargs: {kwargs}")
    logging.info(f"""Previous Reture Value: {ti.xcom_pull("task_start", "return_value")}""")
    return "Task_End"


# CAUTION, not support dynamic expend in other dynamic expend
@task_group()
def two_layer_task_group(exchange: str) -> None:
    """TaskGroup for grouping related Tasks"""
    logging.info(exchange)
    # two layer dynamic tasks
    ret = check_future_orderbook(check_spot_orderbook(exchange))
    logging.info(ret)


@task_group()
def one_layer_task_group() -> None:
    """Expend each symbol of each exchange in one layer"""
    logging.info("one_layer_task_group")
    for exchange in list_exchange():
        check_orderbook.override(task_id=f"check_{exchange}_spot_orderbook").partial(exchange=exchange).expand(
            symbol=[f"symbol_{i}" for i in range(SYMBOL_COUNT)]
        )


def list_exchange() -> list:
    return [f"exchange_{i}" for i in range(15)]


# Executing Tasks and TaskGroups
with DAG(
    dag_id=example_dags.DYNAMIC_SYMBOL_CHECKER,
    description="an example show dynamic task generation, notifier, parameters and trigger rule",
    doc_md=__doc__,
    default_args=default_args,
    schedule="@daily",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
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
        "my_multi_type_param": Param(
            default=5,
            type=["null", "number", "string"],
            section="my series",
        ),
        "my_date_time_param": Param(
            default=f"{(datetime.today() - timedelta(hours=12)).isoformat()}Z",
            description="Please select a date and time, use the button on the left for a pup-up calendar.",
            type="string",
            format="date-time",
            section="my series",
        ),
        "exchange": Param(
            default="exchange_0",
            enum=list_exchange(),
            example=["apple", "banana", "cat"],
        ),
        "multi_exchange": Param(
            default=["exchange_0", "exchange_10"],
            description="Select from the list of options.",
            type="array",
            examples=list_exchange(),
        ),
    },  # https://json-schema.org/draft/2020-12/json-schema-validation
    dagrun_timeout=duration(hours=3),
    # sla_miss_callback=sla_callback,
    catchup=False,
    on_failure_callback=callback,
    on_success_callback=callback,
    tags=[models.Major.example.value, "fan_out", "order_book"],
    dag_display_name=example_dags.DYNAMIC_SYMBOL_CHECKER + "_change_display_name",
) as dag:
    logging.info("hi")
    start_task = task_start()
    # it will make remain tasks status become skipped if trigger_rule=TriggerRule.ONE_SUCCESS
    end_task = task_end.override(trigger_rule=TriggerRule.ONE_SUCCESS)()

    current_task_group = check_spot_orderbook.expand(exchange=list_exchange())
    start_task >> current_task_group >> end_task

    start_task >> one_layer_task_group() >> end_task

    current_task_group = two_layer_task_group.expand(exchange=list_exchange())
    start_task >> current_task_group >> end_task
