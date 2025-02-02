from datetime import datetime, timedelta
from functools import partial

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from dag_id import ExampleDags

from dags.utils.consts import SLACK_ALERT_DEV_CHANNEL
from dags.utils.models import Major
from dags.utils.slack_utils import Status, send_slack_alert

example_dags = ExampleDags()

with DAG(
    dag_id=example_dags.SLACK_CALLBACK,
    description="send_slack",
    doc_md=__doc__,
    default_args={"owner": "data", "retries": 0, "retry_delay": timedelta(minutes=5), "start_date": "20210101"},
    start_date=datetime(2024, 8, 22),
    on_failure_callback=partial(
        send_slack_alert, slack_conn="slack_api_connection", status=Status.FAILED, slack_channel=SLACK_ALERT_DEV_CHANNEL
    ),
    schedule=None,
    catchup=False,
    tags=[Major.example.value, "callback_function"],
) as dag:

    @task
    def raise_an_error():
        raise AirflowException()

    @task
    def say(input: str):
        return input

    greetings = ["hi", "ciao", "servus"]
    say.expand(input=greetings) >> raise_an_error()
