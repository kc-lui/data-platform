from datetime import timedelta

from airflow.models import Param

from dags.utils.slack_utils import Status, send_slack_alert


def callback(context):
    logging.info(context)


def on_failure_callback(context):
    logging.info(context)
    send_slack_alert(context, Status.FAILED)

# default_args only accepts a start_date string
default_args = {"owner": "data", "retries": 1, "retry_delay": timedelta(minutes=5), "start_date": "20210101"}
marketdata_default_args = {
    "owner": "data",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": "20210101",
    "on_success_callback": callback,
    "on_failure_callback": on_failure_callback,
}

def DRY_RUN(default=True, description: str = "dry run mode", type: str = "boolean", section: str = "developer mode"):
    return Param(default=default, description=description, type=type, section=section)

def BOOLEAN(default=True, description: str = "True or False", type: str = "boolean", section: str = "developer mode"):
    return Param(default=default, description=description, type=type, section=section)

def LOG_LEVEL(default="debug", enum: list = ["debug", "info", "error"], section: str = "developer mode"):
    return Param(default=default, enum=enum, section=section)

def DATE_PARAMS(
    default,
    description: str = "Please select a end datetime, dag will grabber till the end datetime",
    type: str = "string",
    format: str = "date",
):
    return Param(default=default, description=description, type=type, format=format)

def DATETIME_PARAMS(
    default,
    description: str = "Please select a end datetime, dag will grabber till the end datetime",
    type: str = "string",
    format: str = "date-time",
):
    return Param(default=default, description=description, type=type, format=format)
