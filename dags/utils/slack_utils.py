"""
Default callback function sends notifications to the slack channel `airflow-alerts`
A notification consists of
    1. the status of the DAG
    2. DAG ID
    3. execution time
    4. DAG run URL

Task id is not included in the notification yet since the callback function always get the task id of the task at
the end of the DAG regardless its status.
"""
import logging
import os
from dataclasses import dataclass
from enum import Enum
from urllib.parse import quote, urlparse

from airflow.exceptions import AirflowException
from airflow.providers.slack.notifications.slack import SlackNotifier


@dataclass
class Status(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"


def send_slack_alert(context, status: Status, slack_conn=None, slack_channel=None):
    if not slack_conn or not slack_channel:
        raise AirflowException("Slack connection ID or channel not found")

    try_number = context.get("task_instance").try_number

    dag_id = context.get("task_instance").dag_id
    encoded_dag_run_id = quote(context["dag_run"].run_id)

    execution_date = context.get("execution_date").strftime("%Y-%m-%d %H:%M:%S")
    execution_date_str = context.get("execution_date").strftime("%Y-%m-%dT%H:%M:%SZ")

    raw_url = context.get("task_instance").log_url
    parsed_url = urlparse(raw_url)

    env = os.getenv("ENV")
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    path = f"/dags/{dag_id}/grid?execution_date{execution_date_str}&dag_run_id{encoded_dag_run_id}&tab=graph"
    log_url = f"{base_url}{path}"

    attachments = [
        generate_attachment(
            status=status,
            messages=[
                f"*DAG ID*: {dag_id}",
                f"*DAG Run URL*: <{log_url}|View DAG Run>",
                f"*Execution Time*: {execution_date}",
                f"*Attempts*: {try_number}",
            ],
        )
    ]

    logging.debug(f"Sending slack alert to {slack_channel} through {slack_conn}")
    failed_alert = SlackNotifier(channel=slack_channel, slack_conn_id=slack_conn, attachments=attachments, text="")
    failed_alert(context)


def generate_attachment(status, messages: list = []):
    attachment = {}
    if status is Status.SUCCESS:
        color = "#00CF00"
        title = f"DAG Succeeded"
    elif status is Status.FAILED:
        color = "#A30000"
        title = f"DAG Failed"
    else:
        color = "#439FE0"
        title = f"DAG Status: {status}"

    attachment.update({"color": color})
    if messages:
        blocks = []
        blocks.append({"type": "header", "text": {"type": "plain_text", "text": title}})
        for message in messages:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": message}})
        attachment.update({"blocks": blocks})

    return attachment
