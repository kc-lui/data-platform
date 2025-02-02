import os
import socket
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras
import pymysql
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from dag_id import ExampleDags

import dags.utils.models as models
from dags.utils.consts import (
    SLACK_ALERT_DEV_CHANNEL,
    SLACK_ALERT_PROD_CHANNEL,
    TELEGRAM_RISK_CHAT_ID,
    TELEGRAM_TEST_CHAT_ID,
)


def get_slack_channel_by_env():
    env = os.environ.get("ENV")
    if env == "dev":
        return SLACK_ALERT_DEV_CHANNEL
    elif env == "prod":
        return SLACK_ALERT_PROD_CHANNEL
    else:
        print(f"Error env:{env}")


def get_telegram_channel_by_env():
    env = os.environ.get("ENV")
    if env == "dev":
        return TELEGRAM_TEST_CHAT_ID  # ryan-test
    elif env == "prod":
        return TELEGRAM_RISK_CHAT_ID
    else:
        print(f"Error env:{env}")


class DatabaseConnection:
    @staticmethod
    def get_connection_vars(conn_id):
        connection = BaseHook.get_connection(conn_id)
        return {
            "host": connection.host,
            "port": connection.port,
            "user": connection.login,
            "password": connection.password,
            "database": connection.schema,
        }


class SlackConnection:
    @staticmethod
    def get_connection_vars(conn_id):
        connection = BaseHook.get_connection(conn_id)
        return {
            "bot_token": connection.password,
        }


class TelegramConnection:
    @staticmethod
    def get_connection_vars(conn_id):
        connection = BaseHook.get_connection(conn_id)
        return {
            "bot_token": connection.password,
        }


class DatabaseOperations:
    @staticmethod
    def query_mysql(conn_id):
        conn_vars = DatabaseConnection.get_connection_vars(conn_id)
        print(f"Connecting to MySQL at {conn_vars['host']}:{conn_vars['port']}, Database: {conn_vars['database']}")
        with pymysql.connect(**conn_vars) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT version();")
                result = cursor.fetchone()
                print(f"Connected to database: {result[0]}")

    @staticmethod
    def query_postgres(conn_id):
        conn_vars = DatabaseConnection.get_connection_vars(conn_id)
        print(f"Connecting to PostgreSQL at {conn_vars['host']}:{conn_vars['port']}, Database: {conn_vars['database']}")
        with psycopg2.connect(**conn_vars) as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute("SELECT version();")
                result = cursor.fetchone()
                print(f"Connected to database: {result[0]}")


class SlackOperations:
    @staticmethod
    def send_message(conn_id, channel):
        conn_vars = SlackConnection.get_connection_vars(conn_id)
        bot_token = conn_vars["bot_token"]
        hostname = socket.gethostname()

        payload = {
            "text": f"Airflow example_connections_test. conn_id: {conn_id} hostname: {hostname}",
            "channel": channel,
        }

        headers = {"Authorization": f"Bearer {bot_token}"}

        response = requests.request("POST", "https://slack.com/api/chat.postMessage", headers=headers, data=payload)

        print(response.text)


class SftpOperations:
    @staticmethod
    def create_file(folder_path):

        # create folder
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        # create 3 files
        for i in range(3):
            file_path = f"{folder_path}/test_{i}.txt"
            with open(file_path, "w") as f:
                f.write("Hello, World!")


class TelegramOperations:
    @staticmethod
    def send_message(conn_id, channel):
        conn_vars = TelegramConnection.get_connection_vars(conn_id)
        bot_token = conn_vars["bot_token"]
        hostname = socket.gethostname()

        payload = {
            "text": f"Airflow example_connections_test. conn_id: {conn_id} hostname: {hostname}",
            "chat_id": channel,
        }

        response = requests.request("POST", f"https://api.telegram.org/bot{bot_token}/sendMessage", data=payload)

        print(response.text)


default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

example_dags = ExampleDags()

with DAG(
    dag_id=example_dags.EXAMPLE_CONNECTIONS_TEST_DAG_ID,
    description=example_dags.EXAMPLE_CONNECTIONS_TEST_DAG_ID,
    doc_md=__doc__,
    default_args=default_args,
    start_date=datetime(2024, 11, 19),
    schedule=None,
    catchup=False,
    tags=[models.Major.example.value, "connection_test"],
) as dag:

    mysql_conn_id_list = [
        "mysql_master_trading",
        "mysql_slave_trading",
        "mysql_master_woox_analysis",
        "mysql_master_marketdata",
        "mysql_slave_marketdata",
        "mysql_slave_trading_record",
        "mysql_slave_custody",
    ]

    for conn_id in mysql_conn_id_list:
        query_task = PythonOperator(
            task_id=f"query_mysql_task_{conn_id}",
            python_callable=DatabaseOperations.query_mysql,
            op_kwargs={"conn_id": conn_id},
        )

    redshift_conn_id_list = [
        "redshift_serverless",
        "redshift_provision",
    ]

    for conn_id in redshift_conn_id_list:
        query_task = PythonOperator(
            task_id=f"query_redshift_task_{conn_id}",
            python_callable=DatabaseOperations.query_postgres,
            op_kwargs={"conn_id": conn_id},
        )

    slack_conn_id_list = ["slack_airflow_notifier"]

    for conn_id in slack_conn_id_list:
        channel = get_slack_channel_by_env()
        query_task = PythonOperator(
            task_id=f"send_slack_message_task_{conn_id}_{channel}",
            python_callable=SlackOperations.send_message,
            op_kwargs={"conn_id": conn_id, "channel": channel},
        )

    ssh_conn_id_list = ["ssh_tokyo547"]

    for conn_id in ssh_conn_id_list:

        folder_path = "/tmp/airflow-test"

        ssh_task = SSHOperator(
            task_id=f"SSHOperator_task_{conn_id}",
            ssh_conn_id=conn_id,
            command="hostname",
        )

        # create_file and put_file need to run on same worker
        create_file = PythonOperator(
            task_id=f"create_file_task_{conn_id}",
            python_callable=SftpOperations.create_file,
            op_kwargs={"folder_path": folder_path},
            queue="accounting01",
        )

        # get all file path in folder
        file_paths = []
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_paths.append(os.path.join(root, file))

        put_file = SFTPOperator(
            task_id=f"SFTPOperator_task_{conn_id}",
            ssh_conn_id=conn_id,
            local_filepath=file_paths,
            remote_filepath=file_paths,
            operation="put",
            create_intermediate_dirs=True,
            queue="accounting01",
        )

        create_file >> put_file

    telegram_conn_id_list = ["telegram_risk_admin_bot"]

    for conn_id in telegram_conn_id_list:
        channel = get_telegram_channel_by_env()
        query_task = PythonOperator(
            task_id=f"send_telegram_message_task_{conn_id}_{channel}",
            python_callable=TelegramOperations.send_message,
            op_kwargs={"conn_id": conn_id, "channel": channel},
        )
