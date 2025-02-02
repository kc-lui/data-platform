from time import sleep

import pendulum
from airflow import DAG
from airflow.decorators import task, task_group

import dags.utils.models as models
from dags.dag_id import ExampleDags

example_dags = ExampleDags()

with DAG(
    dag_id=example_dags.DYNAMIC_FAN_OUT,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=[models.Major.example.value, "dynamic task group"],
) as dag:

    @task(multiple_outputs=True)
    def get_greetings():
        return {
            "salutation": "Hello",
            "names": ["John", "Jane", "Jack"],
        }

    greetings = get_greetings()

    @task_group()
    def greet_by_name(name, salutation):
        @task()
        def format_greeting(name, salutation):
            if name == "John":
                sleep(20)
            return f"{salutation}, {name}"

        @task()
        def print_message(msg):
            print(msg)

        print_message(format_greeting(name, salutation))

    greet_everyone = greet_by_name.partial(salutation=greetings["salutation"]).expand(name=greetings["names"])
