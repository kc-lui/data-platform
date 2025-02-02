# Data Platform

This repository provides a comprehensive data platform built using Apache Airflow and Docker.

## Installation
Python virtual environment
```sh
mkdir -p ENV
virtualenv ENV -p /usr/bin/python3
. ENV/bin/activate
```
Initlize Submodules
```sh
git submodule update --init --recursive
```
Install airflow and related package for local implementation.
```sh
pip install -r requirements.txt
```
Install Docker and initlize airflow for local version
```sh
./setup.sh
```

## Development
### Source Code Structure
We are encouraged (but not required) to prioritize Docker Image over built-in over Python packages over git submodules,

#### Docker Image
Refer to this [example](./dags/example/docker_operator.py) for using `DockerOperator` to managea docker containers during DAG excution.

#### Build-In Source Code
Place the source code in `src` folder and ensure to:
 1. add your source path in `DockerFile`
 2. add your source path in `tests/__init__.py`
to make it run well.

#### Python packages
Modify `requirements.in` and compile it using:
``` sh
pip-compile -v requirements.in
```
### DAG
Create your dag file in `dags` folder, airflow will automatically detect and load them.

For cross-DAG functionality, we create some utilize in [`dags/utils/common.py`](./dags/utils/common.py), follow these guidelines:

- Call `init_task()` in your task function to retrieve DAG parameters.
```python
@task
def task_fun():
    params = init_task()
```

- Use `get_parmas_jinja()` in the caller to pass DAG parameters when using isolated Python virtual environments.
```python
@task.external_python
def task_fun(params):
	...

@task_group
def task_group_fun():
    task_fun(params=get_parmas_jinja())
```

## Test
### Basic Test
This test should ensure that your DAG does not contain any uninstalled dependency, syntax errors, etc. while loading.
```sh
python your_dag_file.py
python -m example.your_dag_file
```
### Unit Test
This test should list and test all DAG in `dags` folder, ensure it is runnable.
```sh
pytest tests -k test_dag_integrity.py
```
### Local Test
Launch airflow local version
```sh
docker compose build
docker compose up -d
open http://localhost:8080
# default account: airflow / airflow
# reference docker-compose.yaml for details
```
Shutdown airflow container
```
docker compose down --volumes --rmi all
```
