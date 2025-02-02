import functools
import inspect

import pytest
from airflow.models import DagBag
from airflow.notifications.basenotifier import BaseNotifier

import dags.utils.models as models

# @pytest.mark.parametrize("dag_path", DAG_PATHS)


@pytest.fixture()
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag is not None
    assert dagbag.import_errors == {}

    for dag_id, dag in dagbag.dags.items():
        assert len(dag.tasks) > 0
        assert dag.catchup is not True, "it will make backfill from start date automatically"
        assert (
            dag.is_paused_upon_creation is not True
        ), "it will make all developer run the job when launch airflow as dev environment"


def test_tags(dagbag):
    team_tag = [e.value for e in models.Team]
    major_tag = [e.value for e in models.Major]

    for dag_id, dag in dagbag.dags.items():
        assert (
            any(t in team_tag for t in dag.tags) if not any(models.Major.example.value == t for t in dag.tags) else True
        ), "please add team tag"
        assert any(t in major_tag for t in dag.tags), "please add major feature tag"


def test_queue(dagbag):
    queues = [e.value for e in models.Queue]

    for dag_id, dag in dagbag.dags.items():
        assert all(t.queue in queues for t in dag.tasks), f"wrong queue label: {[t.queue for t in dag.tasks]}"


def test_callback_functions_accept_context(dagbag):
    for dag_id, dag in dagbag.dags.items():
        # check DAG-level callbacks
        callbacks = [
            ("on_success_callback", dag.on_success_callback),
            ("on_failure_callback", dag.on_failure_callback),
        ]
        for callback_name, callback in callbacks:
            if callback is not None:
                if isinstance(callback, BaseNotifier):
                    continue
                elif isinstance(callback, functools.partial):
                    sig = inspect.signature(callback.func)
                    partial_args = callback.args or ()
                    partial_kwargs = callback.keywords or {}
                    params = list(sig.parameters.values())
                    params = params[len(partial_args) :]
                    params = [p for p in params if p.name not in partial_kwargs]
                    accepts_context = any(param.name == "context" for param in params)
                elif inspect.isfunction(callback):
                    sig = inspect.signature(callback)
                    params = sig.parameters.values()
                    accepts_context = any(param.name == "context" for param in params)
                else:
                    assert False, f"Callback '{callback_name}' in DAG '{dag_id}' is not callable."
                assert (
                    accepts_context
                ), f"Callback '{callback_name}' in DAG '{dag_id}' must accept 'context' as a parameter."

        # check task-level callbacks
        for task_id, task in dag.task_dict.items():
            task_callbacks = [
                ("on_success_callback", task.on_success_callback),
                ("on_failure_callback", task.on_failure_callback),
            ]
            for callback_name, callback in task_callbacks:
                if callback is not None:
                    if isinstance(callback, BaseNotifier):
                        continue
                    elif isinstance(callback, functools.partial):
                        sig = inspect.signature(callback.func)
                        partial_args = callback.args or ()
                        partial_kwargs = callback.keywords or {}
                        params = list(sig.parameters.values())
                        params = params[len(partial_args) :]
                        params = [p for p in params if p.name not in partial_kwargs]
                        accepts_context = any(param.name == "context" for param in params)
                    elif inspect.isfunction(callback):
                        sig = inspect.signature(callback)
                        params = sig.parameters.values()
                        accepts_context = any(param.name == "context" for param in params)
                    else:
                        assert (
                            False
                        ), f"Callback '{callback_name}' in task '{task_id}' of DAG '{dag_id}' is not callable."
                    assert accepts_context, f"""Callback '{callback_name}' in task '{task_id}' of DAG '{dag_id}'
                            must accept 'context' as a parameter."""
