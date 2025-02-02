import logging
from ast import literal_eval
from urllib.parse import quote

import pendulum
from airflow.operators.python import get_current_context

from dags.utils.setup_logger import setup_logger


def get_parmas_jinja():
    return """{{ params["configuration"] if "configuration" in params else params }}"""


def get_context_params():
    context = get_current_context()
    params = context["params"]
    if "configuration" in params:
        params = {**params, **literal_eval(str(params["configuration"]))}
        del params["configuration"]
    return params


def init_task():
    params = get_context_params()
    setup_logger(params["log_level"])
    logging.debug(params)

    return params
