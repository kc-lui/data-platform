# airflow
FROM apache/airflow:2.10.4-python3.8

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

USER root

RUN groupadd --gid 998 docker \
    && usermod -aG docker airflow

RUN apt-get update \
    && apt-get install -y --no-install-recommends vim gcc build-essential libgmp-dev tzdata \
    && apt-get autoremove -yqq --purge

USER airflow
RUN pip -V
RUN pip show setuptools

# for python 3.12
# RUN python -m ensurepip --upgrade
# RUN pip install --no-cache-dir --upgrade setuptools

RUN pip install --upgrade pip pip-tools

RUN pip install --no-cache-dir -r requirements.txt
# RUN pip install --no-cache-dir apache-airflow==${AIRFLOW_VERSION}

ENV PYTHONPATH="${PYTHONPATH}:${AIRFLOW_HOME}/src"

# copy dags, src, plugin
COPY ./pools.json /opt/airflow/pools.json
COPY ./connections.json /opt/airflow/connections.json

CMD ["/bin/sh"]
