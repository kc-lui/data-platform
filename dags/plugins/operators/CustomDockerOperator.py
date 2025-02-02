import time

import docker
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator


class CustomDockerOperator(BaseOperator):
    def __init__(
        self,
        image,
        container_name=None,
        network=None,
        detach=False,
        command=None,
        ports: dict = None,
        docker_url="unix://var/run/docker.sock",
        check_interval=10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.image = image
        self.client = None  # delay docker client initialization
        self.container_name = container_name
        self.network = network
        self.detach = detach
        self.command = command
        self.ports = ports or {}
        self.docker_url = docker_url
        self.check_interval = check_interval

    def execute(self, context):
        # lazy initialization
        if self.client is None:
            self.client = docker.from_env()

        container = self._get_container(self.container_name)
        if not container:
            container = self.client.containers.run(
                image=self.image,
                command=self.command,
                network=self.network,
                ports=self.ports,
                detach=self.detach,
                name=self.container_name,
            )
            self.log.info(f"Container {self.container_name} started in detach mode")

        while True:
            container_status = self._get_container_status(self.container_name)
            if container_status == "exited":
                self.log.error(f"Container {self.container_name} exited unexpectedly")
                raise AirflowException(
                    f"Container {self.container_name} exited unexpectedly with status: {container_status}"
                )
            elif container_status == "running":
                self.log.info(f"Container {self.container_name} is running")
                break
            else:
                self.log.info(f"Container {self.container_name} status: {container_status}")
            time.sleep(self.check_interval)

        self.log.info(f"Container {self.container_name} is confirmed running")
        return f"Container {self.container_name} is running"

    def _get_container(self, container_name):
        container = None
        try:
            container = self.client.containers.get(container_name)
            self.log.info(f"container {container_name} already exists.")
        except docker.errors.NotFound:
            self.log.info(f"container {container_name} does not exist.")

        return container

    def _get_container_status(self, container_name):
        low_level_client = docker.DockerClient(base_url=self.docker_url)
        container = low_level_client.containers.get(container_name)
        status = container.attrs["State"]["Status"]

        return status
