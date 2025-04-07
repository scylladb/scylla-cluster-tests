# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2023 ScyllaDB
import logging
from pathlib import Path

import requests

from sdcm import cluster
from sdcm.wait import wait_for
from sdcm.remote import LOCALRUNNER
from sdcm.utils.git import clone_repo
from sdcm.utils.common import get_sct_root_path
from sdcm.utils.remote_logger import DockerComposeLogger
from sdcm.kafka.kafka_config import SctKafkaConfiguration

# TODO: write/think more about the consumers

LOGGER = logging.getLogger(__name__)


class LocalKafkaCluster(cluster.BaseCluster):
    def __init__(self, params, remoter=LOCALRUNNER):
        super().__init__(cluster_prefix="kafka", add_nodes=False, params=params)
        self.remoter = remoter
        self.docker_compose_path = (
            Path(get_sct_root_path()) / "kafka-stack-docker-compose"
        )
        self._journal_thread: DockerComposeLogger | None = None
        self.init_repository()

    def init_repository(self):
        # TODO: make the url configurable
        # TODO: get the version after install, and send out to Argus
        repo_url = "https://github.com/fruch/kafka-stack-docker-compose.git"
        branch = 'master'
        clone_repo(
            remoter=self.remoter,
            repo_url=repo_url,
            branch=branch,
            destination_dir_name=str(self.docker_compose_path),
            clone_as_root=False,
        )
        self.remoter.run(f'mkdir -p {self.docker_compose_path / "connectors"}')

    @property
    def compose_context(self):
        return f"cd {self.docker_compose_path}; docker compose -f full-stack.yml"

    @property
    def kafka_connect_url(self):
        return "http://localhost:8083"

    def compose(self, cmd):
        self.remoter.run(f"{self.compose_context} {cmd}")

    def start(self):
        self.compose("up -d")
        self.start_journal_thread()

    def stop(self):
        self._journal_thread.stop(timeout=120)
        self.compose("down")

    def install_connector(self, connector_version: str):
        if connector_version.startswith("hub:"):
            self.install_connector_from_hub(connector_version.replace("hub:", ""))
        else:
            self.install_connector_from_url(connector_version)

    def install_connector_from_hub(
        self, connector_version: str = "scylladb/scylla-cdc-source-connector:latest"
    ):
        self.compose(
            f"exec kafka-connect confluent-hub install --no-prompt {connector_version}"
        )
        self.compose("restart kafka-connect")

    def install_connector_from_url(self, connector_url: str):
        if connector_url.startswith("http"):
            if connector_url.endswith('.jar'):
                self.remoter.run(
                    f'curl -L --create-dirs -O --output-dir {self.docker_compose_path / "connectors"} {connector_url} '
                )
            if connector_url.endswith('.zip'):
                self.remoter.run(
                    f'curl -L -o /tmp/connector.zip {connector_url} && '
                    f'unzip -o /tmp/connector.zip -d {self.docker_compose_path / "connectors"} && rm /tmp/connector.zip'
                )
        if connector_url.startswith("file://"):
            connector_local_path = connector_url.replace("file://", "")
            if connector_url.endswith('.jar'):
                self.remoter.run(
                    f'cp {connector_local_path} {self.docker_compose_path / "connectors"}'
                )
            if connector_url.endswith('.zip'):
                self.remoter.run(
                    f'unzip {connector_local_path} -d {self.docker_compose_path / "connectors"}'
                )
        self.compose("restart kafka-connect")

        # TODO: find release based on 'curl https://api.github.com/repos/scylladb/scylla-cdc-source-connector/releases'

    def create_connector(
        self,
        db_cluster: cluster.BaseScyllaCluster,
        connector_config: SctKafkaConfiguration,
    ):
        # TODO: extend the number of tasks
        # TODO: handle client encryption SSL

        connector_data = connector_config.model_dump(by_alias=True, exclude_none=True)
        match connector_config.config.connector_class:
            case "io.connect.scylladb.ScyllaDbSinkConnector":
                scylla_addresses = [node.cql_address for node in db_cluster.nodes]
                connector_data["config"]["scylladb.contact.points"] = ",".join(scylla_addresses)
                if credentials := self.get_db_auth():
                    connector_data["config"]["scylladb.security.enabled"] = True
                    connector_data["config"]["scylladb.username"] = credentials[0]
                    connector_data["config"]["scylladb.password"] = credentials[1]

            case "com.scylladb.cdc.debezium.connector.ScyllaConnector":
                scylla_addresses = [f"{node.cql_address}:{node.CQL_PORT}" for node in db_cluster.nodes]
                connector_data["config"]["scylla.cluster.ip.addresses"] = ",".join(scylla_addresses)
                if credentials := self.get_db_auth():
                    connector_data["config"]["scylla.user"] = credentials[0]
                    connector_data["config"]["scylla.password"] = credentials[1]

        self.install_connector(connector_config.source)

        def kafka_connect_api_available():
            res = requests.head(url=self.kafka_connect_url)
            res.raise_for_status()
            return True

        wait_for(
            func=kafka_connect_api_available,
            step=2,
            text="waiting for kafka-connect api",
            timeout=120,
            throw_exc=True,
        )
        LOGGER.debug(connector_data)
        res = requests.post(
            url=f"{self.kafka_connect_url}/connectors", json=connector_data
        )
        LOGGER.debug(res)
        LOGGER.debug(res.text)
        res.raise_for_status()

    @property
    def kafka_log(self) -> Path:
        return Path(self.logdir) / "kafka.log"

    def start_journal_thread(self) -> None:
        self._journal_thread = DockerComposeLogger(self, str(self.kafka_log))
        self._journal_thread.start()

    def add_nodes(
        self,
        count,
        ec2_user_data="",
        dc_idx=0,
        rack=0,
        enable_auto_bootstrap=False,
        instance_type=None,
    ):
        raise NotImplementedError
