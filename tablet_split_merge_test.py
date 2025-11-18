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
# Copyright (c) 2025 ScyllaDB

from enum import StrEnum
from functools import cache, cached_property
from textwrap import dedent

from longevity_test import LongevityTest
from sdcm import cluster
from sdcm.sct_events import Severity
from sdcm.sct_events.health import DataValidatorEvent
from sdcm.sct_events.group_common_events import ignore_mutation_write_errors


REPLICATOR_URL = "https://mlitvk.s3.eu-north-1.amazonaws.com/scylla-cdc-replicator-1.3.8-SNAPSHOT-jar-with-dependencies.jar"
REPLICATOR_JAR = "replicator.jar"
REPLICATOR_LOG = "cdc-replicator.log"


class Mode(StrEnum):
    DELTA = "delta"
    PREIMAGE = "preimage"
    POSTIMAGE = "postimage"


class TabletSplitMergeTest(LongevityTest):
    @cached_property
    def enable_cdc(self) -> bool:
        return self.params["latte_schema_parameters"]["cdc"] == "true"

    @cached_property
    def ks_name(self) -> str:
        return self.params["latte_schema_parameters"]["ks_name"]

    @cached_property
    def table_name(self) -> str:
        return self.params["latte_schema_parameters"]["table_name"]

    @cached_property
    def mv_name(self) -> str:
        return self.params["latte_schema_parameters"]["mv_name"]

    def setUp(self) -> None:
        super().setUp()
        if self.enable_cdc:
            self.setup_replicator_tools(loader_node=self.loaders.nodes[0])

    def run_pre_create_keyspace(self) -> None:
        super().run_pre_create_schema()

        if self.enable_cdc:
            with self.cs_db_cluster.cql_connection_patient(node=self.cs_db_cluster.nodes[0]) as sess:
                self.log.info("Create the keyspace on the replica cluster.")
                sess.execute(dedent(f"""\
                    CREATE KEYSPACE IF NOT EXISTS {self.ks_name}
                        WITH replication = {{
                            'class' : 'NetworkTopologyStrategy',
                            'replication_factor' : 3
                        }}
                        AND tablets = {{ 'enabled' : true }}
                """))

    @cache
    def run_post_latte_schema_cmd(self) -> None:
        if self.enable_cdc:
            with self.db_cluster.cql_connection_patient(node=self.db_cluster.nodes[0]) as sess:
                table = sess.cluster.metadata.keyspaces[self.ks_name].tables[self.table_name]
                del table.extensions["cdc"]
            self.log.info("Create the table on the replica cluster.")
            with self.cs_db_cluster.cql_connection_patient(node=self.cs_db_cluster.nodes[0]) as sess:
                sess.execute(table.as_cql_query())
            self.start_replicator(mode=Mode.DELTA)

    def test_custom_time(self) -> None:
        with ignore_mutation_write_errors():
            super().test_custom_time()

            # Prefer all nodes will be run before collect data for validation.
            # Increase timeout to wait for nemesis finish.
            if self.db_cluster.nemesis_threads:
                self.db_cluster.stop_nemesis(timeout=300)

            self.validate_data()

    def validate_data(self) -> None:
        if self.enable_cdc:
            self.log.info("Validate number of rows in master and replica.")
            count_query = f"SELECT COUNT(*) FROM {self.ks_name}.{self.table_name};"
            with self.db_cluster.cql_connection_patient(node=self.db_cluster.nodes[0]) as sess:
                master_row_count = sess.execute(count_query).current_rows[0].count
            with self.cs_db_cluster.cql_connection_patient(node=self.cs_db_cluster.nodes[0]) as sess:
                replica_row_count = sess.execute(count_query).current_rows[0].count
            if master_row_count != replica_row_count:
                DataValidatorEvent.DataValidator(
                    severity=Severity.ERROR,
                    error=f"Number of rows in master and replica do not match: {master_row_count=}, {replica_row_count=}",
                ).publish()
            else:
                DataValidatorEvent.DataValidator(
                    severity=Severity.NORMAL,
                    message=f"Number of rows in master and replica: {master_row_count}",
                ).publish()

    def setup_replicator_tools(self, loader_node: cluster.BaseNode) -> None:
        self.log.info("Installing tmux on loader node.")
        try:
            loader_node.install_package("tmux")
        except Exception as e:  # noqa: BLE001
            raise Exception(f"Could not install tmux: {e}")
        try:
            loader_node.install_package("openjdk-17-jre")
        except Exception as e:  # noqa: BLE001
            raise Exception(f"Could not install java: {e}")

        self.log.info("Getting replicator on loader node.")
        res = loader_node.remoter.run(cmd=f"wget {REPLICATOR_URL} -O {REPLICATOR_JAR}")
        if res.failed:
            self.fail("Could not obtain CDC replicator.")

    def start_replicator(self, mode: Mode) -> None:
        """Start scylla-cdc-replicator in a tmux session on the loader node.

        Redirect stdout and stderr to cdc-replicator.log file.
        """
        replicator_cmd = " ".join([
            "java", "-cp", REPLICATOR_JAR, "com.scylladb.cdc.replicator.Main",
            "-k", self.ks_name,
            "-t", self.table_name,
            "-s", self.db_cluster.nodes[0].external_address,
            "-d", self.cs_db_cluster.nodes[0].external_address,
            "-cl", "one",
            "-m", mode,
        ])
        replicator_script = dedent(f"""\
            tmux new-session -d -s replicator
            tmux pipe-pane -t replicator -o 'cat >> {REPLICATOR_LOG}'
            tmux send-keys -t replicator '{replicator_cmd}' ENTER
        """)

        self.log.debug("Replicator script:\n%s", replicator_script)

        self.log.info("Starting replicator.")
        res = self.loaders.nodes[0].remoter.run(cmd=replicator_script)
        if res.failed:
            self.fail("Could not start CDC replicator.")

    def get_email_data(self) -> dict:
        email_data = super().get_email_data()
        if self.enable_cdc:
            email_data.update({
                "number_of_oracle_nodes": self.params.get("n_test_oracle_db_nodes"),
                "oracle_ami_id": self.params.get("ami_id_db_oracle"),
                "oracle_db_version": self.cs_db_cluster.nodes[0].scylla_version if self.cs_db_cluster else "N/A",
                "oracle_instance_type": self.params.get("instance_type_db_oracle"),
            })
        return email_data
