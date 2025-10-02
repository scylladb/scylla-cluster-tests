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
# Copyright (c) 2020 ScyllaDB

# This is stress longevity test that runs lightweight transactions in parallel with different node operations:
# disruptive and not disruptive
#
# After the test is finished will be performed the data validation.

import time
from enum import StrEnum
from textwrap import dedent
from unittest.mock import MagicMock

from longevity_test import LongevityTest
from sdcm import cluster
from sdcm.sct_events import Severity
from sdcm.sct_events.health import DataValidatorEvent
from sdcm.utils.data_validator import LongevityDataValidator
from sdcm.utils.common import skip_optional_stage
from sdcm.sct_events.group_common_events import ignore_mutation_write_errors


REPLICATOR_URL = "https://mlitvk.s3.eu-north-1.amazonaws.com/scylla-cdc-replicator-1.3.8-SNAPSHOT-jar-with-dependencies.jar"
REPLICATOR_JAR = "replicator.jar"
REPLICATOR_LOG = "cdc-replicator.log"


class Mode(StrEnum):
    DELTA = "delta"
    PREIMAGE = "preimage"
    POSTIMAGE = "postimage"


class LWTLongevityTest(LongevityTest):
    BASE_TABLE_PARTITION_KEYS = ['domain', 'published_date']

    def setUp(self):
        super().setUp()
        self.data_validator = None

    def run_prepare_write_cmd(self):
        # `mutation_write_*' errors are thrown when system is overloaded and got timeout on
        # operations on system.paxos table.
        #
        # Decrease severity of this event during prepare.  Shouldn't impact on test result.
        if not skip_optional_stage('prepare_write'):
            with ignore_mutation_write_errors():
                super().run_prepare_write_cmd()

        # Stop nemesis. Prefer all nodes will be run before collect data for validation
        # Increase timeout to wait for nemesis finish
        if self.db_cluster.nemesis_threads:
            self.db_cluster.stop_nemesis(timeout=300)

        # Wait for MVs data will be fully inserted (running on background)
        time.sleep(300)

        # Data validation can be run when no nemesis, that almost not happens in case of parallel nemesis.
        # If we even will catch period when no nemesis are running, it may happen that the nemesis will start in the
        # middle of data validation and fail it
        if self.db_cluster.nemesis_count > 1:
            self.data_validator = MagicMock()
            self.data_validator.keyspace_name = None
            DataValidatorEvent.DataValidator(severity=Severity.NORMAL,
                                             message="Test runs with parallel nemesis. Data validator is disabled."
                                             ).publish()
        else:
            self.data_validator = LongevityDataValidator(longevity_self_object=self,
                                                         user_profile_name='c-s_lwt',
                                                         base_table_partition_keys=self.BASE_TABLE_PARTITION_KEYS)

            self.data_validator.copy_immutable_expected_data()
            self.data_validator.copy_updated_expected_data()
            self.data_validator.save_count_rows_for_deletion()

        # Run nemesis during stress as it was stopped before copy expected data
        if self.params.get('nemesis_during_prepare'):
            self.start_nemesis()

    def start_nemesis(self):
        self.db_cluster.start_nemesis()

    def test_lwt_longevity(self):
        with ignore_mutation_write_errors():
            self.test_custom_time()

            # Stop nemesis. Prefer all nodes will be run before collect data for validation
            # Increase timeout to wait for nemesis finish
            if self.db_cluster.nemesis_threads:
                self.db_cluster.stop_nemesis(timeout=300)
            self.validate_data()

    def validate_data(self):
        node = self.db_cluster.nodes[0]
        if not (keyspace := self.data_validator.keyspace_name):
            DataValidatorEvent.DataValidator(severity=Severity.NORMAL,
                                             message="Failed fo get keyspace name. Data validator is disabled."
                                             ).publish()
            return

        with self.db_cluster.cql_connection_patient(node, keyspace=keyspace) as session:
            self.data_validator.validate_range_not_expected_to_change(session=session)
            self.data_validator.validate_range_expected_to_change(session=session)
            self.data_validator.validate_deleted_rows(session=session)


class LWTLongevityWithCDCReplicatorTest(LWTLongevityTest):
    KS_NAME = "lwt_keyspace"
    RF = 3
    TABLE_NAME = "lwt_io"
    MV_NAME = "lwt_mv"
    COMPACTION_STRATEGY = "IncrementalCompactionStrategy"

    def setUp(self) -> None:
        super().setUp()
        self.setup_tools(loader_node=self.loaders.nodes[0])

    def run_pre_create_schema(self) -> None:
        super().run_pre_create_schema()

        ks_query = dedent(f"""\
            CREATE KEYSPACE IF NOT EXISTS {self.KS_NAME}
            WITH replication = {{
                'class'              : 'NetworkTopologyStrategy',
                'replication_factor' : {self.RF}
            }}
            AND tablets = {{ 'enabled' : true }}
        """)

        table_query = dedent(f"""\
            CREATE TABLE IF NOT EXISTS {self.KS_NAME}.{self.TABLE_NAME} (
                pk bigint,
                ck bigint,
                a int,
                pad text,
                PRIMARY KEY (pk, ck)
            )
            WITH compaction = {{ 'class' : '{self.COMPACTION_STRATEGY}' }}
        """)

        enable_cdc_query = dedent(f"""\
            ALTER TABLE {self.KS_NAME}.{self.TABLE_NAME}
            WITH cdc = {{ 'enabled' : true }}
        """)

        mv_query = dedent(f"""\
            CREATE MATERIALIZED VIEW IF NOT EXISTS {self.KS_NAME}.{self.MV_NAME}
            AS SELECT pk, ck, a, pad
            FROM {self.KS_NAME}.{self.TABLE_NAME}
            WHERE pk IS NOT NULL
            AND ck IS NOT NULL
            PRIMARY KEY (pk, ck)
        """)

        self.log.info("Create the schema on the master cluster.")
        with self.db_cluster.cql_connection_patient(node=self.db_cluster.nodes[0]) as sess:
            sess.execute(ks_query)
            sess.execute(table_query)
            sess.execute(enable_cdc_query)
            sess.execute(mv_query)

        self.log.info("Create the schema on the replica cluster.")
        with self.cs_db_cluster.cql_connection_patient(node=self.cs_db_cluster.nodes[0]) as sess:
            sess.execute(ks_query)
            sess.execute(table_query)

        self.start_replicator(mode=Mode.DELTA)

    def validate_data(self) -> None:
        super().validate_data()

        self.log.info("Validate number of rows in master and replica.")
        count_query = f"SELECT COUNT(*) FROM {self.KS_NAME}.{self.TABLE_NAME};"
        with self.db_cluster.cql_connection_patient(node=self.db_cluster.nodes[0]) as sess:
            master_row_count = sess.execute(count_query).current_rows[0].count
        with self.cs_db_cluster.cql_connection_patient(node=self.cs_db_cluster.nodes[0]) as sess:
            replica_row_count = sess.execute(count_query).current_rows[0].count
        if master_row_count != replica_row_count:
            DataValidatorEvent.DataValidator(
                severity=Severity.ERROR,
                message=f"Number of rows in master and replica do not match: {master_row_count=}, {replica_row_count=}",
            ).publish()
        else:
            DataValidatorEvent.DataValidator(
                severity=Severity.NORMAL,
                message=f"Number of rows in master and replica: {master_row_count}",
            ).publish()

    def setup_tools(self, loader_node: cluster.BaseNode) -> None:
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
            "-k", self.KS_NAME,
            "-t", self.TABLE_NAME,
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
        email_data.update({
            "number_of_oracle_nodes": self.params.get("n_test_oracle_db_nodes"),
            "oracle_ami_id": self.params.get("ami_id_db_oracle"),
            "oracle_db_version": self.cs_db_cluster.nodes[0].scylla_version if self.cs_db_cluster else "N/A",
            "oracle_instance_type": self.params.get("instance_type_db_oracle"),
        })
        return email_data
