"""
Module containing the table encryption-at-rest nemesis classes (AWS KMS provider).

Both nemesis create a temporary encrypted table, write/read data through it with
scylla-bench, optionally rotate the AWS KMS key, and then disable the encryption
again to make sure sstables are rewritten unencrypted.

# TODO: add support for the 'LocalFileSystemKeyProviderFactory' and 'KmipKeyProviderFactory' key providers
# TODO: add encryption for a table with large partitions?
"""

import abc
import time

from sdcm.exceptions import UnsupportedNemesis
from sdcm.nemesis import NemesisBaseClass, target_all_nodes
from sdcm.sct_events import Severity
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.filters import EventsSeverityChangerFilter
from sdcm.sct_events.group_common_events import suppress_expected_unavailability_errors, decorate_with_context
from sdcm.sct_events.loaders import ScyllaBenchEvent
from sdcm.utils.aws_kms import AwsKms
from sdcm.utils.cql_utils import cql_quote_if_needed, cql_unquote_if_needed
from sdcm.utils.decorators import retrying
from sdcm.utils.sstable.sstable_utils import SstableUtils


@retrying(n=4, sleep_time=30, allowed_exceptions=(AssertionError,))
def _check_encryption_fact(sstable_util_instance, expected_bool_value):
    sstable_util_instance.check_that_sstables_are_encrypted(expected_bool_value=expected_bool_value)


class EnableDisableTableEncryptionBaseMonkey(NemesisBaseClass, abc.ABC):
    """Shared logic for the AWS KMS table-encryption nemesis (with/without key rotation)."""

    def _upgrade_sstables(self, nodes, keyspace_name, table_name):
        self.runner.actions_log.info("Upgrade sstables for the new encrypted table on all nodes")
        for node in nodes:
            self.runner.log.info("Upgradesstables on the '%s' node for the new encrypted table", node.name)
            # NOTE: 'flush' is needed in case there are no sstables yet
            node.remoter.run(f"nodetool flush -- {keyspace_name} {table_name}", verbose=True)
            # NOTE: 'flush' is needed for system_schema, to make sure the new table info
            # is on disk, `scylla sstable` reads only from disk
            node.remoter.run("nodetool flush -- system_schema", verbose=True)
            time.sleep(2)
            node.remoter.run(f"nodetool upgradesstables -a -- {keyspace_name} {table_name}", verbose=True)
        self.runner.actions_log.info("Upgraded sstables for the new encrypted table on all nodes")

    def _run_write_scylla_bench_load(self, write_cmd):
        # NOTE: 'scylla-bench' runs 'truncate' operation when 'validate-data' is used in addition
        #       to the 'write' mode. So it may cause racy following loader error:
        #
        #       Error during truncate: seastar::rpc::remote_verb_error (filesystem error: \
        #         link failed: No such file or directory
        #       It also may cause the 'sstable - Error while linking SSTable' error messages in DB logs
        with (
            EventsSeverityChangerFilter(
                new_severity=Severity.WARNING,
                event_class=ScyllaBenchEvent,
                extra_time_to_expiration=30,
                regex=r".*Error during truncate: seastar::rpc::remote_verb_error \(filesystem error.*",
            ),
            EventsSeverityChangerFilter(
                new_severity=Severity.WARNING,
                event_class=DatabaseLogEvent,
                extra_time_to_expiration=30,
                regex=".*sstable - Error while linking SSTable.*filesystem error: stat failed: No such file or directory.*",
            ),
            self.runner.action_log_scope(f"Write data with scylla-bench using cmd: {write_cmd}"),
        ):
            write_thread = self.runner.tester.run_stress_thread(stress_cmd=write_cmd, stop_test_on_failure=False)
            self.runner.tester.verify_stress_thread(
                write_thread, error_handler=self.runner._nemesis_stress_failure_handler
            )

    @decorate_with_context(suppress_expected_unavailability_errors)
    def _enable_disable_table_encryption(self, enable_kms_key_rotation, additional_scylla_encryption_options=None):  # noqa: PLR0914
        if self.runner.cluster.params.get("cluster_backend") != "aws":
            raise UnsupportedNemesis("This nemesis is supported only on the AWS cluster backend")

        scylla_encryption_options = {"cipher_algorithm": "AES/ECB/PKCS5Padding", "secret_key_strength": 128}
        scylla_encryption_options |= additional_scylla_encryption_options or {}
        aws_kms, kms_key_alias_name = None, None

        # Handle AWS KMS specific parts
        if (
            additional_scylla_encryption_options
            and additional_scylla_encryption_options.get("key_provider", "N/A") == "KmsKeyProviderFactory"
        ):
            kms_host_name = "kms-host"
            kms_key_alias_name = f"alias/testid-{self.runner.cluster.test_config.test_id()}"
            scylla_encryption_options |= {"kms_host": kms_host_name}
            aws_kms = AwsKms(region_names=self.runner.cluster.params.region_names)
            aws_kms.create_alias(kms_key_alias_name)
            self.runner.actions_log.info("Reconfigure Scylla nodes to use AWS KMS")
            for node in self.runner.cluster.nodes:
                is_restart_needed = False
                with node.remote_scylla_yaml() as scylla_yml:
                    if not scylla_yml.kms_hosts:
                        scylla_yml.kms_hosts = {}
                    if kms_host_name not in scylla_yml.kms_hosts:
                        scylla_yml.kms_hosts[kms_host_name] = {
                            "master_key": kms_key_alias_name,
                            "aws_region": node.region,
                            "aws_use_ec2_credentials": True,
                        }
                        is_restart_needed = True
                if is_restart_needed:
                    node.restart_scylla()
            self.runner.actions_log.info("Reconfigured Scylla nodes to use AWS KMS")

        # Create table with encryption
        keyspace_name, table_name = (
            cql_unquote_if_needed(self.runner.cluster.get_test_keyspaces()[0]),
            "tmp_encrypted_table",
        )
        self.runner.actions_log.info(f"Create encrypted table: {keyspace_name}.{table_name}")
        with self.runner.cluster.cql_connection_patient(self.runner.target_node, keyspace=keyspace_name) as session:
            # NOTE: scylla-bench expects following table structure:
            #       (pk bigint, ck bigint, v blob, PRIMARY KEY(pk, ck)) WITH compression = { }
            create_table_query_cmd = (
                f"CREATE TABLE IF NOT EXISTS {table_name}"
                " (pk bigint, ck bigint, v blob, PRIMARY KEY (pk, ck))"
                " WITH compression = { } AND read_repair_chance=0.0"
                f" AND compaction = {{ 'class' : '{self.runner.cluster.params.get('compaction_strategy')}' }}"
                f" AND scylla_encryption_options = {scylla_encryption_options};"
            )
            session.execute(create_table_query_cmd)

        try:
            for i in range(2 if (aws_kms and kms_key_alias_name and enable_kms_key_rotation) else 1):
                # Write data
                write_cmd = (
                    "scylla-bench -mode=write -workload=sequential -consistency-level=all -replication-factor=3"
                    " -partition-count=50 -clustering-row-count=100 -clustering-row-size=uniform:75..125"
                    f" -keyspace '{cql_quote_if_needed(keyspace_name)}' -table '{cql_quote_if_needed(table_name)}' -timeout=120s -validate-data"
                )
                self._run_write_scylla_bench_load(write_cmd)
                self._upgrade_sstables(self.runner.cluster.data_nodes, keyspace_name, table_name)

                # Read data
                read_cmd = (
                    "scylla-bench -mode=read -workload=sequential -consistency-level=all -replication-factor=3"
                    " -partition-count=50 -clustering-row-count=100 -clustering-row-size=uniform:75..125"
                    f" -keyspace '{cql_quote_if_needed(keyspace_name)}' -table '{cql_quote_if_needed(table_name)}' -timeout=120s -validate-data"
                    " -iterations=1 -concurrency=10 -connection-count=10 -rows-per-request=10"
                )
                with self.runner.action_log_scope(f"Read data with scylla-bench with {read_cmd}"):
                    read_thread = self.runner.tester.run_stress_thread(stress_cmd=read_cmd, stop_test_on_failure=False)
                    self.runner.tester.verify_stress_thread(
                        read_thread, error_handler=self.runner._nemesis_stress_failure_handler
                    )

                # Rotate KMS key
                if enable_kms_key_rotation and aws_kms and kms_key_alias_name and i == 0:
                    self.runner.actions_log.info(f"Rotate AWS KMS key. Alias name: {kms_key_alias_name}")
                    aws_kms.rotate_kms_key(kms_key_alias_name)

            # Check that sstables of that table are really encrypted
            sstable_util = SstableUtils(db_node=self.runner.target_node, ks_cf=f"{keyspace_name}.{table_name}")
            _check_encryption_fact(sstable_util, True)

            with self.runner.target_node.remote_scylla_yaml() as scylla_yaml:
                user_info_encryption_enabled = (
                    scylla_yaml.user_info_encryption and scylla_yaml.user_info_encryption.get("enabled", False)
                )

            # if encryption is enabled by default, we currently can't disable it
            if not user_info_encryption_enabled:
                # Disable encryption for the encrypted table
                self.runner.actions_log.info(f"Disable encryption for {keyspace_name}.{table_name}")
                with self.runner.cluster.cql_connection_patient(
                    self.runner.target_node, keyspace=keyspace_name
                ) as session:
                    query = f"ALTER TABLE {table_name} WITH scylla_encryption_options = {{'key_provider': 'none'}};"
                    session.execute(query)
                self._upgrade_sstables(self.runner.cluster.nodes, keyspace_name, table_name)

                # ReRead data
                read_thread2 = self.runner.tester.run_stress_thread(stress_cmd=read_cmd, stop_test_on_failure=False)
                self.runner.tester.verify_stress_thread(
                    read_thread2, error_handler=self.runner._nemesis_stress_failure_handler
                )

                # ReWrite data making the sstables be rewritten
                self._run_write_scylla_bench_load(write_cmd)
                self._upgrade_sstables(self.runner.cluster.nodes, keyspace_name, table_name)

                # ReRead data
                read_thread3 = self.runner.tester.run_stress_thread(stress_cmd=read_cmd, stop_test_on_failure=False)
                self.runner.tester.verify_stress_thread(
                    read_thread3, error_handler=self.runner._nemesis_stress_failure_handler
                )

                # Check that sstables of that table are not encrypted anymore
                _check_encryption_fact(sstable_util, False)
        finally:
            # Delete table
            self.runner.actions_log.info(f"Delete encrypted table {keyspace_name}.{table_name}")
            with self.runner.cluster.cql_connection_patient(self.runner.target_node, keyspace=keyspace_name) as session:
                session.execute(f"DROP TABLE {table_name};")


@target_all_nodes
class EnableDisableTableEncryptionAwsKmsProviderWithRotationMonkey(EnableDisableTableEncryptionBaseMonkey):
    disruptive = True
    kubernetes = False  # Enable it when EKS SCT code starts supporting the KMS service

    def disrupt(self):
        self._enable_disable_table_encryption(
            enable_kms_key_rotation=True, additional_scylla_encryption_options={"key_provider": "KmsKeyProviderFactory"}
        )


@target_all_nodes
class EnableDisableTableEncryptionAwsKmsProviderWithoutRotationMonkey(EnableDisableTableEncryptionBaseMonkey):
    disruptive = True
    kubernetes = False  # Enable it when EKS SCT code starts supporting the KMS service

    def disrupt(self):
        self._enable_disable_table_encryption(
            enable_kms_key_rotation=False,
            additional_scylla_encryption_options={"key_provider": "KmsKeyProviderFactory"},
        )
