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

import contextlib

from performance_regression_test import PerformanceRegressionTest
from sdcm.sct_events.group_common_events import ignore_operation_errors, ignore_alternator_client_errors
from sdcm.utils import alternator


class PerformanceRegressionAlternatorTest(PerformanceRegressionTest):
<<<<<<< HEAD
    def __init__(self, *args):
        super().__init__(*args)
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
    '''
Performance tests for alternator.
Following workloads are supposed:
- test_full - all tests below
- test_latency - all latency tests (read write mixed)
- test_latency_read(self) - read only latency tests
- test_latency_write(self) - write only latency tests
- test_latency_mixed - read 50% write 50% latency tests
- test_throughput - all throughput tests
- test_throughput_read - read only throughput tests
- test_throughput_write - write only throughput tests
all tests are run with cql and alternator, with FORBID_RMW isolation and with ALWAYS_USE_LWT
    '''

    def setUp(self):
        super().setUp()
=======
    """
    Performance tests for alternator.
    Following workloads are supposed:
    - test_full - all tests below
    - test_latency - all latency tests (read write mixed)
    - test_latency_read(self) - read only latency tests
    - test_latency_write(self) - write only latency tests
    - test_latency_mixed - read 50% write 50% latency tests
    - test_throughput - all throughput tests
    - test_throughput_read - read only throughput tests
    - test_throughput_write - write only throughput tests
    all tests are run with cql and alternator, with FORBID_RMW isolation and with ALWAYS_USE_LWT
    """

    def setUp(self):
        super().setUp()
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

        # suppress YCSB client error and timeout to warnings for all the test in this class
        self.stack = contextlib.ExitStack()
        self.stack.enter_context(ignore_alternator_client_errors())
        self.stack.enter_context(ignore_operation_errors())
<<<<<<< HEAD
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        self.hdr_tags = ['READ', 'SCAN', 'UPDATE', 'INSERT', 'DELETE', 'WRITE']

    def _prepare_and_execute_workload_with_latency_calculator_decorator(self, *, test_name, row_name, target_ops_text, stress_num=1, **kwargs):
        self.log.info(f'Running workload with test_name={test_name}, stress_num={stress_num}, kwargs={kwargs}')
        try:
            # test_name must end with one of '_read', '_write', '_mixed', '_throughput_read', '_throughput_write'
            # indicating the type of workload that will be run
            cycle_name = str(target_ops_text)
            if test_name.endswith('_read'):
                self.params['workload_name'] = 'read'
            elif test_name.endswith('_write'):
                self.params['workload_name'] = 'write'
            elif test_name.endswith('_mixed'):
                self.params['workload_name'] = 'mixed'
            else:
                raise ValueError(
                    f"Unknown test_name {test_name} for workload, only test_name values ending with '_read', '_write', '_mixed', '_throughput_read', or '_throughput_write' are supported.")

            @latency_calculator_decorator(cycle_name=cycle_name, row_name=row_name)
            def execute_workload_with_latency_calculator_decorator(self, *args, **kwargs):
                return self._workload(*args, **kwargs)

            ret = execute_workload_with_latency_calculator_decorator(
                self, test_name=test_name, stress_num=stress_num, **kwargs)
            return ret
        except Exception as e:
            self.log.error(
                f"Error while running workload with test_name={test_name}, stress_num={stress_num}, kwargs={kwargs}: {e}")
            raise
        finally:
            self.log.info(f'Finished workload with test_name={test_name}, stress_num={stress_num}, kwargs={kwargs}')
=======
        self.hdr_tags = ["READ", "SCAN", "UPDATE", "INSERT", "DELETE", "WRITE"]

    def _prepare_and_execute_workload_with_latency_calculator_decorator(
        self, *, test_name, row_name, target_ops_text, stress_num=1, **kwargs
    ):
        self.log.info(f"Running workload with test_name={test_name}, stress_num={stress_num}, kwargs={kwargs}")
        try:
            # test_name must end with one of '_read', '_write', '_mixed', '_throughput_read', '_throughput_write'
            # indicating the type of workload that will be run
            cycle_name = str(target_ops_text)
            if test_name.endswith("_read"):
                self.params["workload_name"] = "read"
            elif test_name.endswith("_write"):
                self.params["workload_name"] = "write"
            elif test_name.endswith("_mixed"):
                self.params["workload_name"] = "mixed"
            else:
                raise ValueError(
                    f"Unknown test_name {test_name} for workload, only test_name values ending with '_read', '_write', '_mixed', '_throughput_read', or '_throughput_write' are supported."
                )

            @latency_calculator_decorator(cycle_name=cycle_name, row_name=row_name)
            def execute_workload_with_latency_calculator_decorator(self, *args, **kwargs):
                return self._workload(*args, **kwargs)

            ret = execute_workload_with_latency_calculator_decorator(
                self, test_name=test_name, stress_num=stress_num, **kwargs
            )
            return ret
        except Exception as e:
            self.log.error(
                f"Error while running workload with test_name={test_name}, stress_num={stress_num}, kwargs={kwargs}: {e}"
            )
            raise
        finally:
            self.log.info(f"Finished workload with test_name={test_name}, stress_num={stress_num}, kwargs={kwargs}")
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

    def _workload(
        self,
        stress_cmd,
        stress_num,
        test_name=None,
        sub_type=None,
        keyspace_num=1,
        prefix="",
        debug_message="",
        save_stats=True,
        is_alternator=True,
    ):
        if not is_alternator:
<<<<<<< HEAD
            stress_cmd = stress_cmd.replace('dynamodb', 'cassandra-cql')
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
            stress_cmd = stress_cmd.replace('dynamodb', 'scylla')
=======
            stress_cmd = stress_cmd.replace("dynamodb", "scylla")
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

        if debug_message:
            self.log.debug(debug_message)

        if save_stats:
<<<<<<< HEAD
            self.create_test_stats(test_name=test_name, sub_type=sub_type,
                                   doc_id_with_timestamp=True, append_sub_test_to_name=False)
        stress_queue = self.run_stress_thread(stress_cmd=stress_cmd, stress_num=stress_num, keyspace_num=keyspace_num,
                                              prefix=prefix, stats_aggregate_cmds=False)
        self.get_stress_results(queue=stress_queue, store_results=True)
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
            self.create_test_stats(test_name=test_name, sub_type=sub_type,
                                   doc_id_with_timestamp=True, append_sub_test_to_name=False)
        self.log.info(f'Starting stress cmd: {stress_cmd}')
        stress_queue = self.run_stress_thread(stress_cmd=stress_cmd, stress_num=stress_num, keyspace_num=keyspace_num,
                                              prefix=prefix, stats_aggregate_cmds=False)
        self.log.info(f'Started stress cmd: {stress_cmd}, waiting for results')
        try:
            self.get_stress_results(queue=stress_queue, store_results=True)
        except:
            self.log.exception(f'Stress cmd failed: {stress_cmd}')
            self.log.error(traceback.format_exc())
            raise
        self.log.info(f'Completed stress cmd: {stress_cmd}')
        self.build_histogram(self.params['workload_name'], hdr_tags=self.hdr_tags)
=======
            self.create_test_stats(
                test_name=test_name, sub_type=sub_type, doc_id_with_timestamp=True, append_sub_test_to_name=False
            )
        self.log.info(f"Starting stress cmd: {stress_cmd}")
        stress_queue = self.run_stress_thread(
            stress_cmd=stress_cmd,
            stress_num=stress_num,
            keyspace_num=keyspace_num,
            prefix=prefix,
            stats_aggregate_cmds=False,
        )
        self.log.info(f"Started stress cmd: {stress_cmd}, waiting for results")
        try:
            self.get_stress_results(queue=stress_queue, store_results=True)
        except:
            self.log.exception(f"Stress cmd failed: {stress_cmd}")
            self.log.error(traceback.format_exc())
            raise
        self.log.info(f"Completed stress cmd: {stress_cmd}")
        self.build_histogram(self.params["workload_name"], hdr_tags=self.hdr_tags)
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)
        if save_stats:
            self.update_test_details(scylla_conf=True, alternator=is_alternator)

    def create_alternator_table(self, schema, alternator_write_isolation):
        node = self.db_cluster.nodes[0]

        # drop tables
        table_name = alternator.consts.TABLE_NAME
        if self.alternator.is_table_exists(node=node, table_name=table_name):
            self.alternator.delete_table(node=node, table_name=table_name)
        # create new tables
        self.log.info("Going to create alternator tables")
        self.alternator.create_table(node=node, schema=schema, isolation=alternator_write_isolation)

        self.run_fstrim_on_all_db_nodes()
        self.wait_no_compactions_running()

    def create_cql_ks_and_table(self, field_number):
        node = self.db_cluster.nodes[0]
        with self.db_cluster.cql_connection_patient(node) as session:
            session.execute(
                """create keyspace ycsb WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor': 3 };"""
            )
            fields = ", ".join([f"field{i} varchar" for i in range(field_number)])
            session.execute(f"""CREATE TABLE ycsb.usertable (
                                y_id varchar primary key,
                                {fields});""")

<<<<<<< HEAD
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
    @optional_stage('perf_preload_data')
=======
    @optional_stage("perf_preload_data")
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)
    def preload_data(self, compaction_strategy=None):
        # if test require a pre-population of data
<<<<<<< HEAD
        prepare_write_cmd = self.params.get('prepare_write_cmd')
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        prepare_write_cmd = self.params.get('prepare_write_cmd')
        self.log.info(f'Preloading data with `{prepare_write_cmd}` write command.')
=======
        prepare_write_cmd = self.params.get("prepare_write_cmd")
        self.log.info(f"Preloading data with `{prepare_write_cmd}` write command.")
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)
        if prepare_write_cmd:
            # create new document in ES with doc_id = test_id + timestamp
            # allow to correctly save results for future compare
            self.create_test_stats(sub_type="write-prepare", doc_id_with_timestamp=True)
            stress_queue = []
<<<<<<< HEAD
            params = {'prefix': 'preload-'}
            for stress_type in ['dynamodb', 'cassandra-cql']:
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
            params = {'prefix': 'preload-'}
            for stress_type in ['dynamodb', 'scylla']:
=======
            params = {"prefix": "preload-"}
            for stress_type in ["dynamodb", "scylla"]:
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)
                # Check if the prepare_cmd is a list of commands
                if not isinstance(prepare_write_cmd, str) and len(prepare_write_cmd) > 1:
                    # Check if it should be round_robin across loaders
                    if self.params.get("round_robin"):
                        self.log.debug("Populating data using round_robin")
                        params.update({"stress_num": 1, "round_robin": True})

                    for stress_cmd in prepare_write_cmd:
<<<<<<< HEAD
                        params.update({'stress_cmd': stress_cmd.replace('dynamodb', stress_type)})
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
                        params.update({
                            'stress_cmd': stress_cmd.replace('dynamodb', stress_type),
                            'duration': self.params.get('prepare_stress_duration'),
                        })
=======
                        params.update(
                            {
                                "stress_cmd": stress_cmd.replace("dynamodb", stress_type),
                                "duration": self.params.get("prepare_stress_duration"),
                            }
                        )
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

                        # Run all stress commands
                        params.update(dict(stats_aggregate_cmds=False))
                        self.log.debug("RUNNING stress cmd: {}".format(stress_cmd.replace("dynamodb", stress_type)))
                        stress_queue.append(self.run_stress_thread(**params))

                # One stress cmd command
                else:
<<<<<<< HEAD
                    stress_queue.append(self.run_stress_thread(stress_cmd=prepare_write_cmd.replace('dynamodb', stress_type), stress_num=1,
                                                               prefix='preload-', stats_aggregate_cmds=False))
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
                    stress_queue.append(self.run_stress_thread(
                        stress_cmd=prepare_write_cmd.replace('dynamodb', stress_type),
                        duration=self.params.get('prepare_stress_duration'),
                        stress_num=1,
                        prefix='preload-',
                        stats_aggregate_cmds=False,
                    ))
=======
                    stress_queue.append(
                        self.run_stress_thread(
                            stress_cmd=prepare_write_cmd.replace("dynamodb", stress_type),
                            duration=self.params.get("prepare_stress_duration"),
                            stress_num=1,
                            prefix="preload-",
                            stats_aggregate_cmds=False,
                        )
                    )
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

<<<<<<< HEAD
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
            self.log.debug('Waiting for loaders...')
=======
            self.log.debug("Waiting for loaders...")
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)
            for stress in stress_queue:
                self.get_stress_results(queue=stress, store_results=False)
<<<<<<< HEAD

||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
            self.log.debug('Loaders completed.')
            self.build_histogram('<unused>', hdr_tags=['_tag_'])
=======
            self.log.debug("Loaders completed.")
            self.build_histogram("<unused>", hdr_tags=["_tag_"])
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)
            self.update_test_details()
        else:
            self.log.warning("No prepare command defined in YAML!")

<<<<<<< HEAD
    def test_write(self):
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
    def test_full(self):
        self.run_test_suite_by_configuration_name('full')

    def test_latency(self):
        self.run_test_suite_by_configuration_name('basic-read')

    def test_latency_read(self):
        self.run_test_suite_by_configuration_name('basic-read')

    def test_latency_write(self):
        self.run_test_suite_by_configuration_name('basic-write')

    def test_latency_mixed(self):
        self.run_test_suite_by_configuration_name('basic-mixed')

    def test_throughput(self):
        self.run_test_suite_by_configuration_name('basic-throughput')

    def test_throughput_read(self):
        self.run_test_suite_by_configuration_name('basic-throughput-read')

    def test_throughput_write(self):
        self.run_test_suite_by_configuration_name('basic-throughput-write')

    def run_test_suite_by_configuration_name(self, mode):  # noqa: PLR0914
=======
    def test_full(self):
        self.run_test_suite_by_configuration_name("full")

    def test_latency(self):
        self.run_test_suite_by_configuration_name("basic-read")

    def test_latency_read(self):
        self.run_test_suite_by_configuration_name("basic-read")

    def test_latency_write(self):
        self.run_test_suite_by_configuration_name("basic-write")

    def test_latency_mixed(self):
        self.run_test_suite_by_configuration_name("basic-mixed")

    def test_throughput(self):
        self.run_test_suite_by_configuration_name("basic-throughput")

    def test_throughput_read(self):
        self.run_test_suite_by_configuration_name("basic-throughput-read")

    def test_throughput_write(self):
        self.run_test_suite_by_configuration_name("basic-throughput-write")

    def run_test_suite_by_configuration_name(self, mode):  # noqa: PLR0914
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)
        """
        Test steps:

        1. Run a write workload with cql
        2. Run a write workload without lwt
        3. Run a write workload with lwt enabled
        """
        # run a write workload
        base_cmd_w = self.params.get('stress_cmd_w')
        stress_multiplier = self.params.get('stress_multiplier')

        self.create_cql_ks_and_table(field_number=10)

        self._workload(sub_type='cql', stress_cmd=base_cmd_w,
                       stress_num=stress_multiplier, keyspace_num=1, is_alternator=False)

        schema = self.params.get("dynamodb_primarykey_type")
        # run a workload without lwt as baseline
        self.create_alternator_table(
            schema=schema, alternator_write_isolation=alternator.enums.WriteIsolation.FORBID_RMW)

        self._workload(sub_type='without-lwt', stress_cmd=base_cmd_w, stress_num=stress_multiplier, keyspace_num=1)

        # run a workload with lwt
        self.create_alternator_table(
            schema=schema, alternator_write_isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
        self._workload(sub_type='with-lwt', stress_cmd=base_cmd_w, stress_num=stress_multiplier, keyspace_num=1)

        self.check_regression_with_baseline('cql')

    def test_read(self):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a read workload with cql
        3. Run a read workload without lwt
        4. Run a read workload with lwt enabled
        """
        node = self.db_cluster.nodes[0]

<<<<<<< HEAD
        base_cmd_r = self.params.get('stress_cmd_r')
        stress_multiplier = self.params.get('stress_multiplier')
        self.run_fstrim_on_all_db_nodes()
        # run a prepare write workload
        self.create_cql_ks_and_table(field_number=10)

        self.create_alternator_table(schema=self.params.get("dynamodb_primarykey_type"),
                                     alternator_write_isolation=alternator.enums.WriteIsolation.FORBID_RMW)

        self.preload_data()

        self._workload(sub_type='cql', stress_cmd=base_cmd_r,
                       stress_num=stress_multiplier, keyspace_num=1, is_alternator=False)

        # run a workload without lwt as baseline
        self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
        self._workload(sub_type='without-lwt', stress_cmd=base_cmd_r, stress_num=stress_multiplier, keyspace_num=1)

        # run a workload with lwt
        self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
        self._workload(sub_type='with-lwt', stress_cmd=base_cmd_r, stress_num=stress_multiplier, keyspace_num=1)

        self.check_regression_with_baseline('cql')

    def test_mixed(self):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a mixed workload with cql
        3. Run a mixed workload without lwt
        4. Run a mixed workload with lwt
        """
        node = self.db_cluster.nodes[0]

        base_cmd_m = self.params.get('stress_cmd_m')
        stress_multiplier = self.params.get('stress_multiplier')
        self.run_fstrim_on_all_db_nodes()
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        base_cmd_w = self.params.get('stress_cmd_w')
        base_cmd_r = self.params.get('stress_cmd_r')
        base_cmd_m = self.params.get('stress_cmd_m')
=======
        base_cmd_w = self.params.get("stress_cmd_w")
        base_cmd_r = self.params.get("stress_cmd_r")
        base_cmd_m = self.params.get("stress_cmd_m")
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

<<<<<<< HEAD
        self.create_cql_ks_and_table(field_number=10)
        self.create_alternator_table(schema=self.params.get("dynamodb_primarykey_type"),
                                     alternator_write_isolation=alternator.enums.WriteIsolation.FORBID_RMW)
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        is_basic = mode.startswith('basic')
        run_read = mode in ('full', 'basic', 'basic-read')
        run_write = mode in ('full', 'basic', 'basic-write')
        run_mixed = mode in ('full', 'basic', 'basic-mixed')
        run_read_throughput = mode in ('full', 'basic', 'basic-throoughput', 'basic-throughput-read')
        run_write_throughput = mode in ('full', 'basic', 'basic-throoughput', 'basic-throughput-write')
        loaders = self.params.get('n_loaders')
=======
        is_basic = mode.startswith("basic")
        run_read = mode in ("full", "basic", "basic-read")
        run_write = mode in ("full", "basic", "basic-write")
        run_mixed = mode in ("full", "basic", "basic-mixed")
        run_read_throughput = mode in ("full", "basic", "basic-throoughput", "basic-throughput-read")
        run_write_throughput = mode in ("full", "basic", "basic-throoughput", "basic-throughput-write")
        loaders = self.params.get("n_loaders")
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

<<<<<<< HEAD
        # run a write workload as a preparation
        self.preload_data()
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        def run_read_cql():
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text=total_target_ops_per_sec_text, test_name=self.id() + '_read', sub_type='cql', stress_cmd=base_cmd_r + cmd_add_params, stress_num=1,
                keyspace_num=1, is_alternator=False, row_name='cql')
=======
        def run_read_cql():
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text=total_target_ops_per_sec_text,
                test_name=self.id() + "_read",
                sub_type="cql",
                stress_cmd=base_cmd_r + cmd_add_params,
                stress_num=1,
                keyspace_num=1,
                is_alternator=False,
                row_name="cql",
            )
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

<<<<<<< HEAD
        self._workload(sub_type='cql', stress_cmd=base_cmd_m,
                       stress_num=stress_multiplier, keyspace_num=1, is_alternator=False)
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        def run_read_alternator_no_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text=total_target_ops_per_sec_text, test_name=self.id() + '_read', sub_type='without-lwt', stress_cmd=base_cmd_r + cmd_add_params, stress_num=1,
                keyspace_num=1, row_name='alternator-no-lwt')
=======
        def run_read_alternator_no_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text=total_target_ops_per_sec_text,
                test_name=self.id() + "_read",
                sub_type="without-lwt",
                stress_cmd=base_cmd_r + cmd_add_params,
                stress_num=1,
                keyspace_num=1,
                row_name="alternator-no-lwt",
            )
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

<<<<<<< HEAD
        # run a mixed workload
        # run a workload without lwt as baseline
        self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
        self._workload(sub_type='without-lwt', stress_cmd=base_cmd_m, stress_num=stress_multiplier, keyspace_num=1)
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        def run_write_cql():
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text=total_target_ops_per_sec_text, test_name=self.id() + '_write', sub_type='cql', stress_cmd=base_cmd_w + cmd_add_params,
                stress_num=1, keyspace_num=1, is_alternator=False, row_name='cql')
=======
        def run_write_cql():
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text=total_target_ops_per_sec_text,
                test_name=self.id() + "_write",
                sub_type="cql",
                stress_cmd=base_cmd_w + cmd_add_params,
                stress_num=1,
                keyspace_num=1,
                is_alternator=False,
                row_name="cql",
            )
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

<<<<<<< HEAD
        # run a workload with lwt
        self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
        self._workload(sub_type='with-lwt', stress_cmd=base_cmd_m, stress_num=stress_multiplier, keyspace_num=1)
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        def run_write_alternator_no_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text=total_target_ops_per_sec_text, test_name=self.id() + '_write', sub_type='without-lwt', stress_cmd=base_cmd_w + cmd_add_params,
                stress_num=1, keyspace_num=1, row_name='alternator-no-lwt')
=======
        def run_write_alternator_no_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text=total_target_ops_per_sec_text,
                test_name=self.id() + "_write",
                sub_type="without-lwt",
                stress_cmd=base_cmd_w + cmd_add_params,
                stress_num=1,
                keyspace_num=1,
                row_name="alternator-no-lwt",
            )
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

<<<<<<< HEAD
        self.check_regression_with_baseline('cql')
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        def run_write_alternator_with_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text=total_target_always_lwt_ops_per_sec_text, test_name=self.id() + '_write', sub_type='with-lwt', stress_cmd=base_cmd_w + cmd_add_write_always_lwt_params,
                stress_num=1, keyspace_num=1, row_name='alternator-always-lwt')
=======
        def run_write_alternator_with_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text=total_target_always_lwt_ops_per_sec_text,
                test_name=self.id() + "_write",
                sub_type="with-lwt",
                stress_cmd=base_cmd_w + cmd_add_write_always_lwt_params,
                stress_num=1,
                keyspace_num=1,
                row_name="alternator-always-lwt",
            )
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

<<<<<<< HEAD
    def test_latency(self):
        """
        Test steps:
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        def run_mixed_cql():
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text=total_target_ops_per_sec_text, test_name=self.id() + '_mixed', sub_type='cql', stress_cmd=base_cmd_m + cmd_add_params,
                stress_num=1, keyspace_num=1, is_alternator=False, row_name='cql')
=======
        def run_mixed_cql():
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text=total_target_ops_per_sec_text,
                test_name=self.id() + "_mixed",
                sub_type="cql",
                stress_cmd=base_cmd_m + cmd_add_params,
                stress_num=1,
                keyspace_num=1,
                is_alternator=False,
                row_name="cql",
            )
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

<<<<<<< HEAD
        1. Prepare cluster with data (reach steady_stet of compactions and ~x10 capacity than RAM.
        with round_robin and list of stress_cmd - the data will load several times faster.
        2. Run READ workload with cql.
        3. Run READ workload without lwt.
        4. Run READ workload with lwt.
        5. Run WRITE workload with cql.
        6. Run WRITE workload without lwt.
        7. Run WRITE workload with lwt.
        8. Run MIXED workload with cql.
        9. Run MIXED workload without lwt.
        10. Run MIXED workload with lwt.
        """
        node = self.db_cluster.nodes[0]
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        def run_mixed_alternator_no_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(target_ops_text=total_target_ops_per_sec_text, test_name=self.id() + '_mixed', sub_type='without-lwt',
                                                                                 stress_cmd=base_cmd_m + cmd_add_params, stress_num=1, keyspace_num=1, row_name='alternator-no-lwt')
=======
        def run_mixed_alternator_no_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text=total_target_ops_per_sec_text,
                test_name=self.id() + "_mixed",
                sub_type="without-lwt",
                stress_cmd=base_cmd_m + cmd_add_params,
                stress_num=1,
                keyspace_num=1,
                row_name="alternator-no-lwt",
            )
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

<<<<<<< HEAD
        self.create_alternator_table(schema=self.params.get("dynamodb_primarykey_type"),
                                     alternator_write_isolation=alternator.enums.WriteIsolation.FORBID_RMW)
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        def run_mixed_alternator_with_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(target_ops_text=total_target_always_lwt_ops_per_sec_text, test_name=self.id() + '_mixed', sub_type='with-lwt',
                                                                                 stress_cmd=base_cmd_m + cmd_add_write_always_lwt_params, stress_num=1, keyspace_num=1, row_name='alternator-always-lwt')

        def run_read_throughput_cql():
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text="unthrottled", test_name=self.id() + '_throughput_read', sub_type='cql', stress_cmd=base_cmd_r + cmd_add_throughput_params,
                stress_num=1, keyspace_num=1, is_alternator=False, row_name='cql')

        def run_read_throughput_alternator_no_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(target_ops_text="unthrottled", test_name=self.id() + '_throughput_read', sub_type='without-lwt',
                                                                                 stress_cmd=base_cmd_r + cmd_add_throughput_params, stress_num=1, keyspace_num=1, row_name='alternator-no-lwt')

        def run_write_throughput_cql():
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text="unthrottled", test_name=self.id() + '_throughput_write', sub_type='cql', stress_cmd=base_cmd_w + cmd_add_throughput_params,
                stress_num=1, keyspace_num=1, is_alternator=False, row_name='cql')

        def run_write_throughput_alternator_no_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(target_ops_text="unthrottled", test_name=self.id() + '_throughput_write', sub_type='without-lwt',
                                                                                 stress_cmd=base_cmd_w + cmd_add_throughput_params, stress_num=1, keyspace_num=1, row_name='alternator-no-lwt')

        tests_to_run = tuple(test for test, condition in (
            (run_read_cql, not is_basic or run_read),
            (run_read_alternator_no_lwt, run_read),
            (run_write_cql, not is_basic or run_write),
            (run_write_alternator_no_lwt, run_write),
            (run_write_alternator_with_lwt, run_write),
            (run_mixed_cql, not is_basic or run_mixed),
            (run_mixed_alternator_no_lwt, run_mixed),
            (run_mixed_alternator_with_lwt, run_mixed),
            (run_read_throughput_cql, not is_basic or run_read_throughput),
            (run_read_throughput_alternator_no_lwt, run_read_throughput),
            (run_write_throughput_cql, not is_basic or run_write_throughput),
            (run_write_throughput_alternator_no_lwt, run_write_throughput),
        ) if condition)

        single_test_duration_in_seconds = int(60 * self.params.get('stress_duration') / len(tests_to_run))
        target_ops_per_sec_for_unlimited_scenario = 999999
        try:
            rate = self.params.get('alternator_stress_rate')
            total_target_ops_per_sec_text = str(rate)
            target_ops_per_sec = int(rate / loaders)
            self.log.info(f"Using target {target_ops_per_sec} ops/s per node ({loaders} nodes) for stress tests.")
        except KeyError:
            total_target_ops_per_sec_text = '15000'
            target_ops_per_sec = int(total_target_ops_per_sec_text) // loaders
            self.log.info(
                f"Parameter alternator_stress_rate not found, using default target {target_ops_per_sec} ops/s per node ({loaders} nodes) for stress tests.")
        try:
            rate = self.params.get('alternator_write_always_lwt_stress_rate')
            total_target_always_lwt_ops_per_sec_text = str(rate)
            target_write_always_lwt_ops_per_sec = int(rate / loaders)
            self.log.info(
                f"Using target {target_write_always_lwt_ops_per_sec} ops/s per node ({loaders} nodes) for stress tests.")
        except KeyError:
            total_target_always_lwt_ops_per_sec_text = '15000'
            target_write_always_lwt_ops_per_sec = int(total_target_always_lwt_ops_per_sec_text) // loaders
            self.log.info(
                f"Parameter alternator_write_always_lwt_stress_rate not found, using default target {target_write_always_lwt_ops_per_sec} ops/s per node ({loaders} nodes) for stress tests.")
        if single_test_duration_in_seconds < 60:
            self.log.warning(
                f"Increasing single stress duration to 1 minute - after calculating got {self.params.get('stress_duration')}s. Total runtime will be around {len(tests_to_run)} minutes.")
            single_test_duration_in_seconds = 60
        cmd_add_params = f" -target {target_ops_per_sec} -p maxexecutiontime={single_test_duration_in_seconds}"
        cmd_add_write_always_lwt_params = f" -target {target_write_always_lwt_ops_per_sec} -p maxexecutiontime={single_test_duration_in_seconds}"
        cmd_add_throughput_params = f" -target {target_ops_per_sec_for_unlimited_scenario} -p maxexecutiontime={single_test_duration_in_seconds}"

        self.pre_create_alternator_tables()
=======
        def run_mixed_alternator_with_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text=total_target_always_lwt_ops_per_sec_text,
                test_name=self.id() + "_mixed",
                sub_type="with-lwt",
                stress_cmd=base_cmd_m + cmd_add_write_always_lwt_params,
                stress_num=1,
                keyspace_num=1,
                row_name="alternator-always-lwt",
            )

        def run_read_throughput_cql():
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text="unthrottled",
                test_name=self.id() + "_throughput_read",
                sub_type="cql",
                stress_cmd=base_cmd_r + cmd_add_throughput_params,
                stress_num=1,
                keyspace_num=1,
                is_alternator=False,
                row_name="cql",
            )

        def run_read_throughput_alternator_no_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text="unthrottled",
                test_name=self.id() + "_throughput_read",
                sub_type="without-lwt",
                stress_cmd=base_cmd_r + cmd_add_throughput_params,
                stress_num=1,
                keyspace_num=1,
                row_name="alternator-no-lwt",
            )

        def run_write_throughput_cql():
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text="unthrottled",
                test_name=self.id() + "_throughput_write",
                sub_type="cql",
                stress_cmd=base_cmd_w + cmd_add_throughput_params,
                stress_num=1,
                keyspace_num=1,
                is_alternator=False,
                row_name="cql",
            )

        def run_write_throughput_alternator_no_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                target_ops_text="unthrottled",
                test_name=self.id() + "_throughput_write",
                sub_type="without-lwt",
                stress_cmd=base_cmd_w + cmd_add_throughput_params,
                stress_num=1,
                keyspace_num=1,
                row_name="alternator-no-lwt",
            )

        tests_to_run = tuple(
            test
            for test, condition in (
                (run_read_cql, not is_basic or run_read),
                (run_read_alternator_no_lwt, run_read),
                (run_write_cql, not is_basic or run_write),
                (run_write_alternator_no_lwt, run_write),
                (run_write_alternator_with_lwt, run_write),
                (run_mixed_cql, not is_basic or run_mixed),
                (run_mixed_alternator_no_lwt, run_mixed),
                (run_mixed_alternator_with_lwt, run_mixed),
                (run_read_throughput_cql, not is_basic or run_read_throughput),
                (run_read_throughput_alternator_no_lwt, run_read_throughput),
                (run_write_throughput_cql, not is_basic or run_write_throughput),
                (run_write_throughput_alternator_no_lwt, run_write_throughput),
            )
            if condition
        )

        single_test_duration_in_seconds = int(60 * self.params.get("stress_duration") / len(tests_to_run))
        target_ops_per_sec_for_unlimited_scenario = 999999
        try:
            rate = self.params.get("alternator_stress_rate")
            total_target_ops_per_sec_text = str(rate)
            target_ops_per_sec = int(rate / loaders)
            self.log.info(f"Using target {target_ops_per_sec} ops/s per node ({loaders} nodes) for stress tests.")
        except KeyError:
            total_target_ops_per_sec_text = "15000"
            target_ops_per_sec = int(total_target_ops_per_sec_text) // loaders
            self.log.info(
                f"Parameter alternator_stress_rate not found, using default target {target_ops_per_sec} ops/s per node ({loaders} nodes) for stress tests."
            )
        try:
            rate = self.params.get("alternator_write_always_lwt_stress_rate")
            total_target_always_lwt_ops_per_sec_text = str(rate)
            target_write_always_lwt_ops_per_sec = int(rate / loaders)
            self.log.info(
                f"Using target {target_write_always_lwt_ops_per_sec} ops/s per node ({loaders} nodes) for stress tests."
            )
        except KeyError:
            total_target_always_lwt_ops_per_sec_text = "15000"
            target_write_always_lwt_ops_per_sec = int(total_target_always_lwt_ops_per_sec_text) // loaders
            self.log.info(
                f"Parameter alternator_write_always_lwt_stress_rate not found, using default target {target_write_always_lwt_ops_per_sec} ops/s per node ({loaders} nodes) for stress tests."
            )
        if single_test_duration_in_seconds < 60:
            self.log.warning(
                f"Increasing single stress duration to 1 minute - after calculating got {self.params.get('stress_duration')}s. Total runtime will be around {len(tests_to_run)} minutes."
            )
            single_test_duration_in_seconds = 60
        cmd_add_params = f" -target {target_ops_per_sec} -p maxexecutiontime={single_test_duration_in_seconds}"
        cmd_add_write_always_lwt_params = (
            f" -target {target_write_always_lwt_ops_per_sec} -p maxexecutiontime={single_test_duration_in_seconds}"
        )
        cmd_add_throughput_params = f" -target {target_ops_per_sec_for_unlimited_scenario} -p maxexecutiontime={single_test_duration_in_seconds}"

        self.pre_create_alternator_tables()
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)
        self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)

        self.create_cql_ks_and_table(field_number=10)
        self.run_fstrim_on_all_db_nodes()
        self.preload_data()

        base_cmd_w = self.params.get('stress_cmd_w')
        base_cmd_r = self.params.get('stress_cmd_r')
        base_cmd_m = self.params.get('stress_cmd_m')

        stress_multiplier = 2
        self.wait_no_compactions_running(n=120)

        self.run_fstrim_on_all_db_nodes()
        self._workload(
            test_name=self.id() + '_read', sub_type='cql', stress_cmd=base_cmd_r, stress_num=stress_multiplier,
            keyspace_num=1, is_alternator=False)

        # run a workload without lwt as baseline
        self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
        self._workload(
            test_name=self.id() + '_read', sub_type='without-lwt', stress_cmd=base_cmd_r, stress_num=stress_multiplier,
            keyspace_num=1)

        self.wait_no_compactions_running()
        # run a workload with lwt
        self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
        self._workload(
            test_name=self.id() + '_read', sub_type='with-lwt', stress_cmd=base_cmd_r, stress_num=stress_multiplier,
            keyspace_num=1)
        self.check_regression_with_baseline('cql')

        stress_multiplier = 1
        self.run_fstrim_on_all_db_nodes()

        self.wait_no_compactions_running()
        self._workload(
            test_name=self.id() + '_write', sub_type='cql', stress_cmd=base_cmd_w + " -target 10000",
            stress_num=stress_multiplier, keyspace_num=1, is_alternator=False)

        self.wait_no_compactions_running()
        # run a workload without lwt as baseline
        self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
        self._workload(
            test_name=self.id() + '_write', sub_type='without-lwt', stress_cmd=base_cmd_w + " -target 10000",
            stress_num=stress_multiplier, keyspace_num=1)

        self.wait_no_compactions_running(n=120)
        # run a workload with lwt
        self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
        self._workload(
            test_name=self.id() + '_write', sub_type='with-lwt', stress_cmd=base_cmd_w + " -target 3000",
            stress_num=stress_multiplier, keyspace_num=1)
        self.check_regression_with_baseline('cql')

        stress_multiplier = 1
        self.wait_no_compactions_running(n=120)
        self.run_fstrim_on_all_db_nodes()

        self._workload(
            test_name=self.id() + '_mixed', sub_type='cql', stress_cmd=base_cmd_m + " -target 10000",
            stress_num=stress_multiplier, keyspace_num=1, is_alternator=False)

        self.wait_no_compactions_running()
        # run a workload without lwt as baseline
        self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
        self._workload(test_name=self.id() + '_mixed', sub_type='without-lwt',
                       stress_cmd=base_cmd_m + " -target 10000", stress_num=stress_multiplier, keyspace_num=1)

        self.wait_no_compactions_running()
        # run a workload with lwt
        self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
        self._workload(test_name=self.id() + '_mixed', sub_type='with-lwt',
                       stress_cmd=base_cmd_m + " -target 5000", stress_num=stress_multiplier, keyspace_num=1)

        self.check_regression_with_baseline('cql')
