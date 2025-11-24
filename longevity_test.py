#!/usr/bin/env python

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
# Copyright (c) 2016 ScyllaDB
import json
import os
import re
import string
import tempfile
import itertools
import contextlib
from typing import List, Dict

import yaml
from cassandra import AlreadyExists, InvalidRequest
from cassandra.query import SimpleStatement

from sdcm import sct_abs_path
from sdcm.sct_events.group_common_events import \
    ignore_large_collection_warning, \
    ignore_max_memory_for_unlimited_query_soft_limit
from sdcm.tester import ClusterTester
from sdcm.utils import loader_utils
from sdcm.utils.adaptive_timeouts import adaptive_timeout, Operations
from sdcm.utils.common import skip_optional_stage
from sdcm.utils.cluster_tools import group_nodes_by_dc_idx
from sdcm.utils.decorators import optional_stage
from sdcm.utils.operations_thread import ThreadParams
from sdcm.sct_events.system import InfoEvent, TestFrameworkEvent
from sdcm.sct_events import Severity
from sdcm.cluster import MAX_TIME_WAIT_FOR_NEW_NODE_UP


class LongevityTest(ClusterTester, loader_utils.LoaderUtilsMixin):
    """
    Test a Scylla cluster stability over a time period.
    """

    def setUp(self):
        super().setUp()

        # This ignores large_data warning messages "Writing large collection" for large collections to prevent
        # creating SCT Events from these warnings.
        # During large collections test thousands of warnings are being created.
        self.validate_large_collections = self.params.get('validate_large_collections')
        if self.validate_large_collections:
            self.stack = contextlib.ExitStack()
            self.stack.enter_context(ignore_large_collection_warning())
            self.stack.enter_context(ignore_max_memory_for_unlimited_query_soft_limit())

    default_params = {'timeout': 650000}

    def _get_tombstone_gc_verification_params(self) -> dict:
        params = {}
        if tombstone_params := self.params.get('run_tombstone_gc_verification'):
            params = json.loads(tombstone_params)
            self.log.info('Tombstone GC verification params are: %s', params)
        return params

    def _get_scan_operation_params(self) -> list[ThreadParams]:
        params = self.params.get("run_fullscan")
        self.log.info('Scan operation params are: %s', params)

        sla_role_name, sla_role_password = None, None
        if fullscan_role := getattr(self, "fullscan_role", None):
            sla_role_name = fullscan_role.name
            sla_role_password = fullscan_role.password
        scan_operation_params_list = []
        for item in params:
            item_dict = json.loads(item)
            scan_operation_params_list.append(ThreadParams(
                db_cluster=self.db_cluster,
                termination_event=self.db_cluster.nemesis_termination_event,
                user=sla_role_name,
                user_password=sla_role_password,
                duration=self.get_duration(item_dict.get("duration")),
                **item_dict
            ))

            self.log.info('Scan operation scan_operation_params_list are: %s', scan_operation_params_list)
        return scan_operation_params_list

    @optional_stage('prepare_write')
    def run_pre_create_schema(self):
        pre_create_schema = self.params.get('pre_create_schema')
        keyspace_num = self.params.get('keyspace_num')
        if pre_create_schema:
            self._pre_create_schema(keyspace_num, scylla_encryption_options=self.params.get(
                'scylla_encryption_options'))

    @optional_stage('prepare_write')
    def run_pre_create_keyspace(self):
        if self.params.get('pre_create_keyspace'):
            self._pre_create_keyspace()

    @optional_stage('main_load')
    def _run_validate_large_collections_in_system(self, node, table='table_with_large_collection'):
        self.log.info("Verifying large collections in system tables on node: {}".format(node))
        with self.db_cluster.cql_connection_exclusive(node=node) as session:
            query = "SELECT * from system.large_cells WHERE keyspace_name='large_collection_test'" \
                    f" AND table_name='{table}' ALLOW FILTERING"
            statement = SimpleStatement(query, fetch_size=10)
            data = list(session.execute(statement))
            if not data:
                InfoEvent("Did not find expected row in system.large_cells", severity=Severity.ERROR)

    @optional_stage('main_load')
    def _run_validate_large_collections_warning_in_logs(self, node):
        self.log.info("Verifying warning for large collections in logs on node: {}".format(node))
        msg = "Writing large collection"
        res = list(node.follow_system_log(patterns=[msg], start_from_beginning=True))
        if not res:
            InfoEvent("Did not find expected log message warning: {}".format(msg), severity=Severity.ERROR)

    def test_custom_time(self):  # noqa: PLR0914
        """
        Run cassandra-stress with params defined in data_dir/scylla.yaml
        """

        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(),
                                    tester_obj=self)

        self.download_artifacts_from_s3()

        stress_queue = []

        # prepare write workload
        prepare_write_cmd = self.params.get('prepare_write_cmd')
        keyspace_num = self.params.get('keyspace_num')

        self.pre_create_alternator_tables()

        self.run_pre_create_keyspace()
        self.run_pre_create_schema()

        self.kafka_configure()

        if scan_operation_params := self._get_scan_operation_params():
            for scan_param in scan_operation_params:
                self.log.info("Starting fullscan operation thread with the following params: %s", scan_param)
                self.run_fullscan_thread(fullscan_params=scan_param,
                                         thread_name=str(scan_operation_params.index(scan_param)))

        if tombstone_gc_verification_params := self._get_tombstone_gc_verification_params():
            self.run_tombstone_gc_verification_thread(**tombstone_gc_verification_params)

        self.run_prepare_write_cmd()

        # Grow cluster to target size if requested
        if cluster_target_size := self.params.get('cluster_target_size'):
            def is_target_reached(current: list[int], target: list[int]) -> bool:
                return all([x >= y for x, y in zip(current, target)])

            cluster_target_size = list(map(int, cluster_target_size.split())) if isinstance(
                cluster_target_size, str) else [cluster_target_size]
            add_node_cnt = self.params.get('add_node_cnt')
            nodes_by_dcx = group_nodes_by_dc_idx(self.db_cluster.data_nodes)
            current_cluster_size = [len(nodes_by_dcx[dcx]) for dcx in sorted(nodes_by_dcx)]

            InfoEvent(
                message=f"Starting to grow cluster from {self.params.get('n_db_nodes')} to {cluster_target_size}").publish()

            while not is_target_reached(current_cluster_size, cluster_target_size):
                added_nodes = []
                for dcx, target in enumerate(cluster_target_size):
                    if current_cluster_size[dcx] < target:
                        add_nodes_num = add_node_cnt if (
                            target - current_cluster_size[dcx]) >= add_node_cnt else target - current_cluster_size[dcx]
                        InfoEvent(message=f"Adding next number of nodes {add_nodes_num} to dc_idx {dcx}").publish()
                        added_nodes.extend(self.db_cluster.add_nodes(
                            count=add_nodes_num, enable_auto_bootstrap=True, dc_idx=dcx, rack=None))

                self.monitors.reconfigure_scylla_monitoring()
                up_timeout = MAX_TIME_WAIT_FOR_NEW_NODE_UP
                with adaptive_timeout(Operations.NEW_NODE, node=self.db_cluster.data_nodes[0], timeout=up_timeout):
                    self.db_cluster.wait_for_init(node_list=added_nodes, timeout=up_timeout, check_node_health=False)
                self.db_cluster.wait_for_nodes_up_and_normal(nodes=added_nodes)
                # node_cnt = len(self.db_cluster.data_nodes)
                nodes_by_dcx = group_nodes_by_dc_idx(self.db_cluster.data_nodes)
                current_cluster_size = [len(nodes_by_dcx[dcx]) for dcx in sorted(nodes_by_dcx)]

            InfoEvent(message=f"Growing cluster finished, new cluster size is {current_cluster_size}").publish()

        # Collect data about partitions and their rows amount
        if self.partitions_attrs and self.partitions_attrs.validate_partitions:
            self.partitions_attrs.collect_initial_partitions_info()

        stress_cmd = self.params.get('stress_cmd')
        self.assemble_and_run_all_stress_cmd(stress_queue, stress_cmd, keyspace_num)

        customer_profiles = self.params.get('cs_user_profiles')
        if customer_profiles:
            cs_duration = self.params.get('cs_duration')
            for cs_profile in customer_profiles:
                cs_profile = sct_abs_path(cs_profile)  # noqa: PLW2901
                assert os.path.exists(cs_profile), 'File not found: {}'.format(cs_profile)
                self.log.debug('Run stress test with user profile {}, duration {}'.format(cs_profile, cs_duration))
                profile_dst = os.path.join('/tmp', os.path.basename(cs_profile))
                with open(cs_profile, encoding="utf-8") as pconf:
                    cont = pconf.readlines()
                    user_profile_table_count = self.params.get(
                        'user_profile_table_count')
                    for _ in range(user_profile_table_count):
                        for cmd in [line.lstrip('#').strip() for line in cont if line.find('cassandra-stress') > 0]:
                            stress_cmd = (cmd.format(profile_dst, cs_duration))
                            params = {'stress_cmd': stress_cmd, 'profile': cs_profile}
                            self.log.debug('Stress cmd: {}'.format(stress_cmd))
                            if not skip_optional_stage('main_load'):
                                self._run_all_stress_cmds(stress_queue, params)

        # Check if we shall wait for total_used_space or if nemesis wasn't started
        if not prepare_write_cmd or not self.params.get('nemesis_during_prepare'):
            self.db_cluster.wait_total_space_used_per_node(keyspace=None)
            self.db_cluster.start_nemesis()

        stress_read_cmd = self.params.get('stress_read_cmd')
        if stress_read_cmd and not skip_optional_stage('main_load'):
            params = {'keyspace_num': keyspace_num, 'stress_cmd': stress_read_cmd}
            self._run_all_stress_cmds(stress_queue, params)

        for stress in stress_queue:
            self.verify_stress_thread(stress)

        if self.partitions_attrs and self.partitions_attrs.validate_partitions:
            self.partitions_attrs.validate_rows_per_partitions(ignore_limit_rows_number=True)

        if (stress_read_cmd or stress_cmd) and self.validate_large_collections:
            with ignore_large_collection_warning():
                for node in self.db_cluster.data_nodes:
                    self._run_validate_large_collections_in_system(node)
                    self._run_validate_large_collections_warning_in_logs(node)

    def test_batch_custom_time(self):
        """
        The test runs like test_custom_time but designed for running multiple stress commands in batches.
        It take the keyspace_num and calculates the number of batches to run based on batch_size.
        For every batch, it runs the stress and verify them and only then moves to the next batch.

        Test assumes:
        - pre_create_schema (The test pre-creating the schema for all batches)
        - round_robin
        - No nemesis during prepare

        :param keyspace_num: Number of keyspaces to be batched (in future it can be enhanced with number of tables).
        :param batch_size: Number of stress commands to run together in a batch.
        """
        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(), tester_obj=self)

        total_stress = self.params.get('keyspace_num')  # In future it may be 1 keyspace but multiple tables in it.
        batch_size = self.params.get('batch_size')

        prepare_write_cmd = self.params.get('prepare_write_cmd')
        if prepare_write_cmd and not skip_optional_stage('prepare_write'):
            self._run_stress_in_batches(total_stress=total_stress, batch_size=batch_size,
                                        stress_cmd=prepare_write_cmd)

        self.db_cluster.start_nemesis()

        stress_cmd = self.params.get('stress_cmd')
        if not skip_optional_stage('main_load'):
            self._run_stress_in_batches(total_stress=batch_size, batch_size=batch_size,
                                        stress_cmd=stress_cmd)

    def test_user_batch_custom_time(self):
        """
        The test runs like test_custom_time but designed for running multiple stress commands on user profile with multiple
        tables in one keyspace

        It uses batches of `batch_size` size, until it reach `user_profile_table_count`
        For every batch, it runs the stress command in parallel and verify them and only then moves to the next batch.

        Test assumes:
        - pre_create_schema (The test pre-creating the schema for all batches)
        - round_robin
        - No nemesis during prepare

        :param user_profile_table_count: Number of tables to be batched.
        :param batch_size: Number of stress commands to run together in a batch.
        """

        cs_user_profiles = self.params.get('cs_user_profiles')
        if not cs_user_profiles:
            TestFrameworkEvent(source="test_user_batch_custom_time",
                               message="The Longevity test_user_batch_custom_time cannot run without 'cs_user_profiles' parameter. aborting.",
                               severity=Severity.ERROR).publish()
            return
        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(), tester_obj=self)

        batch_size = self.params.get('batch_size')
        user_profile_table_count = self.params.get('user_profile_table_count')
        if not self.params.get('reuse_cluster'):
            self._pre_create_templated_user_schema()

            # Start new nodes
            # we are starting this test case with only one db to make creating of the tables quicker
            # gossip with multiple node cluster make this painfully slower
            add_node_cnt = self.params.get('add_node_cnt')

            for _ in range(add_node_cnt):
                new_nodes = self.db_cluster.add_nodes(count=1, enable_auto_bootstrap=True)
                self.monitors.reconfigure_scylla_monitoring()
                self.db_cluster.wait_for_init(node_list=new_nodes)

        self.db_cluster.start_nemesis()

        stress_params_list = []

        templated_table_counter = itertools.count()

        cs_duration = self.params.get('cs_duration')
        try:
            duration = int(cs_duration.translate(str.maketrans('', '', string.ascii_letters))) if cs_duration else None
        except ValueError:
            raise ValueError(f"Invalid cs_duration format: {cs_duration}")

        for cs_profile in cs_user_profiles:
            cs_profile = sct_abs_path(cs_profile)  # noqa: PLW2901
            assert os.path.exists(cs_profile), 'File not found: {}'.format(cs_profile)
            msg = f"Run stress test with user profile {cs_profile}"
            msg += f", duration {cs_duration}" if cs_duration else ", no duration parameter"
            self.log.debug(msg)

            for _ in range(user_profile_table_count):
                stress_params_list += self.create_templated_user_stress_params(next(templated_table_counter),
                                                                               cs_profile)
        if not skip_optional_stage('main_load'):
            self.log.debug("Starting stress in batches of: %d with %d stress commands",
                           batch_size, len(stress_params_list))
            self._run_user_stress_in_batches(batch_size=batch_size,
                                             stress_params_list=stress_params_list, duration=duration)

    def _run_user_stress_in_batches(self, batch_size, stress_params_list, duration):
        """
        run user profile in batches, optionally adding stress-commands which are not with pre-created tables
        and wait for them to finish.
        The optional tables are set by add_cs_user_profiles_extra_tables parameter.

        :param batch_size: size of the batch
        :param stress_params_list: the list of all stress commands
        :return:
        """

        def chunks(_list, chunk_size):
            """Yield successive n-sized chunks from _list."""
            for i in range(0, len(_list), chunk_size):
                yield _list[i:i + chunk_size], len(_list) + i * 2

        all_batches = list(chunks(stress_params_list, batch_size))
        total_batches = len(all_batches)

        for batch_number, (batch, extra_tables_idx) in enumerate(all_batches, start=1):
            self.log.info(f"Starting batch {batch_number} out of {total_batches}")

            stress_queue = []
            batch_params = dict(duration=duration, round_robin=True, stress_cmd=[])

            if self.params.get('add_cs_user_profiles_extra_tables'):
                self.log.info("adding extra tables with stress-commands in addition to pre-created tables")
                # add few stress threads with tables that weren't pre-created
                customer_profiles = self.params.get('cs_user_profiles')
                for cs_profile in customer_profiles:
                    cs_profile = sct_abs_path(cs_profile)  # noqa: PLW2901
                    # for now we'll leave to just one fresh table, to kick schema update
                    num_of_newly_created_tables = 1
                    self._pre_create_templated_user_schema(batch_start=extra_tables_idx,
                                                           batch_end=extra_tables_idx+num_of_newly_created_tables)
                    for i in range(num_of_newly_created_tables):
                        batch += self.create_templated_user_stress_params(extra_tables_idx + i, cs_profile=cs_profile)  # noqa: PLW2901

            nodes_ips = self.all_node_ips_for_stress_command
            for params in batch:
                batch_params['stress_cmd'] += [params['stress_cmd'] + nodes_ips]

            self._run_all_stress_cmds(stress_queue, params=batch_params)
            for stress in stress_queue:
                self.verify_stress_thread(stress)

    def _run_stress_in_batches(self, total_stress, batch_size, stress_cmd):
        stress_queue = []
        pre_create_schema = self.params.get('pre_create_schema')

        if pre_create_schema:
            self._pre_create_schema(keyspace_num=total_stress,
                                    scylla_encryption_options=self.params.get('scylla_encryption_options'))

        num_of_batches = int(total_stress / batch_size)
        for batch in range(num_of_batches):
            for i in range(1 + batch * batch_size, (batch + 1) * batch_size + 1):
                keyspace_name = self._get_keyspace_name(i)
                if not isinstance(stress_cmd, list):
                    stress_cmd = [stress_cmd]
                stress_cmds_with_all_ips = [cmd + self.all_node_ips_for_stress_command for cmd in stress_cmd]
                self._run_all_stress_cmds(stress_queue, params={'stress_cmd': stress_cmds_with_all_ips,
                                                                'keyspace_name': keyspace_name, 'round_robin': True})
            for stress in stress_queue:
                self.verify_stress_thread(stress)

    @property
    def all_node_ips_for_stress_command(self):
        return f' -node {",".join([n.cql_address for n in self.db_cluster.data_nodes])}'

    @staticmethod
    def _get_columns_num_of_single_stress(single_stress_cmd):
        if '-col' not in single_stress_cmd:
            return None
        col_num = None
        params_list = single_stress_cmd.split()
        col_params_list = []
        for param in params_list[params_list.index('-col')+1:]:
            col_params_list.append(param.strip("'"))
            if param.endswith("'"):
                break
        for param in col_params_list:
            if param.startswith('n='):
                col_num = int(re.findall(r'\b\d+\b', param)[0])
                break
        return col_num

    def _get_prepare_write_cmd_columns_num(self):
        prepare_write_cmd = self.params.get('prepare_write_cmd')
        if not prepare_write_cmd:
            return None
        if isinstance(prepare_write_cmd, str):
            prepare_write_cmd = [prepare_write_cmd]
        return max([self._get_columns_num_of_single_stress(single_stress_cmd=stress) for stress in prepare_write_cmd])

    def _pre_create_schema(self, keyspace_num=1, scylla_encryption_options=None):
        """
        For cases we are testing many keyspaces and tables, It's a possibility that we will do it better and faster than
        cassandra-stress.
        """

        self.log.debug('Pre Creating Schema for c-s with {} keyspaces'.format(keyspace_num))
        compaction_strategy = self.params.get('compaction_strategy')
        sstable_size = self.params.get('sstable_size')
        for i in range(1, keyspace_num+1):
            keyspace_name = 'keyspace{}'.format(i)
            self.create_keyspace(keyspace_name=keyspace_name, replication_factor=3)
            self.log.debug('{} Created'.format(keyspace_name))
            col_num = self._get_prepare_write_cmd_columns_num() or 5
            columns = {}
            for col_idx in range(col_num):
                cs_key = '"C'+str(col_idx)+'"'
                columns[cs_key] = 'blob'
            self.create_table(name='standard1', keyspace_name=keyspace_name, key_type='blob', read_repair=0.0,
                              columns=columns,
                              scylla_encryption_options=scylla_encryption_options,
                              compaction=compaction_strategy, sstable_size=sstable_size)

    def _pre_create_templated_user_schema(self, batch_start=None, batch_end=None):
        user_profile_table_count = self.params.get(
            'user_profile_table_count') or 0
        cs_user_profiles = self.params.get('cs_user_profiles')
        # read user-profile
        for profile_file in cs_user_profiles:
            with open(sct_abs_path(profile_file), encoding="utf-8") as fobj:
                profile_yaml = yaml.safe_load(fobj)
            keyspace_definition = profile_yaml['keyspace_definition']
            keyspace_name = profile_yaml['keyspace']
            table_template = string.Template(profile_yaml['table_definition'])

            with self.db_cluster.cql_connection_patient(node=self.db_cluster.nodes[0]) as session:
                # since we are using connection while nemesis is running (and we have more then 5000 tables in this
                # use case), we need a bigger timeout here to keep the following CQL commands from failing
                session.default_timeout = 60.0 * 5
                try:
                    session.execute(keyspace_definition)
                except AlreadyExists:
                    self.log.debug("keyspace [{}] exists".format(keyspace_name))

                if batch_start is not None and batch_end is not None:
                    table_range = range(batch_start, batch_end)
                else:
                    table_range = range(user_profile_table_count)
                self.log.debug('Pre Creating Schema for c-s with {} user tables'.format(user_profile_table_count))
                for i in table_range:
                    table_name = 'table{}'.format(i)
                    query = table_template.substitute(table_name=table_name)
                    try:
                        session.execute(query)
                    except AlreadyExists:
                        self.log.debug('table [{}] exists'.format(table_name))
                    self.log.debug('{} Created'.format(table_name))

                    for definition in profile_yaml.get('extra_definitions', []):
                        query = string.Template(definition).substitute(table_name=table_name)
                        try:
                            session.execute(query)
                        except (AlreadyExists, InvalidRequest) as exc:
                            self.log.debug('extra definition for [{}] exists [{}]'.format(table_name, str(exc)))

    def _flush_all_nodes(self):
        """
        This function will connect all db nodes in the cluster and run "nodetool flush" command.
        :return:
        """
        for node in self.db_cluster.nodes:
            node.run_nodetool("flush")

    def get_email_data(self):
        self.log.info("Prepare data for email")

        email_data = self._get_common_email_data()

        benchmarks_results = self.db_cluster.get_node_benchmarks_results() if self.db_cluster else {}
        # If cluster was not created, not need to collect nemesis stats - they do not exist
        nemeses_stats = self.get_nemesises_stats() if self.db_cluster else {}

        email_data.update({
            "node_benchmarks": benchmarks_results,
            "nemesis_details": nemeses_stats,
            "nemesis_name": self.params.get("nemesis_class_name"),
            "scylla_ami_id": self.params.get("ami_id_db_scylla") or "-",
        })
        return email_data

    def create_templated_user_stress_params(self, idx, cs_profile) -> List[Dict]:
        params_list = []
        cs_duration = self.params.get('cs_duration')

        with open(cs_profile, encoding="utf-8") as pconf:
            cont = pconf.readlines()
            pconf.seek(0)
            template = string.Template(pconf.read())
            prefix, suffix = os.path.splitext(os.path.basename(cs_profile))
            table_name = "table%s" % idx

            with tempfile.NamedTemporaryFile(mode='w+', prefix=prefix, suffix=suffix, delete=False, encoding='utf-8') as file_obj:
                output = template.substitute(table_name=table_name)
                file_obj.write(output)
                profile_dst = file_obj.name

            # collect stress command from the comment in the end of the profile yaml
            # example:
            # cassandra-stress user profile={} cl=QUORUM 'ops(insert=1)' duration={} -rate threads=100 -pop 'dist=gauss(0..1M)'
            for cmd in [line.lstrip('#').strip() for line in cont if line.find('cassandra-stress') > 0]:
                # Use a conditional tuple to handle optional formatting arguments for c-s duration parameter.
                # For example, without a duration: cassandra-stress user profile={} 'ops(insert=1)' cl=QUORUM n=2572262 -rate threads=1
                # Or with a duration: cassandra-stress user profile={} 'ops(insert=1)' cl=QUORUM duration={} -rate threads=1
                args = (profile_dst, cs_duration) if cs_duration else (profile_dst,)
                stress_cmd = cmd.format(*args)
                params = {'stress_cmd': stress_cmd, 'profile': profile_dst}
                self.log.debug('Stress cmd: {}'.format(stress_cmd))
                params_list.append(params)

        return params_list
