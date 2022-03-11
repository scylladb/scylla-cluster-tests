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
import logging
import time

from longevity_test import LongevityTest
from sdcm.db_stats import PrometheusDBStats
from test_lib.sla import ServiceLevel, Role, User

logger = logging.getLogger(__name__)


# pylint: disable=too-many-public-methods
class SlaPerUserTest(LongevityTest):
    """
    Test SLA per user feature using cassandra-stress.
    """

    STRESS_WRITE_CMD = 'cassandra-stress write cl=QUORUM n={n} -schema \'replication(factor=3)\' -port jmx=6868 ' \
                       '-mode cql3 native user={user} password={password} -rate threads={threads}'
    STRESS_WRITE_DURATION_CMD = 'cassandra-stress write cl=ALL duration={duration} -schema \'replication(factor=3)\' ' \
        '-port jmx=6868 -mode cql3 native user={user} password={password} -rate threads={threads} ' \
        'throttle=10000/s -pop seq={pop}'
    STRESS_READ_CMD = 'cassandra-stress read cl=ALL duration={duration} -port jmx=6868 -mode cql3 native user={user} ' \
                      'password={password} -rate threads={threads} -pop {pop}'
    STRESS_MIXED_CMD = r"cassandra-stress mixed ratio\(write={write_ratio},read={write_ratio}\) cl=QUORUM " \
                       "duration={duration} -port jmx=6868 " \
                       "-mode cql3 native user={user} password={password} -rate threads={threads} -pop {pop} "
    DEFAULT_USER = 'cassandra'
    DEFAULT_USER_PASSWORD = 'cassandra'
    DEFAULT_USER_SLA = 'sla_cassandra'
    DEFAULT_SHARES = 1000
    VALID_DEVIATION_PRC = 10
    MIN_CPU_UTILIZATION = 97
    WORKLOAD_LATENCY = 'latency'
    WORKLOAD_THROUGHPUT = 'throughput'
    CACHE_ONLY_LOAD = 'cache_only'
    DISK_ONLY_LOAD = 'disk_only'
    MIXED_LOAD = 'mixed'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prometheus_stats = None
        self.num_of_partitions = 200000000
        self.backgroud_task = None
        self.class_users = {}
        self.connection_cql = None
        self._comparison_results = None

    def prepare_schema(self):
        self.prometheus_stats = PrometheusDBStats(host=self.monitors.nodes[0].public_ip_address)
        self.connection_cql = self.db_cluster.cql_connection_patient(
            node=self.db_cluster.nodes[0], user=self.DEFAULT_USER, password=self.DEFAULT_USER_PASSWORD)
        session = self.connection_cql.session
        return session

    def create_test_data(self, rows_amount=None):
        # Prefill data before tests
        if rows_amount is not None:
            self.num_of_partitions = rows_amount

        write_cmd = self.STRESS_WRITE_CMD.format(n=self.num_of_partitions, user=self.DEFAULT_USER,
                                                 password=self.DEFAULT_USER_PASSWORD,
                                                 threads=250)
        self.run_stress_and_verify_threads(params={'stress_cmd': write_cmd,
                                                   'prefix': 'preload-',
                                                   'stats_aggregate_cmds': False})

    @staticmethod
    def user_to_scheduler_group(test_users, scheduler_shares):
        for user, shares in test_users.items():
            for scheduler_group, sg_shares in scheduler_shares.items():
                if shares[0] in sg_shares:
                    test_users[user].append(scheduler_group)
                    break
        return test_users

    def validate_scheduler_runtime(self, start_time, end_time, read_users, expected_ratio):
        users_with_shares = {user['user'].name: [user['service_level'].service_shares] for user in read_users}
        for node_ip in self.db_cluster.get_node_private_ips():
            # Temporary solution
            scheduler_shares = self.prometheus_stats.get_scylla_scheduler_shares_per_sla(start_time, end_time, node_ip)
            self.log.debug('SCHEDULERS SHARES FROM PROMETHEUS: {}'.format(scheduler_shares))
            if 'service_level_sg_0' in scheduler_shares:
                scheduler_shares.pop('service_level_sg_0')

            test_users_to_sg = self.user_to_scheduler_group(test_users=users_with_shares,
                                                            scheduler_shares=scheduler_shares)
            self.log.debug('USER - SERVICE LEVEL - SCHEDULER: {}'.format(test_users_to_sg))
            # End Temporary solution

            shards_time_per_sla = self.prometheus_stats.get_scylla_scheduler_runtime_ms(start_time, end_time, node_ip)
            if not (shards_time_per_sla and scheduler_shares):
                continue

            runtime_per_user = {}
            for username, val in test_users_to_sg.items():
                if val[1] in shards_time_per_sla[node_ip]:
                    runtime_per_user[username] = sum(shards_time_per_sla[node_ip][val[1]]) / \
                        len(shards_time_per_sla[node_ip][val[1]])
                else:
                    runtime_per_user[username] = 0
            self.log.debug('RUN TIME PER USER: {}'.format(runtime_per_user))
            actual_shares_ratio = self.calculate_metrics_ratio_per_user(two_users_list=read_users,
                                                                        metrics=runtime_per_user)
            self.validate_deviation(expected_ratio=expected_ratio, actual_ratio=actual_shares_ratio,
                                    msg='Validate scheduler CPU runtime on the node %s' % node_ip)

    @staticmethod
    def create_auths(entities_list_of_dict):
        """
        :param entities_list_of_dict: Expected structure:
                    [{'user': User(), 'role': Role(), 'service_level': ServiceLevel()},
                    OR
                     {'user': User(), 'service_level': ServiceLevel()},
                    OR
                     {'role': Role(), 'service_level': ServiceLevel()}
                    ]
        """
        for entity in entities_list_of_dict:
            service_level = entity.get('service_level')
            role = entity.get('role')
            user = entity.get('user')
            if service_level:
                service_level.create()
            if role:
                role.create()
                role.attach_service_level(service_level=service_level)
            if user:
                user.create()
                if role:
                    role.grant_me_to(grant_to=user)
                else:
                    user.attach_service_level(service_level=service_level)

    def validate_deviation(self, expected_ratio, actual_ratio, msg):
        dev = self.calculate_deviation(expected_ratio, actual_ratio)
        self.assertIsNotNone(dev, 'Can\'t compare expected and actual shares ratio. Expected: '
                                  '{expected_ratio}. Actual: {actual_ratio}'
                             .format(expected_ratio=expected_ratio, actual_ratio=actual_ratio)
                             )
        # TODO: formulate error message
        self.assertTrue(dev <= self.VALID_DEVIATION_PRC, '{msg}. Actual shares ratio ({actual_ratio}) is not '
                                                         'as expected ({expected_ratio})'
                        .format(msg=msg, actual_ratio=actual_ratio, expected_ratio=expected_ratio)
                        )

    @staticmethod
    def calculate_deviation(first, second):
        if first and second:
            _first, _second = (first, second) if first > second else (second, first)
            dev = float(abs(_first - _second) * 100 / _second)
            return dev
        return None

    @staticmethod
    def calculate_metrics_ratio_per_user(two_users_list, metrics=None):  # pylint: disable=invalid-name
        """
        :param metrics: calculate ratio for specific Scylla or cassandra-stress metrics (ops, scheduler_runtime etc..).
                        If metrics name is not defined - ration will be calculated for service_shares
        """
        if two_users_list[0]['service_level'].service_shares > two_users_list[1]['service_level'].service_shares:
            high_shares_user = two_users_list[0]
            low_shares_user = two_users_list[1]
        else:
            high_shares_user = two_users_list[1]
            low_shares_user = two_users_list[0]

        if metrics:
            high_shares_metrics = metrics[high_shares_user['user'].name]
            low_shares_metrics = metrics[low_shares_user['user'].name]
        else:
            high_shares_metrics = high_shares_user['service_level'].service_shares
            low_shares_metrics = low_shares_user['service_level'].service_shares

        if not high_shares_metrics or not low_shares_metrics:
            return None
        return float(high_shares_metrics) / float(low_shares_metrics)

    def run_stress_and_verify_threads(self, params=None):
        read_queue = []

        self._run_all_stress_cmds(read_queue, params=params)

        for queue in read_queue:
            self.verify_stress_thread(cs_thread_pool=queue)

        return read_queue

    def get_c_s_stats(self, read_queue, users, statistic_name):
        users_names = [user['user'].name for user in users]

        results = {}
        for i, read in enumerate(read_queue):
            res = self.get_stress_results(queue=read, store_results=False)
            stat_rate, username = None, None
            if res:
                stat_rate = res[0].get(statistic_name)
                username = res[0].get('username')

            if not (stat_rate and username):
                self.log.error('Stress statistics are not received for user {}. Can\'t complete the test'
                               .format(users_names[i]))
                return None
            self.assertEqual(username, users_names[i], msg='Expected that stress was run with user "{}" but it was "{}"'
                             .format(users_names[i], username))
            results[username] = float(stat_rate)

        return results

    def validate_if_scylla_load_high_enough(self, start_time, wait_cpu_utilization):  # pylint: disable=invalid-name
        end_time = int(time.time())
        scylla_load = self.prometheus_stats.get_scylla_reactor_utilization(start_time=start_time, end_time=end_time)

        self.assertTrue(scylla_load >= wait_cpu_utilization,
                        msg='Load isn\'t high enough. The test results may be not correct')

    def clean_auth(self, entities_list_of_dict):
        for entity in entities_list_of_dict:
            service_level = entity.get('service_level')
            role = entity.get('role')
            user = entity.get('user')
            if user:
                user.drop()
            if role:
                role.drop()
            if service_level:
                service_level.drop()

        self.backgroud_task = None
        self.connection_cql.cluster.shutdown()

    def warm_up_cache_before_test(self, max_key_for_read, stress_duration):
        read_cmds = [self.STRESS_READ_CMD.format(n=self.num_of_partitions, user=self.DEFAULT_USER,
                                                 password=self.DEFAULT_USER,
                                                 pop="seq=1..%d" % max_key_for_read,
                                                 duration='%dm' % stress_duration,
                                                 threads=200)
                     ]
        self.run_stress_and_verify_threads(params={'stress_cmd': read_cmds})

    # pylint: disable=too-many-arguments, too-many-locals
    def define_read_cassandra_stress_command(self,
                                             role: Role, load_type: str,
                                             c_s_workload_type: str,
                                             threads: int, stress_duration_min: int,
                                             max_rows_for_read: int = None,
                                             stress_command: str = STRESS_READ_CMD,
                                             throttle: int = 20000, **kwargs):
        """
        :param role: Role object
        :param load_type: cache_only/disk_only/mixed
        :param c_s_workload_type: latency: with ops restriction - using throttle
                                or
                              throughput: no restriction
        """
        def latency():
            return '%d throttle=%d/s' % (threads, throttle)

        def throughput():  # pylint: disable=unused-variable
            return threads

        def cache_only(max_rows_for_read):  # pylint: disable=unused-variable
            if not max_rows_for_read:
                max_rows_for_read = int(self.num_of_partitions * 0.3)
            return 'seq=1..%d' % max_rows_for_read

        # Read from cache and disk
        def mixed(max_rows_for_read):  # pylint: disable=unused-variable
            if not max_rows_for_read:
                max_rows_for_read = self.num_of_partitions
            return "'dist=gauss(1..%d, %d, %d)'" % (max_rows_for_read,
                                                    int(max_rows_for_read / 2),
                                                    int(max_rows_for_read * 0.05))

        def disk_only(max_rows_for_read):  # pylint: disable=unused-variable
            if not max_rows_for_read:
                max_rows_for_read = int(self.num_of_partitions * 0.3)
            return 'seq=%d..%d' % (max_rows_for_read, max_rows_for_read+int(self.num_of_partitions*0.25))

        rate = locals()[c_s_workload_type]()  # define -rate for c-s command depend on workload type
        pop = locals()[load_type](max_rows_for_read)  # define -pop for c-s command depend on load type

        params = {'n': self.num_of_partitions, 'user': role.name, 'password': role.password, 'pop': pop,
                  'duration': '%dm' % stress_duration_min, 'threads': rate}
        if kwargs:
            params.update(kwargs['kwargs'])
        c_s_cmd = stress_command.format(**params)
        logger.info("Created cassandra-stress command: %s", c_s_cmd)

        return c_s_cmd

    def test_read_throughput_1to5_ratio(self):
        """
        Basic test
        - Add SLA and grant to user (before any load)
        - user190 with 190 shares
        - user950 qith 950 shares
        - Each user runs load from own loader (round robin)
        - Expect OPS ratio between two loads is 1:5 (e.g. 190:950)
        - Expect scheduler run time between two loads is 1:5 (e.g. 190:950)

        Load from both cache and disk
        """
        # In ideal expected ratio between two users is 5.0.
        # Based on reality change it to 3.5
        # https://github.com/scylladb/scylla-cluster-tests/pull/4943#issuecomment-1168507500
        # http://13.48.103.68/test/71402aa7-051b-4803-a6b4-384529680fb7/runs?additionalRuns[]=1adf34d1-15cf-4973-80ce-9de130be0b09
        self._two_users_load_througput_workload(shares=[190, 950], load=self.MIXED_LOAD, expected_shares_ratio=3.5)

    def _two_users_load_througput_workload(self, shares, load, expected_shares_ratio=None):
        session = self.prepare_schema()
        self.create_test_data()

        # Define Service Levels/Roles/Users

        read_users = []
        for share in shares:
            read_users.append({'user': User(session=session, name='user%d' % share, password='user%d' % share),
                               'role': Role(session=session, name='role%d' % share),
                               'service_level': ServiceLevel(session=session, name='sla%d' % share,
                                                             shares=share)})

        expected_shares_ratio = (expected_shares_ratio or
                                 self.calculate_metrics_ratio_per_user(two_users_list=read_users))

        # Create Service Levels/Roles/Users
        self.create_auths(entities_list_of_dict=read_users)

        stress_duration = 10  # minutes
        read_cmds = [self.define_read_cassandra_stress_command(role=read_users[0]["role"], load_type=load,
                                                               c_s_workload_type=self.WORKLOAD_THROUGHPUT, threads=250,
                                                               stress_duration_min=stress_duration),
                     self.define_read_cassandra_stress_command(role=read_users[1]["role"], load_type=load,
                                                               c_s_workload_type=self.WORKLOAD_THROUGHPUT, threads=250,
                                                               stress_duration_min=stress_duration)
                     ]

        try:
            start_time = time.time()

            read_queue = self.run_stress_and_verify_threads(params={'stress_cmd': read_cmds, 'round_robin': True})

            results = self.get_c_s_stats(read_queue=read_queue, users=read_users, statistic_name='op rate')
            self.validate_if_scylla_load_high_enough(start_time=start_time,
                                                     wait_cpu_utilization=self.MIN_CPU_UTILIZATION)
            end_time = time.time()

            self.validate_scheduler_runtime(start_time=start_time, end_time=end_time,
                                            read_users=read_users, expected_ratio=expected_shares_ratio)

            self.assertTrue(results, msg='Not received cassandra-stress results')

            self.log.debug('Validate cassandra-stress ops deviation')
            actual_shares_ratio = self.calculate_metrics_ratio_per_user(two_users_list=read_users, metrics=results)
            self.validate_deviation(expected_ratio=expected_shares_ratio,
                                    actual_ratio=actual_shares_ratio, msg='Validate cassandra-stress ops.')

        finally:
            self.clean_auth(entities_list_of_dict=read_users)

    def test_read_throughput_vs_latency_cache_and_disk(self):  # pylint: disable=invalid-name
        """
        Test when one user run load with high latency and another  - with high througput
        The load is run on the full data set (that is read from both the cache and the disk)
        Troughput - latency test:
        - Add SLA and grant to user (before any load)
        - user190 with 190 shares
        - user950 qith 950 shares
        - Each user runs load from own loader (round robin):
           - user950 runs load with throttle
           - user190 runs load with high throughput

        Expected results: latency 99th of user950 workload when it runs in parallel with workload of user190 is not
                          significant increased relatively to latency of runed alone user950 workload
        """
        stress_duration = 10  # minutes
        shares = [190, 950]
        read_users = []

        session = self.prepare_schema()
        self.create_test_data()

        # Define Service Levels/Roles/Users
        for share in shares:
            read_users.append({'user': User(session=session, name='user%d' % share, password='user%d' % share),
                               'role': Role(session=session, name='role%d' % share),
                               'service_level': ServiceLevel(session=session, name='sla%d' % share,
                                                             shares=share)})

        # Create Service Levels/Roles/Users
        self.create_auths(entities_list_of_dict=read_users)

        # Define stress commands
        read_cmds = {'troughput': self.define_read_cassandra_stress_command(role=read_users[0]["role"],
                                                                            load_type=self.MIXED_LOAD,
                                                                            c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                            threads=200,
                                                                            stress_duration_min=stress_duration),
                     'latency': self.define_read_cassandra_stress_command(role=read_users[1]["role"],
                                                                          load_type=self.MIXED_LOAD,
                                                                          c_s_workload_type=self.WORKLOAD_LATENCY,
                                                                          threads=250,
                                                                          stress_duration_min=stress_duration)
                     }

        self._throughput_latency_tests_run(read_users=read_users, read_cmds=read_cmds, latency_user=read_users[1])

    def test_read_throughput_vs_latency_cache_only(self):  # pylint: disable=invalid-name
        """
        Test when one user run load with high latency and another  - with high througput
        The load is run on the data set that fully exists in the cache
        Troughput - latency test:
        - Add SLA and grant to user (before any load)
        - user190 with 190 shares
        - user950 qith 950 shares
        - Each user runs load from own loader (round robin):
           - user950 runs load with throttle
           - user190 runs load with high throughput

        Expected results: latency 99th of user950 workload when it runs in parallel with workload of user190 is not
                          significant increased relatively to latency of runed alone user950 workload
        """
        stress_duration = 5  # minutes
        shares = [190, 950]
        # Select part of the record to warm the cache (all this data will be in the cache).
        # This amount of data will be read during the test from cache
        max_key_for_read = int(self.num_of_partitions*0.5)
        read_users = []

        session = self.prepare_schema()
        self.create_test_data()

        # Warm up the cache to guarantee the read will be from disk
        self.warm_up_cache_before_test(max_key_for_read=max_key_for_read, stress_duration=30)

        # Define Service Levels/Roles/Users
        for share in shares:
            read_users.append({'user': User(session=session, name='user%d' % share, password='user%d' % share),
                               'role': Role(session=session, name='role%d' % share),
                               'service_level': ServiceLevel(session=session, name='sla%d' % share,
                                                             shares=share)})

        # Create Service Levels/Roles/Users
        self.create_auths(entities_list_of_dict=read_users)

        read_cmds = {'troughput': self.define_read_cassandra_stress_command(role=read_users[0]["role"],
                                                                            load_type=self.CACHE_ONLY_LOAD,
                                                                            c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                            threads=200,
                                                                            stress_duration_min=stress_duration,
                                                                            max_rows_for_read=max_key_for_read),
                     'latency': self.define_read_cassandra_stress_command(role=read_users[1]["role"],
                                                                          load_type=self.CACHE_ONLY_LOAD,
                                                                          c_s_workload_type=self.WORKLOAD_LATENCY,
                                                                          threads=250,
                                                                          stress_duration_min=stress_duration,
                                                                          max_rows_for_read=max_key_for_read)
                     }

        self._throughput_latency_tests_run(read_users=read_users, read_cmds=read_cmds, latency_user=read_users[1])

    def test_read_throughput_vs_latency_disk_only(self):  # pylint: disable=invalid-name
        """
        Test when one user run load with high latency and another  - with high througput
        The load is run on the data set that fully exists in the cache
        Troughput - latency test:
        - Add SLA and grant to user (before any load)
        - user190 with 190 shares
        - user950 qith 950 shares
        - Each user runs load from own loader (round robin):
           - user950 runs load with throttle
           - user190 runs load with high throughput

        Expected results: latency 99th of user950 workload when it runs in parallel with workload of user190 is not
                          significant increased relatively to latency of runed alone user950 workload
        """
        stress_duration = 5  # minutes

        session = self.prepare_schema()
        self.create_test_data()

        for node in self.db_cluster.nodes:
            node.stop_scylla_server(verify_up=False, verify_down=True)
            node.start_scylla_server(verify_up=True, verify_down=False)

        # Select part of the record to warm the cache (all this data will be in the cache).
        # cassandra-stress "-pop" parameter will start from more then "max_key_for_cache" row number
        # (for read from the disk)
        max_key_for_cache = int(self.num_of_partitions*0.25)
        # Warm up the cache to guarantee the read will be from disk
        self.warm_up_cache_before_test(max_key_for_read=max_key_for_cache, stress_duration=30)

        # Define Service Levels/Roles/Users
        shares = [190, 950]
        read_users = []
        for share in shares:
            read_users.append({'user': User(session=session, name='user%d' % share, password='user%d' % share),
                               'role': Role(session=session, name='role%d' % share),
                               'service_level': ServiceLevel(session=session, name='sla%d' % share,
                                                             shares=share)})

        # Create Service Levels/Roles/Users
        self.create_auths(entities_list_of_dict=read_users)

        read_cmds = {'troughput': self.define_read_cassandra_stress_command(role=read_users[0]["role"],
                                                                            load_type=self.DISK_ONLY_LOAD,
                                                                            c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                            threads=200,
                                                                            stress_duration_min=stress_duration,
                                                                            max_rows_for_read=max_key_for_cache*2),
                     'latency': self.define_read_cassandra_stress_command(role=read_users[1]["role"],
                                                                          load_type=self.DISK_ONLY_LOAD,
                                                                          c_s_workload_type=self.WORKLOAD_LATENCY,
                                                                          threads=250,
                                                                          stress_duration_min=stress_duration,
                                                                          max_rows_for_read=max_key_for_cache*3),
                     'latency_only': self.define_read_cassandra_stress_command(role=read_users[1]["role"],
                                                                               load_type=self.DISK_ONLY_LOAD,
                                                                               c_s_workload_type=self.WORKLOAD_LATENCY,
                                                                               threads=250,
                                                                               stress_duration_min=stress_duration,
                                                                               max_rows_for_read=max_key_for_cache)
                     }

        self._throughput_latency_tests_run(read_users=read_users, read_cmds=read_cmds, latency_user=read_users[1])

    def test_read_50perc_write_50perc_load(self):
        """
        Test scenario:
        - Add SLA and grant to user (before any load)
        - user190 with 190 shares
        - user950 qith 950 shares
        - Each user runs load from own loader (round robin)
        - Expect OPS ratio between two loads is 1:5 (e.g. 190:950)
        - Expect scheduler run time between two loads is 1:5 (e.g. 190:950)
        """

        session = self.prepare_schema()
        self.create_test_data()

        stress_duration_min = 10

        # Define Service Levels/Roles/Users
        shares = [190, 950]
        read_users = []
        for share in shares:
            read_users.append({'user': User(session=session, name='user%d' % share, password='user%d' % share),
                               'role': Role(session=session, name='role%d' % share),
                               'service_level': ServiceLevel(session=session, name='sla%d' % share,
                                                             shares=share)})

        # Create Service Levels/Roles/Users
        self.create_auths(entities_list_of_dict=read_users)

        read_cmds = {'troughput': self.define_read_cassandra_stress_command(role=read_users[0]["role"],
                                                                            load_type=self.MIXED_LOAD,
                                                                            c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                            threads=120,
                                                                            stress_duration_min=stress_duration_min,
                                                                            stress_command=self.STRESS_MIXED_CMD,
                                                                            kwargs={'write_ratio': 1, 'read_ratio': 1}),
                     'latency': self.define_read_cassandra_stress_command(role=read_users[1]["role"],
                                                                          load_type=self.MIXED_LOAD,
                                                                          c_s_workload_type=self.WORKLOAD_LATENCY,
                                                                          threads=120,
                                                                          stress_duration_min=stress_duration_min,
                                                                          stress_command=self.STRESS_MIXED_CMD,
                                                                          kwargs={'write_ratio': 1, 'read_ratio': 1})
                     }

        self._throughput_latency_tests_run(read_users=read_users, read_cmds=read_cmds, latency_user=read_users[1])

    def test_workload_types(self):
        """
        Test scenario: run 2 workload types (batch, interactive) using
        Roles with relevant ServiceLevel objects attached to them.
        Validate that the metrics differ and that the difference is
        within the expected margins.
        """
        session = self.prepare_schema()
        self.create_test_data(rows_amount=100_000)

        stress_duration_min = 180

        # Define Service Levels/Roles/Users
        interactive_role = Role(session=session, name="interactive",
                                password="interactive", login=True, verbose=True).create()
        batch_role = Role(session=session, name="batch", password="batch", login=True, verbose=True).create()
        interactive_sla = ServiceLevel(session=session, name="interactive", shares=None,
                                       workload_type="interactive").create()
        batch_sla = ServiceLevel(session=session, name="batch", shares=None,
                                 workload_type="batch").create()
        interactive_role.attach_service_level(interactive_sla)
        batch_role.attach_service_level(batch_sla)

        read_cmds = {
            'throughput_interactive': self.define_read_cassandra_stress_command(
                role=interactive_role,
                load_type=self.MIXED_LOAD,
                c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                threads=120,
                stress_duration_min=stress_duration_min,
                stress_command=self.STRESS_MIXED_CMD,
                kwargs={'write_ratio': 1, 'read_ratio': 1}
            ),
            'throughput_batch': self.define_read_cassandra_stress_command(
                role=batch_role,
                load_type=self.MIXED_LOAD,
                c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                threads=120,
                stress_duration_min=stress_duration_min,
                stress_command=self.STRESS_MIXED_CMD,
                kwargs={'write_ratio': 1, 'read_ratio': 1}
            )}

        try:
            self.log.debug('Running interactive and batch workloads in sequence...')
            workloads_queue = self.run_stress_and_verify_threads(params={'stress_cmd': [
                read_cmds['throughput_interactive'],
                read_cmds["throughput_batch"]
            ],
                'round_robin': True})
            self._compare_workloads_c_s_metrics(workloads_queue)
        finally:
            pass

    def _throughput_latency_tests_run(self, read_cmds, read_users, latency_user):
        # pylint: disable=too-many-locals
        try:
            # Run latency workload
            test_start_time = time.time()
            self.log.debug('Start latency only workload')
            read_queue = self.run_stress_and_verify_threads(params={'stress_cmd': [read_cmds.get('latency_only')
                                                                                   or read_cmds['latency']],
                                                                    'round_robin': True})

            latency_99_for_latency_workload = self.get_c_s_stats(read_queue=read_queue, users=[latency_user],
                                                                 statistic_name='latency 99th percentile')

            self.assertTrue(latency_99_for_latency_workload, msg='Not received cassandra-stress results for latency '
                            'workload')

            # Run throughput and latency workloads
            self.log.debug('Start latency workload in parallel with throughput workload')
            read_queue = self.run_stress_and_verify_threads(params={'stress_cmd': [read_cmds['troughput'],
                                                                                   read_cmds['latency']],
                                                                    'round_robin': True})
            latency_99_for_mixed_workload = self.get_c_s_stats(read_queue=read_queue, users=read_users,
                                                               statistic_name='latency 99th percentile')

            self.assertTrue(latency_99_for_mixed_workload, msg='Not received cassandra-stress for latency workload')

            grafana_dataset = self.monitors.get_grafana_screenshot_and_snapshot(test_start_time=test_start_time)

            grafana_screenshots = grafana_dataset.get('screenshots', [])
            grafana_snapshots = grafana_dataset.get('snapshots', [])

            self.log.debug('GRAFANA SCREENSHOTS: {}'.format(grafana_screenshots))
            self.log.debug('GRAFANA SNAPSHOTS: {}'.format(grafana_snapshots))

            # Compare latency of two runs
            self.log.debug('Test results:\n---------------------\n')
            latency_99_latency_workload = latency_99_for_latency_workload[latency_user['user'].name]
            latency_99_mixed_workload = latency_99_for_mixed_workload[latency_user['user'].name]
            deviation = self.calculate_deviation(latency_99_latency_workload, latency_99_mixed_workload)
            latency_change = 'increased' if latency_99_mixed_workload > latency_99_latency_workload else 'decreased'

            result_print_str = '\nTest results:\n---------------------\n'
            result_print_str += '\nWorkload                  |      Latency 99%'
            result_print_str += '\n========================= | ================='
            result_print_str += '\nLatency only              |      {}'.format(latency_99_latency_workload)
            result_print_str += '\nLatency and throughput    |      {}'.format(latency_99_mixed_workload)
            result_print_str += '\n------------------------- | -----------------'
            result_print_str += '\nLatency 99 is {} in {}%'.format(latency_change, deviation)

            self.log.info(result_print_str)
        finally:
            self.clean_auth(entities_list_of_dict=read_users)

    def _compare_workloads_c_s_metrics(self, workloads_queue: list) -> dict:
        # define what difference margin is expected for interactive vs. batch workload metrics
        comparison_axis = {"latency 95th percentile": 1.5,
                           "latency 99th percentile": 1.5,
                           "op rate": 0.3}
        workloads_results = {}
        for workload in workloads_queue:
            result = self.get_stress_results(queue=workload, store_results=False)

            workloads_results.update({result[0].get("username"): result[0]})

        assert len(workloads_results) == 2, \
            "Expected workload_results length to be 2, got: %s. workload_results: %s" % (
                len(workloads_results), workloads_results)
        comparison_results = {}
        try:
            for item, target_margin in comparison_axis.items():
                interactive = float(workloads_results["interactive"][item])
                batch = float(workloads_results["batch"][item])
                ratio = batch / interactive

                comparison_results.update(
                    {
                        item: {
                            "interactive": interactive,
                            "batch": batch,
                            "diff": batch - interactive,
                            "within_margin": ratio <= target_margin if item == "op rate" else ratio >= target_margin
                        }
                    }
                )
            return comparison_results
        except Exception:
            logger.info("Failed to compare c-s results for batch and interactive"
                        "workloads.")
            raise

    def get_email_data(self):
        self.log.info("Prepare data for email")
        email_data = {}
        grafana_dataset = {}

        try:
            email_data = self._get_common_email_data()
        except Exception as error:  # pylint: disable=broad-except
            self.log.error("Error in gathering common email data: Error:\n%s", error)

        try:
            grafana_dataset = self.monitors.get_grafana_screenshot_and_snapshot(
                self.start_time) if self.monitors else {}
        except Exception as error:  # pylint: disable=broad-except
            self.log.error("Error in gathering Grafana screenshots and snapshots. Error:\n%s", error)

        email_data.update({"grafana_screenshots": grafana_dataset.get("screenshots", []),
                           "grafana_snapshots": grafana_dataset.get("snapshots", []),
                           "scylla_ami_id": self.params.get("ami_id_db_scylla") or "-",
                           "workload_comparison": self._comparison_results if self._comparison_results else {}
                           })

        return email_data

    # pylint: disable=inconsistent-return-statements
    def get_test_status(self) -> str:
        if self._comparison_results:
            try:
                if all((item["within_margin"] for item in self._comparison_results.values())):
                    return "SUCCESS"
                else:
                    return "FAILED"
            except KeyError as exc:
                logger.error("Exception on attempting to check workload comparison results:\n%s", exc)
                return super().get_test_status()
