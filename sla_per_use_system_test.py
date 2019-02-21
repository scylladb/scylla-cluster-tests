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

from avocado import main
from longevity_test import LongevityTest
from sdcm import wait
from sdcm.db_stats import PrometheusDBStats
import time


class SlaPerUserTest(LongevityTest):
    """
    Test SLA per user feature using cassandra-stress.

    :avocado: enable
    """

    STRESS_WRITE_CMD = 'cassandra-stress write cl=ALL n={n} -schema \'replication(factor=3)\' -port jmx=6868 ' \
                   '-mode cql3 native user={user} password={password} -rate threads={threads}'
    STRESS_WRITE_DURATION_CMD = 'cassandra-stress write cl=ALL duration={duration} -schema \'replication(factor=3)\' ' \
                       '-port jmx=6868 -mode cql3 native user={user} password={password} -rate threads={threads} ' \
                        'throttle=10000/s -pop seq={pop}'
    STRESS_READ_CMD = 'cassandra-stress read cl=ALL duration={duration} -port jmx=6868 -mode cql3 native user={user} ' \
                  'password={password} -rate threads={threads} -pop seq={seq}'
    DEFAULT_USER = 'cassandra'
    DEFAULT_USER_PASSWORD = 'cassandra'
    DEFAULT_USER_SLA = 'sla_cassandra'
    DEFAULT_CASSANDRA_SHARES = 50
    VALID_DEVIATION_PRC = 10
    WAIT_FOR_LOAD = 5 # minutes

    def __init__(self, *args, **kwargs):
        super(SlaPerUserTest, self).__init__(*args, **kwargs)
        self.prometheus_stats = None
        self.n = 11500000
        self.backgroud_task = None
        self.background_sla = [['sla_writes', 500]]
        self.background_role = ['role_writes']
        self.background_user = ['user_writes']
        self.class_users = {self.background_user[0]: self.background_sla[0][1],
                            self.DEFAULT_USER: self.DEFAULT_CASSANDRA_SHARES
                           }

    def prepare_schema(self):
        self.prometheus_stats = PrometheusDBStats(host=self.monitors.nodes[0].public_ip_address)
        session = self.cql_connection_patient(node=self.db_cluster.nodes[0], user=self.DEFAULT_USER,
                                              password=self.DEFAULT_USER_PASSWORD)

        self.create_auths(session=session, roles=self.background_role+[''],
                          users=self.background_user+[self.DEFAULT_USER],
                          slas=self.background_sla+[[self.DEFAULT_USER_SLA, self.DEFAULT_CASSANDRA_SHARES]])
        write_queue = []
        self._run_all_stress_cmds(write_queue, params={'stress_cmd': self.STRESS_WRITE_CMD
                                  .format(n=self.n, user=self.DEFAULT_USER,
                                          password=self.DEFAULT_USER_PASSWORD,
                                          threads=1000),
                                  'prefix': 'preload-', 'stats_aggregate_cmds': False})
        self.verify_stress_thread(queue=write_queue[0])
        return session

    def test_users(self, users, slas, add_class_users=True):
        test_users = {}
        if add_class_users:
            for user, shares in self.class_users.iteritems():
                test_users[user] = [shares]

        for i, user in enumerate(users):
            test_users[user] = [slas[i][1]]
        return test_users

    def scheduler_group_sla_correlation(self, test_users, shards_per_sla):
        for user, shares in test_users.iteritems():
            for sg, sg_shares in shards_per_sla.iteritems():
                if shares[0] in sg_shares:
                    test_users[user].append(sg)
                    break
        return test_users

    def test_run_all(self):
        """
        Start all tests one by one
        """
        session = self.prepare_schema()
        time.sleep(300)
        self._sla_per_user_basic(session=session)
        # Sleep between tests
        # time.sleep(300)

    def _sla_per_user_basic(self, session):
        """
        Basic test - Add SLA and grant to user (before any load)
        - SLA1 configured with "200", ratio is calculated compared to "DEFAULT"
        -User A reads using SLA 200
        -Background load with user cassandra (DEFAULT SLA = 1000)
        -Start load
        (2 SLA with ratio of 1:5 (e.g. 200:1000)
        (Load is 100%)
        """
        test_name = 'sla_per_user_basic'
        stress_duration = 30 # min
        # SLA list = [[name, shares]]
        slas = [['sla200', 200], ['sla1000', 1000]]
        roles = ['role200', 'role1000']
        users = ['user_200', 'user_1000']
        expected_shares_ratio = self.calculate_shares_ratio(slas[1][1], slas[0][1])

        test_users = self.test_users(users=users, slas=slas, add_class_users=False)

        self.log.debug('============== Start {} test'.format(test_name))
        self.create_auths(session=session, roles=roles, users=users, slas=slas)

        waited_cpu_min = 97
        rows_increase = 5000000
        # if not self.create_background_load(cpu_minimum=waited_cpu_min,
        #                                    duration='%dm' % (stress_duration+self.WAIT_FOR_LOAD),
        #                                    timeout=self.WAIT_FOR_LOAD*60):
        #     self.log.error('CPU load {}% was not reached'.format(waited_cpu_min))
        #     assert False, 'CPU load {}% was not reached'.format(waited_cpu_min)

        try:
            read_cmds = [self.STRESS_READ_CMD.format(n=self.n, user=users[0], password=users[0],
                                                     seq='1..%d' % (self.n),
                                                     duration='%dm' % stress_duration, threads=1000),
                         self.STRESS_READ_CMD.format(n=self.n, user=users[1], password=users[1],
                                                     seq='1..%d' % (self.n),
                                                     duration='%dm' % stress_duration, threads=1000)
                        ]
            write_cmd = [self.STRESS_WRITE_DURATION_CMD.format(duration='%dm' % stress_duration,
                                                              user=self.background_user[0],
                                                              password=self.background_user[0],
                                                              threads=1500,
                                                              pop='%d..%d' % (1, self.n+rows_increase))]
            start_time = time.time()
            results = self.run_stress_get_results(read_cmds=read_cmds+write_cmd,
                                                  users=users+self.background_user,
                                                  waited_cpu_min=waited_cpu_min)
            end_time = time.time()

            self.validate_scheduler_runtime(start_time=start_time, end_time=end_time, all_users=test_users,
                                            read_users=users, expected_ratio=expected_shares_ratio)

            self.n += rows_increase
            if not results:
                return

            self.log.debug('Valudate cassandra-stress ops deviation')
            actual_shares_ratio = self.calculate_shares_ratio(results[users[1]], results[users[0]])
            self.validate_deviation(expected_ratio=expected_shares_ratio,
                                    actual_ratio=actual_shares_ratio, msg='Validate cassandra-stress ops.')

            self.log.debug('============== Finish {} test'.format(test_name))
        finally:
            self.clean_auth(session=session, sla_list=[sla[0] for sla in slas], roles_list=roles, users_list=users)

    def validate_scheduler_runtime(self, start_time, end_time, all_users, read_users, expected_ratio):
        for node_ip in self.db_cluster.get_node_public_ips():
            shards_per_sla = self.prometheus_stats.get_scylla_scheduler_shares_per_sla(start_time, end_time, node_ip)
            users_for_validate = {user: details for user, details in all_users.items() if user in read_users}
            test_users_to_sg = self.scheduler_group_sla_correlation(test_users=users_for_validate,
                                                                    shards_per_sla=shards_per_sla)

            shards_time_per_sla = self.prometheus_stats.get_scylla_scheduler_runtime_ms(start_time, end_time, node_ip)
            if not (shards_time_per_sla and shards_per_sla):
                continue

            runtime_per_user = {}
            for username, val in test_users_to_sg.items():
                if val[1] in shards_time_per_sla[node_ip]:
                    runtime_per_user[username] = sum(shards_time_per_sla[node_ip][val[1]]) / \
                                                 len(shards_time_per_sla[node_ip][val[1]])
                else:
                    runtime_per_user[username] = 0
            self.log.debug('runtime_per_user: {}'.format(runtime_per_user))
            actual_shares_ratio = self.calculate_shares_ratio(runtime_per_user[read_users[1]],
                                                              runtime_per_user[read_users[0]])
            self.validate_deviation(expected_ratio=expected_ratio, actual_ratio=actual_shares_ratio,
                                    msg='Validate scheduler CPU runtime on the node %s' % node_ip)

    def create_auths(self, session, users, slas, roles=None):
        for i in xrange(len(users)):
            if roles[i]:
                self.create_role(session=session,role_name=roles[i], service_level_name=slas[i][0],
                                 service_level_shares=slas[i][1])
                self.create_user(session=session, user_name=users[i], password=users[i],
                                 roles_list_to_grant=[roles[i]])
            else:
                self.create_user(session=session, user_name=users[i], password=users[i],
                                 service_level_name=slas[i][0], service_level_shares=slas[i][1])

    def validate_deviation(self, expected_ratio, actual_ratio, msg):
        dev = self.calculate_deviation(expected_ratio, actual_ratio)
        self.assertIsNotNone(dev, 'Can\'t compare expected and actual shares ratio. Expected: '
                                  '{expected_ratio}. Actual: {actual_ratio}'
                             .format(**locals())
                             )
        # TODO: formulate error message
        self.assertTrue(dev <= self.VALID_DEVIATION_PRC, '{msg}. Actual shares ratio ({actual_ratio}) is not '
                                                         'as expected ({expected_ratio})'
                        .format(**locals())
                        )

    def calculate_deviation(self, first, second):
        if first and second:
            f, s = (first, second) if first > second else (second, first)
            dev = int(abs(f - s) * 100 / f)
            return dev
        return None

    def calculate_shares_ratio(self, first, second):
        if not first or not second:
            return None
        return float(first) / float(second)

    def run_stress_get_results(self, read_cmds, users, waited_cpu_min):
        users_all = [user for user in users]
        start = int(time.time())
        read_queue = []
        self._run_all_stress_cmds(read_queue, params={'stress_cmd': read_cmds,
                                                      'round_robin': True})

        results = {}
        for i, read in enumerate(read_queue):
            op_rate, username = self.get_op_rate_and_username(read)
            if not (op_rate and username):
                self.log.error('Stress statistics are not received for user {}. Can\'t complete the test'
                               .format(users_all[i]))
                return None
            self.assertEqual(username, users_all[i], msg='Expected that stress was run with user "{}" but it was "{}"'
                             .format(users_all[i], username))
            results[username] = int(op_rate)

        end = int(time.time())
        scylla_load = self.prometheus_stats.get_scylla_reactor_utilization(start_time=start, end_time=end)
        if scylla_load < waited_cpu_min:
            self.log.warning('Load isn\'t high enough. The test results may be not correct')

        return results

    def get_op_rate_and_username(self, read_queue):
        res = self.get_stress_results(queue=read_queue, store_results=False)
        op_rate, username = None, None
        if res:
            op_rate = res[0].get('op rate')
            username = res[0].get('username')
        return op_rate, username

    def clean_auth(self, session, sla_list, roles_list, users_list):
        for user in users_list:
            self.drop_user(session=session, user_name=user)

        for role in roles_list:
            self.drop_role(session=session, role_name=role)

        for sla in sla_list:
            self.drop_service_level(session=session, sla_name=sla)

        self.backgroud_task = None

    def create_background_load(self, duration='120m', timeout=900, cpu_minimum=99, sleep_time_sec=120,
                               rows_increase=500000):
        stress_cmd = self.STRESS_WRITE_DURATION_CMD.format(duration=duration, user=self.background_user[0],
                                                           password=self.background_user[0],
                                                           threads=1500, pop='%d..%d' % (self.n, self.n+rows_increase))
        self.backgroud_task = self.run_stress_thread(stress_cmd=stress_cmd)

        return wait.wait_for(func=self.wait_for_load_reached, step=sleep_time_sec, timeout=timeout,
                             cpu_minimum=cpu_minimum, sleep_time_sec=sleep_time_sec)

    def wait_for_load_reached(self, cpu_minimum=99, sleep_time_sec=120):
        end = int(time.time())
        start = int(time.time())-sleep_time_sec
        cpu = self.prometheus_stats.get_scylla_reactor_utilization(start_time=start, end_time=end)
        if cpu and cpu > cpu_minimum:
            self.log.debug("CPU load reached {}%".format(cpu_minimum))
            return cpu > cpu_minimum
        return False

if __name__ == '__main__':
    main()
