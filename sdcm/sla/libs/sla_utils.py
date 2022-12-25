#!/usr/bin/env python
import logging

from sdcm.sct_events import Severity
from sdcm.sct_events.workload_prioritisation import WorkloadPrioritisationEvent
from test_lib.sla import Role

LOGGER = logging.getLogger(__name__)


class SchedulerRuntimeUnexpectedValue(Exception):
    pass


class SlaUtils:
    STRESS_READ_CMD = 'cassandra-stress read cl=QUORUM duration={duration} -mode cql3 native user={user} ' \
        'password={password} -rate threads={threads} -pop {pop} -errors retries=50'
    WORKLOAD_LATENCY = 'latency'
    WORKLOAD_THROUGHPUT = 'throughput'
    CACHE_ONLY_LOAD = 'cache_only'
    DISK_ONLY_LOAD = 'disk_only'
    MIXED_LOAD = 'mixed'
    MIN_CPU_UTILIZATION = 97
    VALID_DEVIATION_PRC = 10

    @staticmethod
    # pylint: disable=too-many-arguments, too-many-locals
    def define_read_cassandra_stress_command(role: Role,
                                             load_type: str,
                                             c_s_workload_type: str,
                                             threads: int,
                                             stress_duration_min: int,
                                             max_rows_for_read: int = None,
                                             stress_command: str = STRESS_READ_CMD,
                                             throttle: int = 20000,
                                             num_of_partitions=50000000,
                                             **kwargs):
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
            # Select part of the records to warm the cache (that was read before start this stress command) and all this
            # data exists in the cache (rows from 1 to max_rows_for_read).
            # This data will be read during the test from cache
            if not max_rows_for_read:
                max_rows_for_read = int(num_of_partitions * 0.3)
            return 'seq=1..%d' % max_rows_for_read

        # Read from cache and disk
        def mixed(max_rows_for_read):  # pylint: disable=unused-variable
            # Select part of the records (rows from 1 to max_rows_for_read) for the test.
            # This amount of data will be read during the test. Part of this will be read from a cache (during run the
            # data will be loaded into a cache) and part - from a disk
            if not max_rows_for_read:
                max_rows_for_read = num_of_partitions
            return "'dist=gauss(1..%d, %d, %d)'" % (max_rows_for_read,
                                                    int(max_rows_for_read / 2),
                                                    int(max_rows_for_read * 0.05))

        def disk_only(max_rows_for_read):  # pylint: disable=unused-variable
            # Select part of the record to warm the cache (that was read before start this stress command) -
            # from 1 to max_rows_for_read - all this data will be in the cache.
            # cassandra-stress "-pop" parameter will start from more than "max_key_for_cache" row number -
            # for read from the disk
            if not max_rows_for_read:
                max_rows_for_read = int(num_of_partitions * 0.3)
            return 'seq=%d..%d' % (max_rows_for_read, max_rows_for_read + int(num_of_partitions * 0.25))

        rate = locals()[c_s_workload_type]()  # define -rate for c-s command depend on workload type
        pop = locals()[load_type](max_rows_for_read)  # define -pop for c-s command depend on load type

        params = {'n': num_of_partitions, 'user': role.name, 'password': role.password, 'pop': pop,
                  'duration': '%dm' % stress_duration_min, 'threads': rate}
        if kwargs:
            params.update(kwargs['kwargs'])
        c_s_cmd = stress_command.format(**params)
        LOGGER.info("Created cassandra-stress command: %s", c_s_cmd)

        return c_s_cmd

    @staticmethod
    def get_statistic_value_from_cassandra_stress_result(read_run, user_name, statistic_name, tester):
        """
        Get value of certain statistic reported by cassandra-stress in the log
        """
        res = tester.get_stress_results(queue=read_run, store_results=False)
        stat_rate, username = None, None
        if res:
            stat_rate = res[0].get(statistic_name)
            username = res[0].get('username')

        if not (stat_rate and username):
            LOGGER.error("Stress statistics are not received for user %s. Can't complete the test", user_name)
            return None

        return stat_rate, username

    def get_c_s_stats(self, read_queue, users, statistic_name, tester):
        role_names = [user['role'].name for user in users]

        results = {}
        for i, read in enumerate(read_queue):
            stat_rate, username = self.get_statistic_value_from_cassandra_stress_result(read_run=read,
                                                                                        user_name=role_names[i],
                                                                                        statistic_name=statistic_name,
                                                                                        tester=tester)

            if stat_rate is None:
                return stat_rate

            assert username == role_names[i], \
                f'Expected that stress was run with user "{role_names[i]}" but it was "{username}"'

            results[username] = float(stat_rate)

        return results

    # pylint: disable=too-many-arguments,too-many-locals
    def validate_scheduler_runtime(self, start_time, end_time, read_users, prometheus_stats, db_cluster,
                                   expected_ratio=None, load_high_enough=None, publish_wp_error_event=False):
        # roles_full_info example:
        #   {'role250':
        #   {'service_level': ServiceLevel: name: 'sl250',
        #                                   attributes: ServiceLevelAttributes(shares=250,
        #                                                                      timeout=None, workload_type=None),
        #   'service_level_shares': 250, 'service_level_name': "'sl250'", 'sl_group': 'sl:sl250',
        #   'sl_group_runtime': 181.23794405382156}}
        roles_full_info = {}
        for user in read_users:
            roles_full_info[user['role'].name] = user['role'].role_full_info_dict()
            roles_full_info[user['role'].name]['sl_group_runtime'] = None
            user['role'].validate_role_service_level_attributes_against_db()
        LOGGER.debug('ROLE - SERVICE LEVEL - SCHEDULER - SHARES: %s', roles_full_info)

        result = []
        sl_group_runtime_zero = False
        for node_ip in db_cluster.get_node_private_ips():
            scheduler_runtime_per_sla = prometheus_stats.get_scylla_scheduler_runtime_ms(start_time, end_time, node_ip,
                                                                                         irate_sample_sec='60s')
            # Example of scheduler_runtime_per_sla:
            #   {'10.0.2.177': {'sl:default': [410.5785714285715, 400.36428571428576],
            #   'sl:sl500_596ca81a': [177.11428571428573, 182.02857142857144]}
            LOGGER.debug('SERVICE LEVEL GROUP - RUNTIMES: {}'.format(scheduler_runtime_per_sla))
            # TODO: follow after this issue (prometheus return empty answer despite the data exists),
            #  if it is reproduced
            if not scheduler_runtime_per_sla:
                # Set this message as WARNING because I found that prometheus return empty answer despite the data
                # exists (I run this request manually and got data). Prometheus request doesn't fail, it succeeded but
                # empty, like:
                # {'status': 'success', 'data': {'resultType': 'matrix', 'result': []}}
                WorkloadPrioritisationEvent.EmptyPrometheusData(message=f'Failed to get scheduler_runtime data from '
                                                                        f'Prometheus for node {node_ip}',
                                                                severity=Severity.WARNING).publish()
                continue

            for role_sl_attribute in roles_full_info.values():
                if role_sl_attribute['sl_group'] in scheduler_runtime_per_sla[node_ip]:
                    role_sl_attribute['sl_group_runtime'] = sum(
                        scheduler_runtime_per_sla[node_ip][role_sl_attribute['sl_group']]) / \
                        len(scheduler_runtime_per_sla[node_ip][role_sl_attribute['sl_group']])
                else:
                    role_sl_attribute['sl_group_runtime'] = 0.0

                # Zero Service Level group runtime is not expected. It may happen due to Prometheus problem
                # (connection or else) or issue https://github.com/scylladb/scylla-enterprise/issues/2572
                if role_sl_attribute['sl_group_runtime'] == 0.0:
                    sl_group_runtime_zero = True

            LOGGER.debug('RUN TIME PER ROLE: {}'.format(roles_full_info))

            # We know and validate expected_ratio in the feature test. In the longevity we can not perform such kind
            # of validation because WP load runs in parallel with disruptive and non-disruptive nemeses, so we can not
            # predict the runtime ratio. We can check only that role with higher shares receives more resources (or not)
            if not expected_ratio:
                node_cpu = None
                if load_high_enough is None:
                    node_cpu = prometheus_stats.get_scylla_reactor_utilization(start_time=start_time,
                                                                               end_time=end_time,
                                                                               instance=node_ip)
                result.append(self.validate_runtime_relatively_to_share(roles_full_info=roles_full_info,
                                                                        node_ip=node_ip,
                                                                        node_cpu=node_cpu,
                                                                        load_high_enough=load_high_enough,
                                                                        publish_wp_error_event=publish_wp_error_event))
                continue

            # TODO: next 5 lines will be used by sla_per_user_system_test.py. Will need to be adapted
            # actual_shares_ratio = self.calculate_metrics_ratio_per_user(two_users_list=read_users,
            #                                                             metrics=roles_full_info)
            # self.validate_deviation(expected_ratio=expected_ratio, actual_ratio=actual_shares_ratio,
            #                         msg=f'Validate scheduler CPU runtime on the node {node_ip}. '
            #                             f'Run time per role: {roles_full_info}')

        if any(result):
            if sl_group_runtime_zero:
                result.insert(0, "\nProbably the issue https://github.com/scylladb/scylla-enterprise/issues/2572")
            raise SchedulerRuntimeUnexpectedValue("".join(result))

    @staticmethod
    def validate_runtime_relatively_to_share(roles_full_info: dict, node_ip: str,
                                             load_high_enough: bool = None, node_cpu: float = None,
                                             publish_wp_error_event=False):
        # roles_full_info example:
        #   {'role250': {'service_level': ServiceLevel: name: 'sl250',
        #   attributes: ServiceLevelAttributes(shares=250, timeout=None, workload_type=None),
        #   'service_level_shares': 250, 'service_level_name': "'sl250'", 'sl_group': 'sl:sl250',
        #   'sl_group_runtime': 181.23794405382156}}
        shares = [sl['service_level_shares'] for sl in roles_full_info.values()]

        # TODO: as Eliran about this case
        # If both roles are connected to the servie level with same shares we can not validate the ratio as we can not
        # predict the behaviour
        if shares[0] == shares[1]:
            WorkloadPrioritisationEvent.RatioValidationEvent(
                message='Both roles have same shares. Runtime can not be validated',
                severity=Severity.NORMAL).publish()
            return None

        # If shares of role1 less than shares of role2, dividing result will be less than 1 always
        try:
            shares_ratio = (shares[0] / shares[1]) < 1
        except ZeroDivisionError:
            # If shares[1] == 0 then shares_ratio can not be less than 1
            shares_ratio = False

        runtimes = [sl['sl_group_runtime'] for sl in roles_full_info.values()]
        # If runtime of role1 less than shares of role2, dividing result will be less than 1 always
        try:
            runtimes_ratio = (runtimes[0] / runtimes[1]) < 1
        except ZeroDivisionError:
            # If runtimes[1] == 0 then runtimes_ratio can not be less than 1
            runtimes_ratio = False

        # Validate that role with higher shares get more resources and vice versa
        error_message = ""
        if shares_ratio == runtimes_ratio:
            WorkloadPrioritisationEvent.RatioValidationEvent(
                message=f'Role with higher shares got more resources on the node with IP {node_ip} as expected',
                severity=Severity.NORMAL).publish()
        else:
            # If scheduler runtime per scheduler group is not as expected - return error.
            runtime_per_sl_group = []
            for service_level in roles_full_info.values():
                runtime_per_sl_group.append(f"{service_level['sl_group']} (shares "
                                            f"{service_level['service_level_shares']}): "
                                            f"{round(service_level['sl_group_runtime'], 2)}")
            runtime_per_sl_group_str = "\n  ".join(runtime_per_sl_group)

            message = (f'\n(Node {node_ip}) - Role with higher shares got less resources unexpectedly.%s '
                       f'Runtime per service level group:\n  {runtime_per_sl_group_str}')
            if load_high_enough is None and node_cpu is None:
                WorkloadPrioritisationEvent.RatioValidationEvent(
                    message=message % " Maybe due to not enough high load.",
                    severity=Severity.WARNING).publish()
            else:
                if load_high_enough is not None:
                    error_message = message % ("" if not load_high_enough else "The load is not high enough.")
                else:
                    error_message = message % f" CPU%: {round(node_cpu, 2)}."

                # If error happens during test step, TestStepEvent will publish this error. To prevent duplication do
                # not report WorkloadPrioritisationEvent
                if publish_wp_error_event:
                    WorkloadPrioritisationEvent.RatioValidationEvent(message=error_message,
                                                                     severity=Severity.ERROR).publish()

        return error_message

    @staticmethod
    def calculate_metrics_ratio_per_user(two_users_list, metrics=None):  # pylint: disable=invalid-name
        """
        :param metrics: calculate ratio for specific Scylla or cassandra-stress metrics (ops, scheduler_runtime etc..).
                        If metrics name is not defined - ration will be calculated for service_shares
        """
        if two_users_list[0]['service_level'].shares > two_users_list[1]['service_level'].shares:
            high_shares_user = two_users_list[0]
            low_shares_user = two_users_list[1]
        else:
            high_shares_user = two_users_list[1]
            low_shares_user = two_users_list[0]

        if metrics:
            high_shares_metrics = metrics[high_shares_user['role'].name]
            low_shares_metrics = metrics[low_shares_user['role'].name]
        else:
            high_shares_metrics = high_shares_user['service_level'].shares
            low_shares_metrics = low_shares_user['service_level'].shares

        if not high_shares_metrics or not low_shares_metrics:
            return None
        return float(high_shares_metrics) / float(low_shares_metrics)

    def validate_deviation(self, expected_ratio, actual_ratio, msg):
        dev = self.calculate_deviation(expected_ratio, actual_ratio)
        if dev is None:
            WorkloadPrioritisationEvent.RatioValidationEvent(
                message=f'Can\'t compare expected and actual shares ratio. Expected: {expected_ratio}. '
                        f'Actual: {actual_ratio}', severity=Severity.ERROR).publish()
        elif dev > self.VALID_DEVIATION_PRC:
            WorkloadPrioritisationEvent.RatioValidationEvent(
                message=f'{msg}. Actual ratio ({actual_ratio}) is not as expected ({expected_ratio})',
                severity=Severity.ERROR).publish()
        else:
            WorkloadPrioritisationEvent.RatioValidationEvent(
                message=f'{msg}. Actual ratio ({actual_ratio}) is as expected ({expected_ratio})',
                severity=Severity.NORMAL).publish()

    @staticmethod
    def calculate_deviation(first, second):
        if first and second:
            _first, _second = (first, second) if first > second else (second, first)
            dev = float(abs(_first - _second) * 100 / _second)
            return dev
        return None

    @staticmethod
    def clean_auth(entities_list_of_dict):
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
