#!/usr/bin/env python
import logging

from sdcm.sct_events import Severity
from sdcm.sct_events.workload_prioritisation import WorkloadPrioritisationEvent
from sdcm.utils.adaptive_timeouts import NodeLoadInfoServices
from sdcm.utils.decorators import retrying
from test_lib.sla import Role

LOGGER = logging.getLogger(__name__)


class SchedulerRuntimeUnexpectedValue(Exception):
    pass


class SchedulerGroupNotFound(Exception):
    pass


class WrongServiceLevelShares(Exception):
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

        def throughput():
            return threads

        def cache_only(max_rows_for_read):
            # Select part of the records to warm the cache (that was read before start this stress command) and all this
            # data exists in the cache (rows from 1 to max_rows_for_read).
            # This data will be read during the test from cache
            if not max_rows_for_read:
                max_rows_for_read = int(num_of_partitions * 0.3)
            return 'seq=1..%d' % max_rows_for_read

        # Read from cache and disk
        def mixed(max_rows_for_read):
            # Select part of the records (rows from 1 to max_rows_for_read) for the test.
            # This amount of data will be read during the test. Part of this will be read from a cache (during run the
            # data will be loaded into a cache) and part - from a disk
            if not max_rows_for_read:
                max_rows_for_read = num_of_partitions
            return "'dist=gauss(1..%d, %d, %d)'" % (max_rows_for_read,
                                                    int(max_rows_for_read / 2),
                                                    int(max_rows_for_read * 0.05))

        def disk_only(max_rows_for_read):
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
        c_s_cmd = stress_command.format(**params)

        if kwargs:
            LOGGER.debug("Kwargs: %s", kwargs['kwargs'])
            for param, value in kwargs['kwargs'].items():
                c_s_cmd += f" {param} {value}"
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

    def validate_io_queue_operations(self, start_time, end_time, read_users, prometheus_stats, db_cluster,
                                     expected_ratio=None, load_high_enough=None, publish_wp_error_event=False,
                                     possible_issue=None):
        # roles_full_info example:
        #   {'role250':
        #   {'service_level': ServiceLevel: name: 'sl250',
        #                                   attributes: ServiceLevelAttributes(shares=250,
        #                                                                      timeout=None, workload_type=None),
        #   'service_level_shares': 250, 'service_level_name': "'sl250'", 'sl_group': 'sl:sl250',
        #   'sl_group_ops': 181.23794405382156}}
        roles_full_info = {}
        for user in read_users:
            roles_full_info[user['role'].name] = user['role'].role_full_info_dict()
            roles_full_info[user['role'].name]['sl_group_ops'] = None
            user['role'].validate_role_service_level_attributes_against_db()
        LOGGER.debug('ROLE - SERVICE LEVEL - SCHEDULER - SHARES: %s', roles_full_info)

        result = []
        sl_group_ops_zero = False
        # for node_ip in db_cluster.get_node_private_ips():
        for node in db_cluster.nodes:
            # If Scylla is not running on the node - do not perform validation
            if not (node.jmx_up() and node.db_up()):
                continue

            node_ip = node.private_ip_address
            # TODO: follow after this issue (prometheus return empty answer despite the data exists),
            #  if it is reproduced
            # Query 'scylla_io_queue_total_operations' from prometheus. If no data returned, try to increase the step time
            # and query again
            for step in ['30s', '45s', '60s', '120s']:
                LOGGER.debug("Query 'scylla_io_queue_total_operations' on the node %s with irate step %s ", node_ip, step)
                if io_queue_total_operations := prometheus_stats.get_scylla_io_queue_total_operations(
                        start_time, end_time, node_ip, irate_sample_sec=step):
                    break

            # Example of get_scylla_io_queue_total_operations:
            #   {'10.0.2.177': {'sl:default': [410.5785714285715, 400.36428571428576],
            #   'sl:sl500_596ca81a': [177.11428571428573, 182.02857142857144]}
            LOGGER.debug('SERVICE LEVEL GROUP - Total Operations: %s' % io_queue_total_operations)
            if not io_queue_total_operations:
                # Set this message as WARNING because I found that prometheus return empty answer despite the data
                # exists (I run this request manually and got data). Prometheus request doesn't fail, it succeeded but
                # empty, like:
                # {'status': 'success', 'data': {'resultType': 'matrix', 'result': []}}
                WorkloadPrioritisationEvent.EmptyPrometheusData(message=f'Failed to get io_queue_total_operations data from '
                                                                        f'Prometheus for node {node_ip}',
                                                                severity=Severity.WARNING).publish()
                continue

            for role_sl_attribute in roles_full_info.values():
                if role_sl_attribute['sl_group'] in io_queue_total_operations[node_ip]:
                    role_sl_attribute['sl_group_ops'] = sum(
                        io_queue_total_operations[node_ip][role_sl_attribute['sl_group']]) / \
                        len(io_queue_total_operations[node_ip][role_sl_attribute['sl_group']])
                else:
                    role_sl_attribute['sl_group_ops'] = 0.0

                # Zero Service Level group operations is not expected. It may happen due to Prometheus problem
                # (connection or else) or issue https://github.com/scylladb/scylla-enterprise/issues/2572
                if role_sl_attribute['sl_group_ops'] == 0.0:
                    sl_group_ops_zero = True

            LOGGER.debug('OPERATIONS PER ROLE: %s' % roles_full_info)

            # We know and validate expected_ratio in the feature test. In the longevity we can not perform such kind
            # of validation because WP load runs in parallel with disruptive and non-disruptive nemeses, so we can not
            # predict the ops ratio. We can check only that role with higher shares performed more operations (or not)
            if not expected_ratio:
                node_cpu = None
                if load_high_enough is None:
                    node_cpu = prometheus_stats.get_scylla_reactor_utilization(start_time=start_time,
                                                                               end_time=end_time,
                                                                               instance=node_ip)
                result.append(self.validate_io_queue_operations_relatively_to_share(roles_full_info=roles_full_info,
                                                                                    node_ip=node_ip,
                                                                                    node_cpu=node_cpu,
                                                                                    load_high_enough=load_high_enough,
                                                                                    publish_wp_error_event=publish_wp_error_event,
                                                                                    possible_issue=possible_issue))
                continue

            # TODO: next 5 lines will be used by sla_per_user_system_test.py. Will need to be adapted
            # actual_shares_ratio = self.calculate_metrics_ratio_per_user(two_users_list=read_users,
            #                                                             metrics=roles_full_info)
            # self.validate_deviation(expected_ratio=expected_ratio, actual_ratio=actual_shares_ratio,
            #                         msg=f'Validate scheduler CPU runtime on the node {node_ip}. '
            #                             f'Run time per role: {roles_full_info}')

        if any(result):
            if sl_group_ops_zero:
                result.insert(0, "\nProbably the issue https://github.com/scylladb/scylla-enterprise/issues/2572")
            raise SchedulerRuntimeUnexpectedValue("".join(result))

    @staticmethod
    def validate_io_queue_operations_relatively_to_share(roles_full_info: dict, node_ip: str,
                                                         load_high_enough: bool = None, node_cpu: float = None,
                                                         publish_wp_error_event=False,
                                                         possible_issue=None):
        # roles_full_info example:
        #   {'role250': {'service_level': ServiceLevel: name: 'sl250',
        #   attributes: ServiceLevelAttributes(shares=250, timeout=None, workload_type=None),
        #   'service_level_shares': 250, 'service_level_name': "'sl250'", 'sl_group': 'sl:sl250',
        #   'sl_group_ops': 181.23794405382156}}
        shares = [sl['service_level_shares'] for sl in roles_full_info.values()]

        if not shares or len([s for s in shares if s]) < 2:
            WorkloadPrioritisationEvent.RatioValidationEvent(
                message='Not enough service level shares for validation. Expected two Service Levels, received '
                        f'{len([s for s in shares if s])}: {shares}. Runtime can not be validated',
                severity=Severity.NORMAL).publish()
            return None

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

        operations = [sl['sl_group_ops'] for sl in roles_full_info.values()]
        # If operations of role1 less than operations of role2, dividing result will be less than 1 always
        try:
            operations_ratio = (operations[0] / operations[1]) < 1
        except ZeroDivisionError:
            # If operations[1] == 0 then operations_ratio can not be less than 1
            operations_ratio = False

        # Validate that role with higher shares get more resources and vice versa
        if 0.0 not in operations and shares_ratio == operations_ratio:
            WorkloadPrioritisationEvent.RatioValidationEvent(
                message=f'Role with higher shares got more resources on the node with IP {node_ip} as expected',
                severity=Severity.NORMAL).publish()
            return ""

        error_message = ""
        ops_per_sl_group = []
        zero_operations_service_level = []
        # If operations per scheduler group is not as expected - return error.
        for service_level in roles_full_info.values():
            ops_per_sl_group.append(f"{service_level['sl_group']} (shares "
                                    f"{service_level['service_level_shares']}): "
                                    f"{round(service_level['sl_group_ops'], 2)}")
            if service_level['sl_group_ops'] == 0.0:
                zero_operations_service_level.append(service_level['sl_group'])

        ops_per_sl_group_str = "\n  ".join(ops_per_sl_group)

        issue = ""
        if zero_operations_service_level:
            if possible_issue and possible_issue.get("zero resources"):
                issue = f"\n  (Possible issue {possible_issue['zero resources']})"
            message = (f'\n(Node {node_ip}) - Service level{"(s)" if len(zero_operations_service_level) > 1 else ""} '
                       f'{", ".join(zero_operations_service_level)} did not get resources unexpectedly.%s '
                       f'Runtime per service level group:\n  {ops_per_sl_group_str}{issue}')
        else:
            if possible_issue and possible_issue.get("less resources"):
                issue = f"\n  (Possible issue {possible_issue['less resources']})"
            message = (f'\n(Node {node_ip}) - Service level with higher shares got less resources unexpectedly.%s '
                       f'Runtime per service level group:\n  {ops_per_sl_group_str}{issue}')

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
    def calculate_metrics_ratio_per_user(two_users_list, metrics=None):
        """
        :param metrics: calculate ratio for specific Scylla or cassandra-stress metrics (ops for example).
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
            for auth in [entity.get('user'), entity.get('role'), entity.get('service_level')]:
                if auth:
                    try:
                        auth.drop()
                    except Exception as error:  # noqa: BLE001
                        LOGGER.error("Failed to drop '%s'. Error: %s", auth.name, error)

    @staticmethod
    def get_scheduler_shares_per_group(node, scheduler_group_name: str = None) -> list:
        node_info_service = NodeLoadInfoServices().get(node)
        scheduler_groups = node_info_service.scylla_scheduler_shares()
        # scheduler_groups example: {"sl:sl200": [200, 200], "sl:default": [1000, 1000]}
        LOGGER.debug("Found scheduler groups: %s", scheduler_groups)
        if scheduler_group_name is None:
            return scheduler_groups

        return scheduler_groups.get(scheduler_group_name)

    @retrying(n=40, sleep_time=3, message="Wait for service level has been propagated to all nodes",
              allowed_exceptions=(Exception, SchedulerGroupNotFound, WrongServiceLevelShares,))
    def wait_for_service_level_propagated(self, cluster, service_level):
        """Wait for service level has been propagated to all nodes"""
        for node in cluster.nodes:
            LOGGER.debug("Start wait for service level propagated for service_level %s on the node '%s'",
                         service_level.name, node.name)
            if not (scheduler_shares := self.get_scheduler_shares_per_group(
                    node=node,
                    scheduler_group_name=service_level.scheduler_group_name)):
                raise SchedulerGroupNotFound(f"Scheduler groups for {service_level.name} service levels is not created on the "
                                             f"node '{node.name}'. Maybe the issue scylla-cluster-tests#7082")

            if len(scheduler_shares) != node.scylla_shards:
                raise WrongServiceLevelShares(f"Expected that scheduler group is created on every Scylla shard for {service_level.name} "
                                              f"service level but actually it was created on {len(scheduler_shares)} shards: "
                                              f"{scheduler_shares} on the node '{node.name}'")

            if len(set(scheduler_shares)) > 1:
                raise WrongServiceLevelShares(f"Expected same shares on every shard for {service_level.name} service level but "
                                              f"actually the shares on the every shard are: {scheduler_shares} on the node '{node.name}'")

            if scheduler_shares[0] != service_level.shares:
                raise WrongServiceLevelShares(f"Expected {service_level.name} service level with '{service_level.shares}' shares "
                                              f"but actually it is {scheduler_shares} shares on the node '{node.name}'")
            LOGGER.debug("Finish wait for service level propagated for service_level %s on the node '%s'",
                         service_level.name, node.name)
