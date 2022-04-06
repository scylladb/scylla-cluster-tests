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

# pylint: disable=too-many-lines

"""
Classes that introduce disruption in clusters.
"""
import copy
import inspect
import logging
import random
import time
import datetime
import threading
import os
import re
import traceback
import json
from typing import List, Optional, Type, Callable, Tuple, Dict, Set, Union
from functools import wraps, partial
from collections import defaultdict, Counter, namedtuple
from concurrent.futures import ThreadPoolExecutor

from elasticsearch.exceptions import ConnectionTimeout as ElasticSearchConnectionTimeout
from invoke import UnexpectedExit
from cassandra import ConsistencyLevel
from argus.db.db_types import NemesisStatus, NemesisRunInfo, NodeDescription

from sdcm.paths import SCYLLA_YAML_PATH
from sdcm.cluster import (
    BaseCluster,
    BaseNode,
    BaseScyllaCluster,
    ClusterNodesNotReady,
    DB_LOG_PATTERN_RESHARDING_START,
    DB_LOG_PATTERN_RESHARDING_FINISH,
    NodeSetupFailed,
    NodeSetupTimeout,
    NodeStayInClusterAfterDecommission,
)
from sdcm.cluster_k8s.mini_k8s import LocalKindCluster
from sdcm.mgmt import TaskStatus
from sdcm.utils.compaction_ops import CompactionOps
from sdcm.provision.scylla_yaml import SeedProvider
from sdcm.remote import shell_script_cmd
from sdcm.utils.ldap import SASLAUTHD_AUTHENTICATOR
from sdcm.utils.common import (get_db_tables, generate_random_string,
                               update_certificates, reach_enospc_on_node, clean_enospc_on_node,
                               parse_nodetool_listsnapshots,
                               update_authenticator, ParallelObject)
from sdcm.utils import cdc
from sdcm.utils.decorators import retrying, latency_calculator_decorator
from sdcm.utils.decorators import timeout as timeout_decor
from sdcm.utils.docker_utils import ContainerManager
from sdcm.utils.k8s import (
    convert_cpu_units_to_k8s_value,
    convert_cpu_value_from_k8s_to_units,
)
from sdcm.log import SDCMAdapter
from sdcm.prometheus import nemesis_metrics_obj
from sdcm import wait
from sdcm.sct_events import Severity
from sdcm.sct_events.system import InfoEvent
from sdcm.sct_events.filters import DbEventsFilter, EventsSeverityChangerFilter
from sdcm.sct_events.loaders import CassandraStressLogEvent
from sdcm.sct_events.nemesis import DisruptionEvent
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.decorators import raise_event_on_failure
from sdcm.sct_events.group_common_events import (ignore_alternator_client_errors, ignore_no_space_errors,
                                                 ignore_scrub_invalid_errors)
from sdcm.db_stats import PrometheusDBStats
from sdcm.utils.replication_strategy_utils import temporary_replication_strategy_setter, \
    NetworkTopologyReplicationStrategy, ReplicationStrategy, SimpleReplicationStrategy
from sdcm.utils.sstable.load_utils import SstableLoadUtils
from sdcm.utils.toppartition_util import NewApiTopPartitionCmd, OldApiTopPartitionCmd
from sdcm.utils.version_utils import MethodVersionNotFound, scylla_versions
from sdcm.remote.libssh2_client.exceptions import UnexpectedExit as Libssh2UnexpectedExit
from sdcm.cluster_k8s import PodCluster, ScyllaPodCluster
from sdcm.nemesis_publisher import NemesisElasticSearchPublisher
from sdcm.argus_test_run import ArgusTestRun
from sdcm.wait import wait_for
from test_lib.compaction import CompactionStrategy, get_compaction_strategy, get_compaction_random_additional_params
from test_lib.cql_types import CQLTypeBuilder

LOGGER = logging.getLogger(__name__)


class NoFilesFoundToDestroy(Exception):
    pass


class NoKeyspaceFound(Exception):
    pass


class NoTableFound(Exception):
    pass


class FilesNotCorrupted(Exception):
    pass


class LogContentNotFound(Exception):
    pass


class LdapNotRunning(Exception):
    pass


class UnsupportedNemesis(Exception):
    """ raised from within a nemesis execution to skip this nemesis"""


class NoMandatoryParameter(Exception):
    """ raised from within a nemesis execution to skip this nemesis"""


class DefaultValue:  # pylint: disable=too-few-public-methods
    """
    This is class is intended to be used as default value for the cases when None is not applicable
    """
    ...


class CdcStreamsWasNotUpdated(Exception):
    """ raised if messages:
          - Generation {}: streams description table already updated
          - CDC description table successfully updated with generation
        were not found in logs
    """


class Nemesis:  # pylint: disable=too-many-instance-attributes,too-many-public-methods

    MINUTE_IN_SEC: int = 60
    HOUR_IN_SEC: int = 60 * MINUTE_IN_SEC
    disruptions_list: list[Callable] = []
    DISRUPT_NAME_PREF: str = "disrupt_"

    # nemesis flags:
    topology_changes: bool = False  # flag that signal that nemesis is changing cluster topology,
    # i.e. adding/removing nodes/data centers
    disruptive: bool = False  # flag that signal that nemesis disrupts node/cluster,
    # i.e reboot,kill, hardreboot, terminate
    run_with_gemini: bool = True  # flag that signal that nemesis runs with gemini tests
    networking: bool = False  # flag that signal that nemesis interact with nemesis,
    # i.e switch off/on network interface, network issues
    kubernetes: bool = False  # flag that signal that nemesis run with k8s cluster
    limited: bool = False  # flag that signal that nemesis are belong to limited set of nemesises
    has_steady_run: bool = False  # flag that signal that nemesis should be run with perf tests with steady run

    def __new__(cls, tester_obj, termination_event, *args):  # pylint: disable=unused-argument
        for name, member in inspect.getmembers(cls, lambda x: inspect.isfunction(x) or inspect.ismethod(x)):
            if name.startswith(cls.DISRUPT_NAME_PREF):
                # add "disrupt_method_wrapper" decorator to all methods are started with "disrupt_"
                setattr(cls, name, disrupt_method_wrapper(member))
        return object.__new__(cls)

    def __init__(self, tester_obj, termination_event, *args):  # pylint: disable=unused-argument
        # *args -  compatible with CategoricalMonkey
        self.tester = tester_obj  # ClusterTester object
        self.cluster: Union[BaseCluster, BaseScyllaCluster] = tester_obj.db_cluster
        self.loaders = tester_obj.loaders
        self.monitoring_set = tester_obj.monitors
        self.target_node = None
        logger = logging.getLogger(__name__)
        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
        self.termination_event = termination_event
        self.operation_log = []
        self.current_disruption = None
        self.duration_list = []
        self.error_list = []
        self.interval = 60 * self.tester.params.get('nemesis_interval')  # convert from min to sec
        self.start_time = time.time()
        self.stats = {}
        self.metrics_srv = nemesis_metrics_obj()
        self.task_used_streaming = None
        self.filter_seed = self.cluster.params.get('nemesis_filter_seeds')
        self._random_sequence = None
        self._add_drop_column_max_per_drop = 5
        self._add_drop_column_max_per_add = 5
        self._add_drop_column_max_column_name_size = 10
        self._add_drop_column_max_columns = 200
        self._add_drop_column_columns_info = {}
        self._add_drop_column_target_table = []
        self._add_drop_column_tables_to_ignore = {
            'alternator_usertable': '*',  # Ignore alternator tables
            'ks_truncate': 'counter1',  # Ignore counter table
            'keyspace1': 'counter1',  # Ignore counter table
            # TODO: issue https://github.com/scylladb/scylla/issues/6074. Waiting for dev conclusions
            'cqlstress_lwt_example': '*'  # Ignore LWT user-profile tables
        }
        self.es_publisher = NemesisElasticSearchPublisher(self.tester)

    @classmethod
    def add_disrupt_method(cls, func=None):
        """
        Add disrupt methods to Nemesis class, so those can be randomlly selected by `Nemesis.call_random_disrupt_method`
        or `Nemesis.get_list_of_disrupt_methods_for_nemesis_subclasses`.

        example of usage:
        >>> class AddRemoveDCMonkey(Nemesis):
        >>>    @Nemesis.add_disrupt_method
        >>>    def disrupt_add_remove_dc(self):
        >>>        return 'Worked'
        >>>
        >>>    def disrupt(self):
        >>>        self.disrupt_add_remove_dc()

        :param func: if not None, function was used with parantasis @Nemesis.add_disrupt_method()
        :return: the original function
        """
        if func is None:
            # if func is not defined, return a this function wrapped see https://stackoverflow.com/a/39335652
            return partial(cls.add_disrupt_method)

        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(self, *args, **kwargs)

        setattr(cls, func.__name__, wrapper)  # bind it to Nemesis class
        return func  # returning func means func can still be used normally

    def update_stats(self, disrupt, status=True, data=None):
        if not data:
            data = {}
        key = {True: 'runs', False: 'failures'}
        if disrupt not in self.stats:
            self.stats[disrupt] = {'runs': [], 'failures': [], 'cnt': 0}
        self.stats[disrupt][key[status]].append(data)
        self.stats[disrupt]['cnt'] += 1
        self.log.debug('Update nemesis info with: %s', data)
        if self.tester.create_stats:
            self.tester.update({'nemesis': self.stats})
        if self.es_publisher:
            self.es_publisher.publish(disrupt_name=disrupt, status=status, data=data)

    def publish_event(self, disrupt, status=True, data=None):
        if not data:
            data = {}
        data['node'] = self.target_node
        severity = Severity.NORMAL if status else Severity.ERROR
        DisruptionEvent(nemesis_name=disrupt, severity=severity, **data).publish()

    def set_current_running_nemesis(self, node):
        node.running_nemesis = self.current_disruption

    @staticmethod
    def unset_current_running_nemesis(node):
        if node is not None:
            node.running_nemesis = None

    def _get_target_nodes(
            self,
            is_seed: Optional[Union[bool, DefaultValue]] = DefaultValue,
            dc_idx: Optional[int] = None,
            rack: Optional[int] = None) -> list:
        """
        Filters and return nodes in the cluster that has no running nemesis on them
        It can filter node by following criteria: is_seed, dc_idx, rack
        Same mechanism works for other parameters, if multiple criteria provided it will return nodes
        that match all of them.
        if is_seed is None - it will ignore seed status of the nodes
        if is_seed is True - it will pick only seed nodes
        if is_seed is False - it will pick only non-seed nodes
        if is_seed is DefaultValue - if self.filter_seed is True it act as if is_seed=False,
          otherwise it will act as if is_seed is None
        """
        if is_seed is DefaultValue:
            is_seed = False if self.filter_seed else None
        nodes = [node for node in self.cluster.nodes if not node.running_nemesis]
        if is_seed is not None:
            nodes = [node for node in nodes if node.is_seed == is_seed]
        if dc_idx is not None:
            nodes = [node for node in nodes if node.dc_idx == dc_idx]
        if rack is not None:
            nodes = [node for node in nodes if node.rack == rack]
        return nodes

    def set_target_node(self, dc_idx: Optional[int] = None, rack: Optional[int] = None,
                        is_seed: Union[bool, DefaultValue, None] = DefaultValue,
                        allow_only_last_node_in_rack: bool = False):
        """Set a Scylla node as target node.

        if is_seed is None - it will ignore seed status of the nodes
        if is_seed is True - it will pick only seed nodes
        if is_seed is False - it will pick only non-seed nodes
        if is_seed is DefaultValue - if self.filter_seed is True it act as if is_seed=False,
          otherwise it will act as if is_seed is None
        """
        self.unset_current_running_nemesis(self.target_node)
        nodes = self._get_target_nodes(is_seed=is_seed, dc_idx=dc_idx, rack=rack)
        if not nodes:
            dc_str = '' if dc_idx is None else f'dc {dc_idx} '
            rack_str = '' if rack is None else f'rack {rack} '
            raise UnsupportedNemesis(
                f"Can't allocate node from {dc_str}{rack_str}to run nemesis on")
        if allow_only_last_node_in_rack:
            self.target_node = nodes[-1]
        else:
            self.target_node = random.choice(nodes)

        self.set_current_running_nemesis(node=self.target_node)
        self.log.info('Current Target: %s with running nemesis: %s',
                      self.target_node, self.target_node.running_nemesis)

    @raise_event_on_failure
    def run(self, interval=None):
        self.es_publisher.create_es_connection()
        if interval:
            self.interval = interval * 60
        self.log.info('Interval: %s s', self.interval)
        while not self.termination_event.is_set():
            cur_interval = self.interval
            try:
                self.disrupt()
            except (UnsupportedNemesis, MethodVersionNotFound) as exc:
                self.log.warning("Skipping unsupported nemesis: %s", exc)
                cur_interval = 0
            finally:
                self.unset_current_running_nemesis(self.target_node)
                self.termination_event.wait(timeout=cur_interval)

    def report(self):
        if self.duration_list:
            avg_duration = sum(self.duration_list) / len(self.duration_list)
        else:
            avg_duration = 0

        self.log.info('Report')
        self.log.info('DB Version: %s', getattr(self.cluster.nodes[0], "scylla_version", "n/a"))
        self.log.info('Interval: %s s', self.interval)
        self.log.info('Average duration: %s s', avg_duration)
        self.log.info('Total execution time: %s s', int(time.time() - self.start_time))
        self.log.info('Times executed: %s', len(self.duration_list))
        self.log.info('Unexpected errors: %s', len(self.error_list))
        self.log.info('Operation log:')
        for operation in self.operation_log:
            self.log.info(operation)

    # pylint: disable=too-many-arguments,unused-argument
    def get_list_of_methods_compatible_with_backend(
            self,
            disruptive: Optional[bool] = None,
            run_with_gemini: Optional[bool] = None,
            networking: Optional[bool] = None,
            limited: Optional[bool] = None,
            topology_changes: Optional[bool] = None) -> List[str]:
        return self.get_list_of_methods_by_flags(
            disruptive=disruptive,
            run_with_gemini=run_with_gemini,
            networking=networking,
            kubernetes=self._is_it_on_kubernetes() or None,
            limited=limited,
            topology_changes=topology_changes,
        )

    def _is_it_on_kubernetes(self) -> bool:
        return isinstance(getattr(self.tester, "db_cluster", None), PodCluster)

    # pylint: disable=too-many-arguments,unused-argument
    def get_list_of_methods_by_flags(
            self,
            disruptive: Optional[bool] = None,
            run_with_gemini: Optional[bool] = None,
            networking: Optional[bool] = None,
            kubernetes: Optional[bool] = None,
            limited: Optional[bool] = None,
            topology_changes: Optional[bool] = None) -> List[str]:
        subclasses_list = self._get_subclasses(
            disruptive=disruptive,
            run_with_gemini=run_with_gemini,
            networking=networking,
            kubernetes=kubernetes,
            limited=limited,
            topology_changes=topology_changes
        )
        disrupt_methods_list = []
        for subclass in subclasses_list:
            method_name = re.search(
                r'self\.(?P<method_name>disrupt_[A-Za-z_]+?)\(.*\)', inspect.getsource(subclass), flags=re.MULTILINE)
            if method_name:
                disrupt_methods_list.append(method_name.group('method_name'))
        self.log.debug("Gathered subclass methods: {}".format(disrupt_methods_list))
        return disrupt_methods_list

    def get_list_of_subclasses_by_property_name(self, list_of_properties_to_include):
        flags = {flag_name: True for flag_name in list_of_properties_to_include}
        subclasses_list = self._get_subclasses(**flags)
        return subclasses_list

    def get_list_of_disrupt_methods(self, subclasses_list):
        disrupt_methods_list = []
        for subclass in subclasses_list:
            method_name = re.search(
                r'self\.(?P<method_name>disrupt_[A-Za-z_]+?)\(.*\)', inspect.getsource(subclass), flags=re.MULTILINE)
            if method_name:
                disrupt_methods_list.append(method_name.group('method_name'))
        self.log.debug("list of matching disrupions: {}".format(disrupt_methods_list))
        for _ in disrupt_methods_list:
            disrupt_methods = [attr[1] for attr in inspect.getmembers(self) if
                               attr[0] in disrupt_methods_list and
                               callable(attr[1])]
        return disrupt_methods

    @classmethod
    def _get_subclasses(cls, **flags) -> List[Type['Nemesis']]:
        tmp = Nemesis.__subclasses__()
        subclasses = []
        while tmp:
            for nemesis in tmp.copy():
                subclasses.append(nemesis)
                tmp.remove(nemesis)
                tmp.extend(nemesis.__subclasses__())
        return cls._get_subclasses_from_list(subclasses, **flags)

    @staticmethod
    def _get_subclasses_from_list(
            list_of_nemesis: List[Type['Nemesis']],
            **flags) -> List[Type['Nemesis']]:
        """
        It apply 'and' logic to filter,
            if any value in the filter does not match what nemeses have,
            nemeses will be filtered out.
        """
        nemesis_subclasses = []
        nemesis_to_exclude = COMPLEX_NEMESIS + DEPRECATED_LIST_OF_NEMESISES
        for nemesis in list_of_nemesis:
            if nemesis in nemesis_to_exclude:
                continue
            matches = True
            for filter_name, filter_value in flags.items():
                if filter_value is None:
                    continue
                try:
                    attr = getattr(nemesis, filter_name)
                except AttributeError:
                    LOGGER.warning("The required nemesis flag {} was not found".format(filter_name))
                    flags[filter_name] = None
                    continue
                if attr != filter_value:
                    matches = False
                    break
            if not matches:
                continue
            nemesis_subclasses.append(nemesis)
        return nemesis_subclasses

    def __str__(self):
        try:
            return str(self.__class__).split("'")[1]
        except Exception:  # pylint: disable=broad-except
            return str(self.__class__)

    def _kill_scylla_daemon(self):
        with EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                         event_class=CassandraStressLogEvent,
                                         regex=".*Connection reset by peer.*",
                                         extra_time_to_expiration=30):
            self.log.info('Kill all scylla processes in %s', self.target_node)
            self.target_node.remoter.sudo("pkill -9 scylla", ignore_status=True)

            # Wait for the process to be down before waiting for service to be restarted
            self.target_node.wait_db_down(check_interval=2)

            # Let's wait for the target Node to have their services re-started
            self.log.info('Waiting scylla services to be restarted after we killed them...')
            self.target_node.wait_db_up(timeout=14400)
            if self.cluster.params.get('use_mgmt'):
                # Workaround for https://github.com/scylladb/scylla-manager/issues/2813
                # When scylla take too long time to bring api port up
                #  scylla-manager-agent fails to start and never go up
                self.target_node.start_service(service_name='scylla-manager-agent', timeout=600, ignore_status=True)
            self.log.info('Waiting JMX services to be restarted after we killed them...')
            self.target_node.wait_jmx_up()
        self.cluster.wait_for_schema_agreement()

    def disrupt_stop_wait_start_scylla_server(self, sleep_time=300):  # pylint: disable=invalid-name
        self.target_node.stop_scylla_server(verify_up=False, verify_down=True)
        self.log.info("Sleep for %s seconds", sleep_time)
        time.sleep(sleep_time)
        if self._is_it_on_kubernetes():
            # Kubernetes brings node up automatically, no need to start it up
            self.target_node.wait_db_up(timeout=sleep_time)
            return
        self.target_node.start_scylla_server(verify_up=True, verify_down=False)

    def disrupt_stop_start_scylla_server(self):  # pylint: disable=invalid-name
        self.target_node.stop_scylla_server(verify_up=False, verify_down=True)
        self.target_node.start_scylla_server(verify_up=True, verify_down=False)

    def disrupt_start_stop_major_compaction(self):
        self.set_target_node()
        node = self.target_node
        compaction_ops = CompactionOps(cluster=self.cluster, node=node)
        timeout = 360
        trigger_func = partial(compaction_ops.trigger_major_compaction)
        watch_func = partial(compaction_ops.stop_on_user_compaction_logged,
                             node=node,
                             mark=1,
                             watch_for="User initiated compaction started on behalf of",
                             timeout=timeout,
                             stop_func=compaction_ops.stop_major_compaction)
        ParallelObject(objects=[trigger_func, watch_func], timeout=timeout).call_objects()

    def disrupt_start_stop_scrub_compaction(self):
        self.set_target_node()
        node = self.target_node
        compaction_ops = CompactionOps(cluster=self.cluster, node=node)
        timeout = 360
        trigger_func = partial(compaction_ops.trigger_scrub_compaction)
        watch_func = partial(compaction_ops.stop_on_user_compaction_logged,
                             node=node,
                             watch_for="Scrubbing",
                             timeout=timeout,
                             stop_func=compaction_ops.stop_scrub_compaction)
        ParallelObject(objects=[trigger_func, watch_func], timeout=timeout).call_objects()

    def disrupt_start_stop_cleanup_compaction(self):
        self.set_target_node()
        node = self.target_node
        compaction_ops = CompactionOps(cluster=self.cluster, node=node)
        timeout = 120
        trigger_func = partial(compaction_ops.trigger_cleanup_compaction)
        watch_func = partial(compaction_ops.stop_on_user_compaction_logged,
                             node=node,
                             mark=node.mark_log(),
                             timeout=timeout,
                             watch_for="Cleaning",
                             stop_func=compaction_ops.stop_cleanup_compaction)
        ParallelObject(objects=[trigger_func, watch_func], timeout=timeout).call_objects()

    def disrupt_start_stop_validation_compaction(self):
        self.set_target_node()
        node = self.target_node
        compaction_ops = CompactionOps(cluster=self.cluster, node=node)
        timeout = 360
        trigger_func = partial(compaction_ops.trigger_validation_compaction)
        watch_func = partial(compaction_ops.stop_on_user_compaction_logged,
                             node=node,
                             mark=node.mark_log(),
                             watch_for="Scrubbing ",
                             timeout=timeout,
                             stop_func=compaction_ops.stop_validation_compaction)
        ParallelObject(objects=[trigger_func, watch_func], timeout=timeout).call_objects()

    # This nemesis should be run with "private" ip_ssh_connections till the issue #665 is not fixed

    def disabled_disrupt_restart_then_repair_node(self):  # pylint: disable=invalid-name
        # Task https://trello.com/c/llRuLIOJ/2110-add-dbeventfilter-for-nosuchcolumnfamily-error
        # If this error happens during the first boot with the missing disk this issue is expected and it's not an issue
        with DbEventsFilter(db_event=DatabaseLogEvent.DATABASE_ERROR,
                            line="Can't find a column family with UUID", node=self.target_node), \
                DbEventsFilter(db_event=DatabaseLogEvent.BACKTRACE,
                               line="Can't find a column family with UUID", node=self.target_node):
            self.target_node.restart()

        self.log.info('Waiting scylla services to start after node restart')
        self.target_node.wait_db_up(timeout=28800)  # 8 hours
        self.log.info('Waiting JMX services to start after node restart')
        self.target_node.wait_jmx_up()
        self.repair_nodetool_repair()

    def disrupt_resetlocalschema(self):  # pylint: disable=invalid-name
        result = self.target_node.run_nodetool(sub_cmd='help', args='resetlocalschema')
        if 'Unknown command resetlocalschema' in result.stdout:
            raise UnsupportedNemesis("nodetool doesn't support resetlocalschema")

        rlocal_schema_res = self.target_node.follow_system_log(patterns=["schema_tables - Schema version changed to"])
        self.target_node.run_nodetool("resetlocalschema")

        assert wait_for(
            func=lambda: list(rlocal_schema_res),
            timeout=30,
            text="Waiting for schema version being recalculated",
            throw_exc=False,
        ), "Schema version has not been recalculated"

        # Check schema version on the nodes will be preformed after nemesis by ClusterHealthChecker
        # Waiting 60 sec: this time is defined by Tomasz
        self.log.debug("Sleep for 60 sec: the other nodes should pull new version")
        time.sleep(60)

    def disrupt_hard_reboot_node(self):
        self.target_node.reboot(hard=True)
        self.log.info('Waiting scylla services to start after node reboot')
        self.target_node.wait_db_up()
        self.log.info('Waiting JMX services to start after node reboot')
        self.target_node.wait_jmx_up()
        self.cluster.wait_for_nodes_up_and_normal(nodes=[self.target_node])

    def disrupt_multiple_hard_reboot_node(self):  # pylint: disable=invalid-name
        # If this messages appeared, cdc successfully update generation
        cdc_success_msg = ["streams description table already updated",
                           "CDC description table successfully updated with generation"]
        num_of_reboots = random.randint(2, 10)
        InfoEvent(message=f'MultipleHardRebootNode {self.target_node}')
        for i in range(num_of_reboots):
            self.log.debug("Rebooting {} out of {} times".format(i + 1, num_of_reboots))
            cdc_expected_error = self.target_node.follow_system_log(
                patterns=["cdc - Could not update CDC description table with generation"])
            cdc_success_msg = self.target_node.follow_system_log(
                patterns=cdc_success_msg)
            self.target_node.reboot(hard=True)
            if random.choice([True, False]):
                self.log.info('Waiting scylla services to start after node reboot')
                self.target_node.wait_db_up()
            else:
                self.log.info('Waiting JMX services to start after node reboot')
                self.target_node.wait_jmx_up()
            self.cluster.wait_for_nodes_up_and_normal(nodes=[self.target_node])
            found_cdc_error = list(cdc_expected_error)
            found_success_info = list(cdc_success_msg)
            if found_cdc_error and not found_success_info:
                # if cdc error message "cdc - Could not update CDC description..."
                # was found in log during reboot, but after that success messages:
                # "streams description updated" or "CDC desc table updated" were not
                # found in logs, raise Exception to fail the nemesis.
                raise CdcStreamsWasNotUpdated(
                    f"After '{found_cdc_error[0]}', messages '{' or '.join(cdc_success_msg)}' were not found")

            cdc_success_msg = cdc_expected_error = None
            sleep_time = random.randint(0, 100)
            self.log.info(
                'Sleep {} seconds after hard reboot and service-up for node {}'.format(sleep_time, self.target_node))
            time.sleep(sleep_time)

    def disrupt_soft_reboot_node(self):
        self.target_node.reboot(hard=False)
        self.log.info('Waiting scylla services to start after node reboot')
        self.target_node.wait_db_up()
        self.log.info('Waiting JMX services to start after node reboot')
        self.target_node.wait_jmx_up()
        self.cluster.wait_for_nodes_up_and_normal(nodes=[self.target_node])

    def disrupt_rolling_restart_cluster(self, random_order=False):
        self.cluster.restart_scylla(random_order=random_order)

    def disrupt_switch_between_password_authenticator_and_saslauthd_authenticator_and_back(self):
        """
        If prepare_saslauthd is enabled, saslauthd and ldap environment will be prepared for
        using SaslauthdAuthenticator. We have same account (cassandra) for SaslauthdAuthenticator
        and PasswordAuthenticator, so it can be toggled smoothly without effect to c-s workloads.

        It's only support to switch between PasswordAuthenticator and SaslauthdAuthenticator,
        the authenticator will be reset back in the end of nemesis.
        """
        if not self.cluster.params.get('prepare_saslauthd'):
            raise UnsupportedNemesis("SaslauthdAuthenticator can't work without saslauthd environment")
        if not self.cluster.nodes[0].is_enterprise:
            raise UnsupportedNemesis("SaslauthdAuthenticator is only supported by Scylla Enterprise")

        with self.target_node.remote_scylla_yaml() as scylla_yml:
            orig_auth = scylla_yml.authenticator
            if orig_auth == SASLAUTHD_AUTHENTICATOR:
                opposite_auth = 'PasswordAuthenticator'
            elif orig_auth == 'PasswordAuthenticator':
                opposite_auth = SASLAUTHD_AUTHENTICATOR
            else:
                raise UnsupportedNemesis(
                    'This nemesis only supports to switch between SaslauthdAuthenticator and PasswordAuthenticator')
        update_authenticator(self.cluster.nodes, opposite_auth)
        try:
            # Run connect a new session after authenticator switch, and run a short workload
            self._prepare_test_table(ks='keyspace_for_authenticator_switch', table='standard1')
        finally:
            # Wait 2 mins to let the workloads run with new Authenticator,
            # then switch Authenticator back to original
            time.sleep(120)
            update_authenticator(self.cluster.nodes, orig_auth)
            # Run connect a new session after authenticator switch, drop the test keyspace
            with self.cluster.cql_connection_patient(self.target_node) as session:
                session.execute('DROP KEYSPACE keyspace_for_authenticator_switch')

    def disrupt_rolling_config_change_internode_compression(self):
        def get_internode_compression_new_value_randomly(current_compression):
            self.log.debug(f"Current compression is {current_compression}")
            values = ['dc', 'all', 'none']
            values_to_toggle = list(filter(lambda value: value != current_compression, values))
            return random.choice(values_to_toggle)

        if self._is_it_on_kubernetes():
            # NOTE: on K8S update of 'scylla.yaml' and 'cassandra-rackdc.properties' files is done
            #       via update of the single reused place and serial restart of Scylla pods.
            raise UnsupportedNemesis(
                "This logic will be covered by an operator functional test. Skipping.")
        with self.target_node.remote_scylla_yaml() as scylla_yaml:
            current = scylla_yaml.internode_compression
        new_value = get_internode_compression_new_value_randomly(current)
        for node in self.cluster.nodes:
            self.log.debug(f"Changing {node} inter node compression to {new_value}")
            with node.remote_scylla_yaml() as scylla_yaml:
                scylla_yaml.internode_compression = new_value
            self.log.info(f"Restarting node {node}")
            node.restart_scylla_server()

    def disrupt_restart_with_resharding(self):
        murmur3_partitioner_ignore_msb_bits = 15  # pylint: disable=invalid-name
        self.log.info(f'Restart node with resharding. New murmur3_partitioner_ignore_msb_bits value: '
                      f'{murmur3_partitioner_ignore_msb_bits}')
        self.target_node.restart_node_with_resharding(
            murmur3_partitioner_ignore_msb_bits=murmur3_partitioner_ignore_msb_bits)
        self.log.info('Waiting scylla services to start after node restart')
        self.target_node.wait_db_up()
        self.log.info('Waiting JMX services to start after node restart')
        self.target_node.wait_jmx_up()

        # Wait 5 minutes our before return back the default value
        self.log.debug(
            'Wait 5 minutes our before return murmur3_partitioner_ignore_msb_bits back the default value (12)')
        time.sleep(360)
        self.log.info('Set back murmur3_partitioner_ignore_msb_bits value to 12')
        self.target_node.restart_node_with_resharding()

    @retrying(n=10, allowed_exceptions=(NoKeyspaceFound, NoFilesFoundToDestroy))
    def _choose_file_for_destroy(self, ks_cfs):
        file_for_destroy = ''

        ks_cf_for_destroy = random.choice(ks_cfs)  # expected value as: 'keyspace1.standard1'

        ks_cf_for_destroy = ks_cf_for_destroy.replace('.', '/')
        files = self.target_node.remoter.sudo("find /var/lib/scylla/data/%s-* -maxdepth 1 -type f"
                                              % ks_cf_for_destroy, verbose=False)
        if files.stderr:
            raise NoFilesFoundToDestroy(
                'Failed to get data files for destroy in {}. Error: {}'.format(ks_cf_for_destroy,
                                                                               files.stderr))

        for one_file in files.stdout.split():
            if not one_file or '/' not in one_file:
                continue
            file_name = os.path.basename(one_file)
            # The file name like: /var/lib/scylla/data/scylla_bench/test-f60e4f30c98f11e98d46000000000002/mc-220-big-Data.db
            # For corruption we need to remove all files that their names are started from "mc-220-" (MC format)
            # Old format: "system-truncated-ka-" (system-truncated-ka-7-Data.db)
            # Search for "<digit>-" substring

            try:
                file_name_template = re.search(r"(.*-\d+)-", file_name).group(1)
            except Exception as error:  # pylint: disable=broad-except
                self.log.debug('File name "{file_name}" is not as expected for Scylla data files. '
                               'Search files for "{ks_cf_for_destroy}" table'.format(file_name=file_name,
                                                                                     ks_cf_for_destroy=ks_cf_for_destroy))
                self.log.debug('Error: {}'.format(error))
                continue

            file_for_destroy = one_file.replace(file_name, file_name_template + '-*')
            self.log.debug('Selected files for destroy: {}'.format(file_for_destroy))
            if file_for_destroy:
                break

        if not file_for_destroy:
            raise NoFilesFoundToDestroy('Data file for destroy is not found in {}'.format(ks_cf_for_destroy))

        return file_for_destroy

    def _destroy_data_and_restart_scylla(self):

        ks_cfs = self.cluster.get_non_system_ks_cf_list(db_node=self.target_node)
        if not ks_cfs:
            raise UnsupportedNemesis(
                'Non-system keyspace and table are not found. CorruptThenRepair nemesis can\'t be run')

        # Stop scylla service before deleting sstables to avoid partial deletion of files that are under compaction
        self.target_node.stop_scylla_server(verify_up=False, verify_down=True)

        try:
            # Remove 5 data files
            for _ in range(5):
                file_for_destroy = self._choose_file_for_destroy(ks_cfs)

                result = self.target_node.remoter.sudo('rm -f %s' % file_for_destroy)
                if result.stderr:
                    raise FilesNotCorrupted('Files were not corrupted. CorruptThenRepair nemesis can\'t be run. '
                                            'Error: {}'.format(result))
                self.log.debug('Files {} were destroyed'.format(file_for_destroy))

        finally:
            self.target_node.start_scylla_server(verify_up=True, verify_down=False)

    def disrupt(self):
        raise NotImplementedError('Derived classes must implement disrupt()')

    def get_disrupt_name(self):
        return self.current_disruption.split()[0]

    def get_class_name(self):
        return self.__class__.__name__.replace('Monkey', '')

    def _set_current_disruption(self, label=None, node=None):
        self.target_node = node if node else self.target_node

        if not label:
            label = "%s on target node %s" % (self.__class__.__name__, self.target_node)
        self.log.debug('Set current_disruption -> %s', label)
        self.current_disruption = label

    def disrupt_destroy_data_then_repair(self):  # pylint: disable=invalid-name
        self._destroy_data_and_restart_scylla()
        # try to save the node
        self.repair_nodetool_repair()

    def disrupt_destroy_data_then_rebuild(self):  # pylint: disable=invalid-name
        self._destroy_data_and_restart_scylla()
        # try to save the node
        self.repair_nodetool_rebuild()

    def disrupt_nodetool_drain(self):
        result = self.target_node.run_nodetool("drain", timeout=3600, coredump_on_timeout=True)
        self.target_node.run_nodetool("status", ignore_status=True, verbose=True,
                                      warning_event_on_exception=(Exception,))

        if result is not None:
            # workaround for issue #7332: don't interrupt test and don't raise exception
            # for UnexpectedExit, Failure and CommandTimedOut if "scylla-server stop" failed
            # or scylla-server was stopped gracefully.
            self.target_node.stop_scylla_server(verify_up=False, verify_down=True, ignore_status=True)
            self.target_node.start_scylla_server(verify_up=True, verify_down=False)

    def disrupt_ldap_connection_toggle(self):
        if not self.cluster.params.get('use_ldap_authorization'):
            raise UnsupportedNemesis('Cluster is not configured to run with LDAP authorization, hence skipping')
        if not self.target_node.is_enterprise:
            raise UnsupportedNemesis('Cluster is not enterprise. LDAP is supported only for enterprise. Skipping')

        self.log.info('Will now pause the LDAP container')
        ContainerManager.pause_container(self.tester.localhost, 'ldap')
        self.log.info('Will now sleep 180 seconds')
        time.sleep(180)
        self.log.info('Will now resume the LDAP container')
        ContainerManager.unpause_container(self.tester.localhost, 'ldap')
        self.log.info('finished with nemesis')

    def disrupt_disable_enable_ldap_authorization(self):
        if not self.cluster.params.get('use_ldap_authorization'):
            raise UnsupportedNemesis('Cluster is not configured to run with LDAP authorization, hence skipping')
        if not self.target_node.is_enterprise:
            raise UnsupportedNemesis('Cluster is not enterprise. LDAP is supported only for enterprise. Skipping')

        ldap_config = {'role_manager': '',
                       'ldap_url_template': '',
                       'ldap_attr_role': '',
                       'ldap_bind_dn': '',
                       'ldap_bind_passwd': ''}

        def destroy_ldap_container():
            ContainerManager.destroy_container(self.tester.localhost, 'ldap')

        def remove_ldap_configuration_from_node(node):
            with node.remote_scylla_yaml() as scylla_yaml:
                for key in ldap_config:
                    ldap_config[key] = getattr(scylla_yaml, key)
                    setattr(scylla_yaml, key, None)
            node.restart_scylla_server()

        if not ContainerManager.is_running(self.tester.localhost, 'ldap'):
            raise LdapNotRunning("LDAP server was supposed to be running, but it is not")

        InfoEvent(message='Disable LDAP Authorization Configuration').publish()
        for node in self.cluster.nodes:
            remove_ldap_configuration_from_node(node)
        destroy_ldap_container()

        self.log.debug('Will wait few minutes with LDAP disabled, before re-enabling it')
        time.sleep(600)

        def create_ldap_container():
            self.tester.configure_ldap(self.tester.localhost)

        def add_ldap_configuration_to_node(node):
            node.refresh_ip_address()
            with node.remote_scylla_yaml() as scylla_yaml:
                scylla_yaml.update(ldap_config)
            node.restart_scylla_server()

        InfoEvent(message='Re-enable LDAP Authorization Configuration').publish()
        create_ldap_container()
        for node in self.cluster.nodes:
            add_ldap_configuration_to_node(node)

        if not ContainerManager.is_running(self.tester.localhost, 'ldap'):
            raise LdapNotRunning("LDAP server was supposed to be running, but it is not")

    @retrying(n=3, sleep_time=60, allowed_exceptions=(NodeSetupFailed, NodeSetupTimeout))
    def _add_and_init_new_cluster_node(self, old_node_ip=None, timeout=HOUR_IN_SEC * 6, rack=0):
        """When old_node_private_ip is not None replacement node procedure is initiated"""
        # TODO: make it work on K8S when we have decommissioned (by nodetool) nodes.
        #       Now it will fail because pod which hosts decommissioned Scylla member is reported
        #       as 'NotReady' and will fail the pod waiter function.
        self.log.info("Adding new node to cluster...")
        InfoEvent(message='StartEvent - Adding new node to cluster').publish()
        new_node = self.cluster.add_nodes(
            count=1, dc_idx=self.target_node.dc_idx, enable_auto_bootstrap=True, rack=rack)[0]
        self.monitoring_set.reconfigure_scylla_monitoring()
        self.set_current_running_nemesis(node=new_node)  # prevent to run nemesis on new node when running in parallel
        new_node.replacement_node_ip = old_node_ip
        try:
            self.cluster.wait_for_init(node_list=[new_node], timeout=timeout, check_node_health=False)
            self.cluster.clean_replacement_node_ip(new_node)
        except (NodeSetupFailed, NodeSetupTimeout):
            self.log.warning("TestConfig of the '%s' failed, removing it from list of nodes" % new_node)
            self.cluster.nodes.remove(new_node)
            self.log.warning("Node will not be terminated. Please terminate manually!!!")
            raise
        self.cluster.wait_for_nodes_up_and_normal(nodes=[new_node])
        InfoEvent(message="FinishEvent - New Node is up and normal").publish()
        return new_node

    def _get_kubernetes_node_break_methods(self):
        if isinstance(self.cluster, ScyllaPodCluster) and self.cluster.node_terminate_methods:
            return self.cluster.node_terminate_methods
        raise UnsupportedNemesis(f"Backend {self.cluster.params.get('cluster_backend')} "
                                 f"does not support node termination")

    def _terminate_cluster_node(self, node):
        self.cluster.terminate_node(node)
        self.monitoring_set.reconfigure_scylla_monitoring()

    def disrupt_nodetool_decommission(self, add_node=True, disruption_name=None):
        if self._is_it_on_kubernetes() and disruption_name is None:
            self.set_target_node(allow_only_last_node_in_rack=True)
        target_is_seed = self.target_node.is_seed
        self.cluster.decommission(self.target_node)
        new_node = None
        if add_node:
            # When adding node after decommission the node is declared as up only after it completed bootstrapping,
            # increasing the timeout for now
            new_node = self._add_and_init_new_cluster_node(rack=self.target_node.rack)
            # after decomission and add_node, the left nodes have data that isn't part of their tokens anymore.
            # In order to eliminate cases that we miss a "data loss" bug because of it, we cleanup this data.
            # This fix important when just user profile is run in the test and "keyspace1" doesn't exist.
            if new_node.is_seed != target_is_seed:
                new_node.set_seed_flag(target_is_seed)
                self.cluster.update_seed_provider()
            try:
                test_keyspaces = self.cluster.get_test_keyspaces()
                for node in self.cluster.nodes:
                    for keyspace in test_keyspaces:
                        node.run_nodetool(sub_cmd='cleanup', args=keyspace)
            finally:
                self.unset_current_running_nemesis(new_node)
        return new_node

    def disrupt_nodetool_seed_decommission(self, add_node=True):
        if len(self.cluster.seed_nodes) < 2:
            raise UnsupportedNemesis("To running seed decommission the cluster must contains at least 2 seed nodes")

        if not self.target_node.is_seed:
            self.target_node = random.choice(self.cluster.seed_nodes)
        self.target_node.set_seed_flag(False)
        self.cluster.update_seed_provider()

        new_seed_node = self.disrupt_nodetool_decommission(add_node=add_node, disruption_name="SeedDecommission")
        if new_seed_node and not new_seed_node.is_seed:
            new_seed_node.set_seed_flag(True)
            self.cluster.update_seed_provider()

    @latency_calculator_decorator
    def _terminate_and_wait(self, target_node, sleep_time=300):
        self._terminate_cluster_node(target_node)
        time.sleep(sleep_time)  # Sleeping for 5 mins to let the cluster live with a missing node for a while

    @latency_calculator_decorator
    def replace_node(self, old_node_ip, rack=0):
        return self._add_and_init_new_cluster_node(old_node_ip, rack=rack)

    def _verify_resharding_on_k8s(self, cpus):
        nodes_data = []
        for node in reversed(self.cluster.nodes):
            liveness_probe_failures = node.follow_system_log(
                patterns=["healthz probe: can't connect to JMX"])
            resharding_start = node.follow_system_log(patterns=[DB_LOG_PATTERN_RESHARDING_START])
            resharding_finish = node.follow_system_log(patterns=[DB_LOG_PATTERN_RESHARDING_FINISH])
            nodes_data.append((node, liveness_probe_failures, resharding_start, resharding_finish))

        self.log.info(
            "Update the cpu count to '%s' CPUs to make Scylla start "
            "the resharding process on all the nodes 1 by 1", cpus)
        self.tester.db_cluster.replace_scylla_cluster_value(
            "/spec/datacenter/racks/0/resources", {
                "limits": {
                    "cpu": cpus,
                    "memory": self.cluster.k8s_cluster.calculated_memory_limit,
                },
                "requests": {
                    "cpu": cpus,
                    "memory": self.cluster.k8s_cluster.calculated_memory_limit,
                },
            })

        # Wait for the start of the resharding.
        # In K8S it starts from the last node of a rack and then goes to previous ones.
        # One resharding with 100Gb+ may take about 3-4 minutes. So, set 5 minutes timeout per node.
        for node, liveness_probe_failures, resharding_start, resharding_finish in nodes_data:
            assert wait.wait_for(
                func=lambda: list(resharding_start),  # pylint: disable=cell-var-from-loop
                step=1, timeout=300, throw_exc=False,
                text=f"Waiting for the start of resharding on the '{node.name}' node.",
            ), f"Start of resharding hasn't been detected on the '{node.name}' node."
            resharding_started = time.time()
            self.log.debug("Resharding has been started on the '%s' node.", node.name)

            # Wait for the end of resharding
            assert wait.wait_for(
                func=lambda: list(resharding_finish),  # pylint: disable=cell-var-from-loop
                step=3, timeout=600, throw_exc=False,
                text=f"Waiting for the finish of resharding on the '{node.name}' node.",
            ), f"Finish of the resharding hasn't been detected on the '{node.name}' node."
            self.log.debug("Resharding has been finished successfully on the '%s' node.", node.name)

            # Calculate the time spent for resharding. We need to have it be bigger than 2minutes
            # because it is the timeout of the liveness probe for Scylla pods.
            resharding_time = time.time() - resharding_started
            if resharding_time < 120:
                self.log.warning(
                    "Resharding was too fast - '%s's (<120s) on the '%s' node. "
                    "So, nemesis didn't cover the case.",
                    resharding_time, node.name)
            else:
                self.log.info(
                    "Resharding took '%s's on the '%s' node. It is enough to cover the case.",
                    resharding_time, node.name)

            # Check that liveness probe didn't report any errors
            # https://github.com/scylladb/scylla-operator/issues/894
            liveness_probe_failures = list(liveness_probe_failures)
            assert not liveness_probe_failures, (
                f"There are liveness probe failures: {liveness_probe_failures}")
        self.log.info("Resharding has successfully ended on whole Scylla cluster.")

    def disrupt_nodetool_flush_and_reshard_on_kubernetes(self):
        """Covers https://github.com/scylladb/scylla-operator/issues/894"""
        if not self._is_it_on_kubernetes():
            raise UnsupportedNemesis('It is supported only on kubernetes')

        def _check_pod_stats(node):
            # Pod info:
            # status:
            #   containerStatuses:
            #   - ready: false
            #     restartCount: 0
            #     started: true
            #     ...
            target_pod_stats = {}
            for container in node._pod_status.container_statuses:  # pylint: disable=protected-access
                if container.image.split(":")[0].endswith("scylla"):
                    target_pod_stats["started"] = container.started
                    target_pod_stats["restart_count"] = container.restart_count
                    target_pod_stats["ready"] = container.ready
                    break
            return target_pod_stats

        # Calculate new value for the CPU cores dedicated for Scylla pods
        current_cpus = convert_cpu_value_from_k8s_to_units(
            self.cluster.k8s_cluster.calculated_cpu_limit)
        if current_cpus <= 1:
            new_cpus = current_cpus + 1
        else:
            new_cpus = current_cpus - 1
        new_cpus = convert_cpu_units_to_k8s_value(new_cpus)

        # Calculate Scylla pod stats and make sure it is ok
        origin_target_pod_stats = _check_pod_stats(self.target_node)
        assert origin_target_pod_stats["started"] in (True, 'True', 'true')
        assert origin_target_pod_stats["ready"] in (True, 'True', 'true')

        # Run 'nodetool flush' command
        self.target_node.run_nodetool("flush -- keyspace1")

        # Change number of CPUs dedicated for Scylla pods
        # and make sure that the resharding process begins and finishes
        self._verify_resharding_on_k8s(new_cpus)

        # Make sure that Scylla pods didn't get restarted
        target_pod_stats = _check_pod_stats(self.target_node)

        # Return the cpu count back and wait for the resharding begin and finish
        self._verify_resharding_on_k8s(current_cpus)

        # Make sure that pod was not restarted during the first resharding process
        assert target_pod_stats
        assert origin_target_pod_stats
        assert target_pod_stats.get("restart_count") == origin_target_pod_stats.get("restart_count")

        # Make sure that pod was not restarted during the second resharding process
        final_target_pod_stats = _check_pod_stats(self.target_node)
        assert final_target_pod_stats
        assert final_target_pod_stats.get("restart_count") == origin_target_pod_stats.get("restart_count")

    def disrupt_terminate_and_replace_node_kubernetes(self):  # pylint: disable=invalid-name
        for node_terminate_method_name in self._get_kubernetes_node_break_methods():
            self.set_target_node()
            InfoEvent(f'OperatorNodeTerminateAndReplace ({node_terminate_method_name}) {self.target_node}').publish()
            if not self._is_it_on_kubernetes():
                raise UnsupportedNemesis('OperatorNodeTerminateAndReplace is supported only on kubernetes')

            self._disrupt_terminate_and_replace_node_kubernetes(self.target_node, node_terminate_method_name)

    def disrupt_terminate_decommission_add_node_kubernetes(self):  # pylint: disable=invalid-name
        if not self._is_it_on_kubernetes():
            raise UnsupportedNemesis('OperatorNodeTerminateDecommissionAdd is supported only on kubernetes')
        for node_terminate_method_name in self._get_kubernetes_node_break_methods():
            self.set_target_node(rack=random.choice(list(self.cluster.racks)),
                                 allow_only_last_node_in_rack=True)
            InfoEvent(
                f'OperatorNodeTerminateDecommissionAdd ({node_terminate_method_name}) {self.target_node}').publish()
            self._disrupt_terminate_decommission_add_node_kubernetes(self.target_node, node_terminate_method_name)

    def disrupt_replace_node_kubernetes(self):
        if not self._is_it_on_kubernetes():
            raise UnsupportedNemesis('OperatorNodeReplace is supported only on kubernetes')
        old_uid = self.target_node.k8s_pod_uid
        self.log.info('TerminateNode %s', self.target_node)
        self.log.info('Mark %s to be replaced', self.target_node)
        self.target_node.wait_for_svc()
        self.target_node.mark_to_be_replaced()
        self._kubernetes_wait_till_node_up_after_been_recreated(self.target_node, old_uid=old_uid)

    def _disrupt_terminate_decommission_add_node_kubernetes(self, node,
                                                            node_terminate_method_name):  # pylint: disable=invalid-name
        self.log.info('Terminate %s', node)
        node_terminate_method = getattr(node, node_terminate_method_name)
        node_terminate_method()
        self.log.info('Decommission %s', node)
        self.cluster.decommission(node)
        new_node = self.add_new_node(rack=node.rack)
        self.unset_current_running_nemesis(new_node)

    def _disrupt_terminate_and_replace_node_kubernetes(self, node,
                                                       node_terminate_method_name):  # pylint: disable=invalid-name
        old_uid = node.k8s_pod_uid
        self.log.info('TerminateNode %s (uid=%s)', node, old_uid)
        node_terminate_method = getattr(node, node_terminate_method_name)
        node_terminate_method()
        node.wait_till_k8s_pod_get_uid(ignore_uid=old_uid)
        old_uid = node.k8s_pod_uid
        self.log.info('Mark %s (uid=%s) to be replaced', node, old_uid)
        node.wait_for_svc()
        node.mark_to_be_replaced()
        self._kubernetes_wait_till_node_up_after_been_recreated(node, old_uid=old_uid)

    def _kubernetes_wait_till_node_up_after_been_recreated(self, node, old_uid=None):
        node.wait_till_k8s_pod_get_uid(ignore_uid=old_uid)
        self.log.info('Wait till %s is ready', node)
        node.wait_for_pod_readiness()
        self.log.info(f'{node} is ready, updating ip address and monitoring')
        node.refresh_ip_address()
        self.monitoring_set.reconfigure_scylla_monitoring()

    def disrupt_terminate_and_replace_node(self):  # pylint: disable=invalid-name

        def get_node_state(node_ip: str) -> List["str"] | None:
            """Gets node state by IP address from nodetool status response"""
            status = self.cluster.get_nodetool_status()
            states = [val['state'] for dc in status.values() for ip, val in dc.items() if ip == node_ip]
            return states[0] if states else None

        if self._is_it_on_kubernetes():
            raise UnsupportedNemesis(
                'Use "disrupt_terminate_and_replace_node_kubernetes" instead this one for K8S')
        # using "Replace a Dead Node" procedure from http://docs.scylladb.com/procedures/replace_dead_node/
        old_node_ip = self.target_node.ip_address
        is_old_node_seed = self.target_node.is_seed
        InfoEvent(message='StartEvent - Terminate node and wait 5 minutes').publish()
        self._terminate_and_wait(target_node=self.target_node)
        assert get_node_state(old_node_ip) == "DN", "Removed node state should be DN"
        InfoEvent(message='FinishEvent - target_node was terminated').publish()
        new_node = self.replace_node(old_node_ip, rack=self.target_node.rack)
        try:
            if new_node.get_scylla_config_param("enable_repair_based_node_ops") == 'false':
                InfoEvent(message='StartEvent - Run repair on new node').publish()
                self.repair_nodetool_repair(new_node)
                InfoEvent(message='FinishEvent - Finished running repair on new node').publish()

            # wait until node gives up on the old node, the default timeout is `ring_delay_ms: 300000`
            # scylla: [shard 0] gossip - FatClient 10.0.22.115 has been silent for 30000ms, removing from gossip
            @retrying(n=20, sleep_time=20, allowed_exceptions=(AssertionError,))
            def wait_for_old_node_to_removed():
                state = get_node_state(old_node_ip)
                if old_node_ip == new_node.ip_address:
                    assert state == "UN", \
                        f"New node with the same IP as removed one should be in UN state but was: {state}"
                else:
                    assert state is None, \
                        f"Old node should have been removed from status but it wasn't. State was: {state}"

            wait_for_old_node_to_removed()

        finally:
            self.unset_current_running_nemesis(new_node)
            if is_old_node_seed:
                new_node.set_seed_flag(True)
                self.cluster.update_seed_provider()

    def disrupt_kill_scylla(self):
        self._kill_scylla_daemon()

    def disrupt_no_corrupt_repair(self):
        # prepare test tables and fill test data
        for i in range(10):
            self.log.debug('Prepare test tables if they do not exist')
            self._prepare_test_table(ks=f'drop_table_during_repair_ks_{i}', table='standard1')

        self.log.debug("Start repair target_node in background")
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix='NodeToolRepairThread') as thread_pool:
            thread = thread_pool.submit(self.repair_nodetool_repair)
            try:
                # drop test tables one by one during repair
                for i in range(10):
                    time.sleep(random.randint(0, 300))
                    with self.cluster.cql_connection_patient(self.target_node) as session:
                        session.execute(f'DROP TABLE drop_table_during_repair_ks_{i}.standard1')
            finally:
                thread.result()

    def disrupt_major_compaction(self):
        self.target_node.run_nodetool("compact")

    # NOTE: '2022.1.rc0' is set in advance, not guaranteed to match when appears
    @scylla_versions(("4.5.rc1", None), ("2022.1.rc0", None))
    def disrupt_load_and_stream(self):
        # Checking the columns number of keyspace1.standard1
        self.log.debug('Prepare keyspace1.standard1 if it does not exist')
        self._prepare_test_table(ks='keyspace1', table='standard1')
        column_num = SstableLoadUtils.calculate_columns_count_in_table(self.target_node)

        # Run load-and-stream test on regular standard1 table of cassandra-stress.
        if column_num < 5:
            raise UnsupportedNemesis("Schema doesn't match the snapshot, not uploading")

        test_data = SstableLoadUtils.get_load_test_data_inventory(column_num, big_sstable=False, load_and_stream=True)

        result = self.target_node.run_nodetool(sub_cmd="cfstats", args="keyspace1.standard1")

        if result is not None and result.exit_status == 0:
            map_files_to_node = SstableLoadUtils.distribute_test_files_to_cluster_nodes(nodes=self.cluster.nodes,
                                                                                        test_data=test_data)
            for sstables_info, load_on_node in map_files_to_node:
                SstableLoadUtils.upload_sstables(load_on_node, test_data=sstables_info)
                system_log_follower = SstableLoadUtils.run_load_and_stream(load_on_node)
                SstableLoadUtils.validate_load_and_stream_status(load_on_node, system_log_follower)

    # pylint: disable=too-many-statements
    def disrupt_nodetool_refresh(self, big_sstable: bool = False):
        # Checking the columns number of keyspace1.standard1
        self.log.debug('Prepare keyspace1.standard1 if it does not exist')
        self._prepare_test_table(ks='keyspace1', table='standard1')
        column_num = SstableLoadUtils.calculate_columns_count_in_table(self.target_node)

        # Note: when issue #6617 is fixed, we can try to load snapshot (cols=5) to a table (1 < cols < 5),
        #       expect that refresh will fail (no serious db error).
        if 1 < column_num < 5:
            raise UnsupportedNemesis("Schema doesn't match the snapshot, not uploading")

        test_data = SstableLoadUtils.get_load_test_data_inventory(column_num, big_sstable=big_sstable,
                                                                  load_and_stream=False)

        result = self.target_node.run_nodetool(sub_cmd="cfstats", args="keyspace1.standard1")

        if result is not None and result.exit_status == 0:
            key = '0x32373131364f334f3830'
            # Check one special key before refresh, we will verify refresh by query in the end
            # Note: we can't DELETE the key before refresh, otherwise the old sstable won't be loaded
            #       TRUNCATE can be used the clean the table, but we can't do it for keyspace1.standard1
            query_verify = f"SELECT * FROM keyspace1.standard1 WHERE key={key}"
            result = self.target_node.run_cqlsh(query_verify)
            if '(0 rows)' in result.stdout:
                self.log.debug('Key %s does not exist before refresh', key)
            else:
                self.log.debug('Key %s already exists before refresh', key)

            # Executing rolling refresh one by one
            for node in self.cluster.nodes:
                SstableLoadUtils.upload_sstables(node, test_data=test_data[0])
                system_log_follower = SstableLoadUtils.run_refresh(node, test_data=test_data[0])
                SstableLoadUtils.validate_resharding_after_refresh(node=node, system_log_follower=system_log_follower)

            # Verify that the special key is loaded by SELECT query
            result = self.target_node.run_cqlsh(query_verify)
            assert '(1 rows)' in result.stdout, f'The key {key} is not loaded by `nodetool refresh`'

    def disrupt_nodetool_enospc(self, sleep_time=30, all_nodes=False):
        if isinstance(self.cluster, LocalKindCluster):
            # Kind cluster has shared file system, it is shared not only among cluster nodes, but
            # also among kubernetes services which make kubernetes inoperational once enospc is reached.
            # Moreover, it will cause host system's /var hosting disk go out of free space too
            # which may cause unpredictable failures.
            raise UnsupportedNemesis('disrupt_nodetool_enospc is not supported on local K8S clusters')

        if all_nodes:
            nodes = self.cluster.nodes
            InfoEvent('Enospc test on {}'.format([n.name for n in nodes])).publish()
        else:
            nodes = [self.target_node]

        for node in nodes:
            with ignore_no_space_errors(node=node):

                result = node.remoter.run('cat /proc/mounts')
                if '/var/lib/scylla' not in result.stdout:
                    self.log.error("Scylla doesn't use an individual storage, skip enospc test")
                    continue

                try:
                    reach_enospc_on_node(target_node=node)
                finally:
                    clean_enospc_on_node(target_node=node, sleep_time=sleep_time)

    def _deprecated_disrupt_stop_start(self):
        # TODO: We don't support fully stopping the AMI instance anymore
        # TODO: This nemesis has to be rewritten to just stop/start scylla server
        self.log.info('StopStart %s', self.target_node)
        self.target_node.restart()

    def call_random_disrupt_method(self, disrupt_methods=None, predefined_sequence=False):
        # pylint: disable=too-many-branches

        if disrupt_methods is None:
            disrupt_methods = [attr[1] for attr in inspect.getmembers(self) if
                               attr[0].startswith('disrupt_') and
                               callable(attr[1])]
        else:
            disrupt_methods = [attr[1] for attr in inspect.getmembers(self) if
                               attr[0] in disrupt_methods and
                               callable(attr[1])]
        if not disrupt_methods:
            self.log.warning("No monkey to run")
            return
        if not predefined_sequence:
            disrupt_method = random.choice(disrupt_methods)
        else:
            if not self._random_sequence:
                # Generate random sequence, every method has same chance to be called.
                # Here we use multiple original methods list, it will increase the chance
                # to call same method continuously.
                #
                # Adjust the rate according to the test duration. Try to call more unique
                # methods and don't wait to long time to meet the balance if the test
                # duration is short.
                test_duration = self.cluster.params.get('test_duration')
                if test_duration < 600:  # less than 10 hours
                    rate = 1
                elif test_duration < 4320:  # less than 3 days
                    rate = 2
                else:
                    rate = 3
                multiple_disrupt_methods = disrupt_methods * rate
                random.shuffle(multiple_disrupt_methods)
                self._random_sequence = multiple_disrupt_methods
            # consume the random sequence
            disrupt_method = self._random_sequence.pop()

        self.execute_disrupt_method(disrupt_method)

    def execute_disrupt_method(self, disrupt_method):
        disrupt_method_name = disrupt_method.__name__.replace('disrupt_', '')
        self.log.info(">>>>>>>>>>>>>Started random_disrupt_method %s" % disrupt_method_name)
        self.metrics_srv.event_start(disrupt_method_name)
        try:
            disrupt_method()
        except Exception as exc:  # pylint: disable=broad-except
            error_msg = "Exception in random_disrupt_method %s: %s", disrupt_method_name, exc
            self.log.error(error_msg)
            self.error_list.append(error_msg)
            raise
        else:
            self.log.info("<<<<<<<<<<<<<Finished random_disrupt_method %s" % disrupt_method_name)
        finally:
            self.metrics_srv.event_stop(disrupt_method_name)

    def build_list_of_disruptions_to_execute(self, nemesis_include_filter=None, nemesis_multiply_factor=1):
        """
        Builds the list of disruptions that should be excuted during a test.

        nemesis_include_filter: should be retrived from the test yaml by using the "nemesis_include_filter".
        Here it kept for future usages and unit testing ability.
        more about nemesis_include_filter behaviour in sct_config.py

        nemesis_multiply_factor: should be retrived from the test yaml by using the "nemesis_multiply_factor".
        Here it kept for future usages and unit testing ability.
        more about nemesis_include_filter behaviour in sct_config.py
        """
        nemesis_include_filter = self.cluster.params.get('nemesis_include_filter')
        if nemesis_include_filter:
            subclasses = self.get_list_of_subclasses_by_property_name(
                list_of_properties_to_include=nemesis_include_filter)
            if subclasses:
                disruptions = self.get_list_of_disrupt_methods(subclasses_list=subclasses)
            else:
                disruptions = []
        else:
            disruptions = [attr[1] for attr in inspect.getmembers(self)
                           if attr[0].startswith('disrupt_') and callable(attr[1])]

        nemesis_multiply_factor = self.cluster.params.get('nemesis_multiply_factor')
        if nemesis_multiply_factor:
            disruptions = disruptions * nemesis_multiply_factor

        self.disruptions_list.extend(disruptions)
        self.log.debug("This is the list of callable disruptions {}".format(self.disruptions_list))
        return self.disruptions_list

    @property
    def _disruption_list_names(self):
        return [nemesis.__name__ for nemesis in self.disruptions_list]

    def shuffle_list_of_disruptions(self):
        if self.cluster.params.get('nemesis_seed'):
            nemesis_seed = self.cluster.params.get('nemesis_seed')
        else:
            nemesis_seed = random.randint(0, 1000)
            self.log.info(f'nemesis_seed generated for this test is {nemesis_seed}')
        self.log.debug(f'nemesis_seed to be used is {nemesis_seed}')

        self.log.debug(f"nemesis stack BEFORE SHUFFLE is {self._disruption_list_names}")
        random.Random(nemesis_seed).shuffle(self.disruptions_list)
        self.log.info(f"List of Nemesis to execute: {self._disruption_list_names}")

    def call_next_nemesis(self):
        if self.disruptions_list:
            self.log.debug(f'Selecting the next nemesis out of stack {self._disruption_list_names}')
            self.execute_disrupt_method(disrupt_method=self.disruptions_list.pop())
        else:
            self.log.info('Nemesis stack is empty - setting termination_event')
            self.termination_event.set()

    @latency_calculator_decorator
    def repair_nodetool_repair(self, node=None, publish_event=True):
        node = node if node else self.target_node
        node.run_nodetool(sub_cmd="repair", publish_event=publish_event)

    def repair_nodetool_rebuild(self):
        self.target_node.run_nodetool('rebuild')

    def disrupt_nodetool_cleanup(self):
        # This fix important when just user profile is run in the test and "keyspace1" doesn't exist.
        test_keyspaces = self.cluster.get_test_keyspaces()
        for node in self.cluster.nodes:
            InfoEvent('NodetoolCleanupMonkey %s' % node).publish()
            for keyspace in test_keyspaces:
                node.run_nodetool(sub_cmd="cleanup", args=keyspace)

    def _prepare_test_table(self, ks='keyspace1', table=None):
        ks_cfs = self.cluster.get_non_system_ks_cf_list(db_node=self.target_node)
        table_exist = f'{ks}.{table}' in ks_cfs if table else True

        test_keyspaces = self.cluster.get_test_keyspaces()
        # if keyspace or table doesn't exist, create it by cassandra-stress
        if ks not in test_keyspaces or not table_exist:
            stress_cmd = "cassandra-stress write n=400000 cl=QUORUM -port jmx=6868 -mode native cql3 " \
                         f"-schema 'replication(factor={self.tester.reliable_replication_factor})' -log interval=5"
            cs_thread = self.tester.run_stress_thread(
                stress_cmd=stress_cmd, keyspace_name=ks, stop_test_on_failure=False)
            cs_thread.verify_results()

    def disrupt_truncate(self):
        keyspace_truncate = 'ks_truncate'
        table = 'standard1'

        self._prepare_test_table(ks=keyspace_truncate)

        # In order to workaround issue #4924 when truncate timeouts, we try to flush before truncate.
        self.target_node.run_nodetool("flush")
        # do the actual truncation
        self.target_node.run_cqlsh(cmd='TRUNCATE {}.{}'.format(keyspace_truncate, table), timeout=120)

    def disrupt_truncate_large_partition(self):
        """
        Introduced a new truncate nemesis, it truncates a large-partition table,
        it's used to cover one improvement of compaction.
        The increase frequency of checking abortion is very useful for truncate.
        """
        ks_name = 'ks_truncate_large_partition'
        table = 'test_table'
        stress_cmd = "scylla-bench -workload=sequential -mode=write -replication-factor=3 -partition-count=10 " + \
                     "-clustering-row-count=5555 -clustering-row-size=uniform:10..20 -concurrency=10 " + \
                     "-connection-count=10 -consistency-level=quorum -rows-per-request=10 -timeout=60s " + \
                     f"-keyspace {ks_name} -table {table}"
        bench_thread = self.tester.run_stress_thread(
            stress_cmd=stress_cmd, stop_test_on_failure=False)
        self.tester.verify_stress_thread(bench_thread)

        # In order to workaround issue #4924 when truncate timeouts, we try to flush before truncate.
        self.target_node.run_nodetool("flush")
        # do the actual truncation
        self.target_node.run_cqlsh(cmd='TRUNCATE {}.{}'.format(ks_name, table), timeout=120)

    def _modify_table_property(self, name, val, filter_out_table_with_counter=False):
        disruption_name = "".join([p.strip().capitalize() for p in name.split("_")])
        InfoEvent('ModifyTableProperties%s %s' % (disruption_name, self.target_node)).publish()

        ks_cfs = self.cluster.get_non_system_ks_cf_list(
            db_node=self.target_node, filter_out_table_with_counter=filter_out_table_with_counter,
            filter_out_mv=True)  # not allowed to modify MV

        keyspace_table = random.choice(ks_cfs) if ks_cfs else ks_cfs
        if not keyspace_table:
            raise UnsupportedNemesis(
                'Non-system keyspace and table are not found. ModifyTableProperties nemesis can\'t be run')

        cmd = "ALTER TABLE {keyspace_table} WITH {name} = {val};".format(
            keyspace_table=keyspace_table, name=name, val=val)
        self.log.debug('_modify_table_property: %s', cmd)
        with self.cluster.cql_connection_patient(self.target_node) as session:
            session.execute(cmd)

    def _get_all_tables_with_no_compact_storage(self, tables_to_skip=None):
        """
        Return all tables with no "COMPACT STORAGE" in table code
        :param tables_to_skip: Dict of keyspaces/tables to be excluded from results.
            Examples:
                {'ks': 'table'} - will skip ks.table
                {'ks': '*'} - will skip any tables from keyspace "ks"
                {'*': 'table1,table2'} - will skip table1 and table2 from any keyspace
        :return: Dict of with keyspaces as key and list of table names as value
        """
        keyspaces = []
        output = {}
        if tables_to_skip is None:
            tables_to_skip = {}
        to_be_skipped_default = tables_to_skip.get('*', '').split(',')
        with self.cluster.cql_connection_patient(self.tester.db_cluster.nodes[0]) as session:
            query_result = session.execute('SELECT keyspace_name FROM system_schema.keyspaces;')
            for result_rows in query_result:
                keyspaces.extend([row.lower() for row in result_rows if not row.lower().startswith("system")])
            for ks in keyspaces:
                to_be_skipped = tables_to_skip.get(ks, None)
                if to_be_skipped is None:
                    to_be_skipped = to_be_skipped_default
                elif to_be_skipped == '*':
                    continue
                elif to_be_skipped == '':
                    to_be_skipped = []
                else:
                    to_be_skipped = to_be_skipped.split(',') + to_be_skipped_default
                tables = get_db_tables(session, ks, with_compact_storage=False)
                if to_be_skipped:
                    tables = [table for table in tables if table not in to_be_skipped]
                if not tables:
                    continue
                output[ks] = tables
        return output

    def _add_drop_column_get_target_table(self, stored_target_table: list):
        current_tables = self._get_all_tables_with_no_compact_storage(self._add_drop_column_tables_to_ignore)
        if stored_target_table:
            if stored_target_table[1] in current_tables.get(stored_target_table[0], []):
                return stored_target_table
        if not current_tables:
            return None
        ks_name = next(iter(current_tables.keys()))
        table_name = current_tables[ks_name][0]
        return [ks_name, table_name]

    @staticmethod
    def _add_drop_column_get_added_columns_info(target_table: list, added_fields):
        ks = added_fields.get(target_table[0], None)
        if ks is None:
            output = {'column_names': {}, 'column_types': {}}
            added_fields[target_table[0]] = {target_table[1]: output}
            return output
        table = ks.get(target_table[1], None)
        if table is not None:
            return table
        ks[target_table[1]] = output = {'column_names': {}, 'column_types': {}}
        return output

    @staticmethod
    def _random_column_name(avoid_names=None, max_name_size=5):
        if avoid_names is None:
            avoid_names = []
        while True:
            column_name = generate_random_string(max_name_size)
            if column_name not in avoid_names:
                break
        return column_name

    def _add_drop_column_generate_columns_to_drop(self, added_columns_info):  # pylint: disable=too-many-branches
        drop = []
        columns_to_drop = min(len(added_columns_info['column_names']) + 1, self._add_drop_column_max_per_drop + 1)
        if columns_to_drop > 1:
            columns_to_drop = random.randrange(1, columns_to_drop)
        for _ in range(columns_to_drop):
            choice = [n for n in added_columns_info['column_names'] if n not in drop]
            if choice:
                column_name = random.choice(choice)
                drop.append(column_name)
        return drop

    def _add_drop_column_run_cql_query(self, cmd, ks,
                                       consistency_level=ConsistencyLevel.ALL):  # pylint: disable=too-many-branches
        try:
            with self.cluster.cql_connection_patient(self.target_node, keyspace=ks) as session:
                session.default_consistency_level = consistency_level
                session.execute(cmd)
        except Exception as exc:  # pylint: disable=broad-except
            self.log.debug(f"Add/Remove Column Nemesis: CQL query '{cmd}' execution has failed with error '{str(exc)}'")
            return False
        return True

    def _add_drop_column_generate_columns_to_add(self, added_columns_info):
        add = []
        columns_to_add = min(
            self._add_drop_column_max_columns - len(added_columns_info['column_names']),
            self._add_drop_column_max_per_add
        )
        if columns_to_add > 1:
            columns_to_add = random.randrange(1, columns_to_add)
        for _ in range(columns_to_add):
            new_column_name = self._random_column_name(added_columns_info['column_names'].keys(),
                                                       self._add_drop_column_max_column_name_size)
            new_column_type = CQLTypeBuilder.get_random(added_columns_info['column_types'], allow_levels=10,
                                                        avoid_types=['counter'], forget_on_exhaust=True)
            if new_column_type is None:
                continue
            add.append([new_column_name, new_column_type])
        return add

    def _add_drop_column(self, drop=True, add=True):  # pylint: disable=too-many-branches
        self._add_drop_column_target_table = self._add_drop_column_get_target_table(
            self._add_drop_column_target_table)
        if self._add_drop_column_target_table is None:
            return
        added_columns_info = self._add_drop_column_get_added_columns_info(self._add_drop_column_target_table,
                                                                          self._add_drop_column_columns_info)
        added_columns_info.get('column_names', None)
        if not added_columns_info['column_names']:
            drop = False
        if drop:
            drop = self._add_drop_column_generate_columns_to_drop(added_columns_info)
        if add:
            add = self._add_drop_column_generate_columns_to_add(added_columns_info)
        if not add and not drop:
            return
        # TBD: Scylla does not support DROP and ADD in the same statement
        if drop:
            cmd = f"ALTER TABLE {self._add_drop_column_target_table[1]} DROP ( {', '.join(drop)} );"
            if self._add_drop_column_run_cql_query(cmd, self._add_drop_column_target_table[0]):
                for column_name in drop:
                    column_type = added_columns_info['column_names'][column_name]
                    del added_columns_info['column_names'][column_name]
        if add:
            cmd = f"ALTER TABLE {self._add_drop_column_target_table[1]} " \
                  f"ADD ( {', '.join(['%s %s' % (col[0], col[1]) for col in add])} );"
            if self._add_drop_column_run_cql_query(cmd, self._add_drop_column_target_table[0]):
                for column_name, column_type in add:
                    added_columns_info['column_names'][column_name] = column_type
                    column_type.remember_variant(added_columns_info['column_types'])

    def _add_drop_column_run_in_cycle(self):
        start_time = time.time()
        end_time = start_time + 600
        while time.time() < end_time:
            self._add_drop_column()

    def verify_initial_inputs_for_delete_nemesis(self):
        test_keyspaces = self.cluster.get_test_keyspaces()

        if 'scylla_bench' not in test_keyspaces:
            raise UnsupportedNemesis("This nemesis can run on scylla_bench test only")

        max_partitions_in_test_table = self.cluster.params.get('max_partitions_in_test_table')

        if not max_partitions_in_test_table:
            raise UnsupportedNemesis(
                'This nemesis expects mandatory "max_partitions_in_test_table" parameter to be set')

    # pylint: disable=too-many-locals
    def choose_partitions_for_delete(self, partitions_amount, ks_cf, with_clustering_key_data=False,
                                     exclude_partitions=None):
        """
        :type partitions_amount: int
        :type ks_cf: str
        :type with_clustering_key_data: bool
        :type exclude_partitions: list
        :return: defaultdict
        """
        if not exclude_partitions:
            exclude_partitions = []

        max_partitions_in_test_table = self.cluster.params.get('max_partitions_in_test_table')
        partition_range_with_data_validation = self.cluster.params.get('partition_range_with_data_validation')

        if partition_range_with_data_validation and '-' in partition_range_with_data_validation:
            partition_range_splitted = partition_range_with_data_validation.split('-')
            start_range = int(partition_range_splitted[0])
            end_range = int(partition_range_splitted[1])
            exclude_partitions.extend(i for i in range(start_range, end_range))

        partitions_for_delete = defaultdict(list)
        with self.cluster.cql_connection_patient(self.target_node, connect_timeout=300) as session:
            session.default_consistency_level = ConsistencyLevel.ONE

            for partition_key in [i * 2 + 50 for i in range(max_partitions_in_test_table)]:
                if len(partitions_for_delete) == partitions_amount:
                    break

                if exclude_partitions and partition_key in exclude_partitions:
                    continue

                # Get the max cl value in the partition.
                cmd = f"select ck from {ks_cf} where pk={partition_key} order by ck desc limit 1"
                try:
                    result = session.execute(cmd, timeout=300)
                except Exception as exc:  # pylint: disable=broad-except
                    self.log.error(str(exc))
                    continue

                if not result:
                    continue

                if not with_clustering_key_data:
                    partitions_for_delete[partition_key] = []
                    continue

                # Suppose that min ck value is 0 in the partition
                partitions_for_delete[partition_key].extend([0, result[0].ck])

                if None in partitions_for_delete[partition_key]:
                    partitions_for_delete.pop(partition_key)

        self.log.debug(f'Partitions for delete: {partitions_for_delete}')
        return partitions_for_delete

    def run_deletions(self, queries, ks_cf):
        for cmd in queries:
            self.log.debug(f'delete query: {cmd}')
            with self.cluster.cql_connection_patient(self.target_node, connect_timeout=300) as session:
                session.execute(cmd, timeout=3600)

        self.target_node.run_nodetool('flush', args=ks_cf.replace('.', ' '))

    def delete_half_partition(self, ks_cf):
        self.log.debug('Delete by range - half of partition')

        partitions_for_delete = self.choose_partitions_for_delete(10, ks_cf, with_clustering_key_data=True)
        if not partitions_for_delete:
            self.log.error('Not found partitions for delete')
            return partitions_for_delete

        queries = []
        for pkey, ckey in partitions_for_delete.items():
            queries.append(f"delete from {ks_cf} where pk = {pkey} and ck > {int(ckey[1] / 2)}")
        self.run_deletions(queries=queries, ks_cf=ks_cf)

        return partitions_for_delete

    def delete_range_in_few_partitions(self, ks_cf, partitions_for_exclude_dict):
        self.log.debug('Delete same range in the few partitions')

        partitions_for_exclude = list(partitions_for_exclude_dict.keys())
        partitions_for_delete = self.choose_partitions_for_delete(10, ks_cf, with_clustering_key_data=True,
                                                                  exclude_partitions=partitions_for_exclude)
        if not partitions_for_delete:
            self.log.error('Not found partitions for delete')
            return []

        # Choose same "ck" values that exists for all partitions
        # min_clustering_key - the biggest from min(ck) value for all selected partitions
        # max_clustering_key - the smallest from max(ck) value for all selected partitions
        min_clustering_key = max([v[0] for v in partitions_for_delete.values()])
        max_clustering_key = min([v[1] for v in partitions_for_delete.values()])
        clustering_keys = []
        if max_clustering_key > min_clustering_key:
            third_ck = int((max_clustering_key - min_clustering_key) / 3)
            clustering_keys = range(min_clustering_key + third_ck, max_clustering_key - third_ck)

        if not clustering_keys:
            clustering_keys = range(min_clustering_key, max_clustering_key)

        queries = []
        for pkey in partitions_for_delete.keys():
            queries.append(f"delete from {ks_cf} where pk = {pkey} and ck >= {clustering_keys[0]} "
                           f"and ck <= {clustering_keys[-1]}")

        self.run_deletions(queries=queries, ks_cf=ks_cf)

        return list(partitions_for_delete.keys()) + partitions_for_exclude

    def disrupt_delete_10_full_partitions(self):
        """
        Delete few partitions in the table with large partitions
        """
        self.verify_initial_inputs_for_delete_nemesis()

        ks_cf = 'scylla_bench.test'
        partitions_for_delete = self.choose_partitions_for_delete(10, ks_cf)

        if not partitions_for_delete:
            self.log.error('Not found partitions for delete')
            return

        queries = []
        for partition_key in partitions_for_delete.keys():
            queries.append(f"delete from {ks_cf} where pk = {partition_key}")

        self.run_deletions(queries=queries, ks_cf=ks_cf)

    def disrupt_delete_by_rows_range(self):
        """
        Delete few partitions in the table with large partitions
        """
        self.verify_initial_inputs_for_delete_nemesis()

        ks_cf = 'scylla_bench.test'
        partitions_for_exclude = self.delete_half_partition(ks_cf)

        self.delete_range_in_few_partitions(ks_cf, partitions_for_exclude)

    def disrupt_add_drop_column(self):
        """
        It searches for a table that allow add/drop columns (non compact storage table)
        If there is no such table it draw an error and quit
        It keeps tracking what columns where added and never drops column that were added by someone else.
        """
        self.log.debug("AddDropColumnMonkey: Started")
        self._add_drop_column_target_table = self._add_drop_column_get_target_table(
            self._add_drop_column_target_table)
        if self._add_drop_column_target_table is None:
            raise UnsupportedNemesis("AddDropColumnMonkey: can't find table to run on")
        InfoEvent(f'AddDropColumnMonkey table {".".join(self._add_drop_column_target_table)}').publish()
        self._add_drop_column_run_in_cycle()

    def modify_table_comment(self):
        # default: comment = ''
        prop_val = generate_random_string(24)
        self._modify_table_property(name="comment", val=f"'{prop_val}'")

    def modify_table_gc_grace_time(self):
        """
            The number of seconds after data is marked with a tombstone (deletion marker)
            before it is eligible for garbage-collection.
            default: gc_grace_seconds = 864000
        """
        self._modify_table_property(name="gc_grace_seconds", val=random.randint(216000, 864000))

    def modify_table_caching(self):
        """
           Caching optimizes the use of cache memory by a table without manual tuning.
           Cassandra weighs the cached data by size and access frequency.
           default: caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
        """
        prop_val = dict(
            keys=random.choice(["NONE", "ALL"]),
            rows_per_partition=random.choice(["NONE", "ALL", random.randint(1, 10000)])
        )
        self._modify_table_property(name="caching", val=str(prop_val))

    def modify_table_bloom_filter_fp_chance(self):  # pylint: disable=invalid-name
        """
            The Bloom filter sets the false-positive probability for SSTable Bloom filters.
            When a client requests data, Cassandra uses the Bloom filter to check if the row
            exists before doing disk I/O. Bloom filter property value ranges from 0 to 1.0.
            Lower Bloom filter property probabilities result in larger Bloom filters that use more memory.
            The effects of the minimum and maximum values:
                0: Enables the unmodified, effectively the largest possible, Bloom filter.
                1.0: Disables the Bloom filter.
            default: bloom_filter_fp_chance = 0.01
        """
        self._modify_table_property(name="bloom_filter_fp_chance", val=random.random() / 2)

    def toggle_table_ics(self):  # pylint: disable=too-many-locals
        """
            Alters a non-system table compaction strategy from ICS to any-other and vise versa.
        """
        list_additional_params = get_compaction_random_additional_params()
        all_ks_cfs = self.cluster.get_non_system_ks_cf_list(db_node=self.target_node)
        non_mview_ks_cfs = self.cluster.get_non_system_ks_cf_list(db_node=self.target_node, filter_out_mv=True)

        if not all_ks_cfs:
            raise UnsupportedNemesis(
                'Non-system keyspace and table are not found. toggle_tables_ics nemesis can\'t run')

        mview_ks_cfs = list(set(all_ks_cfs) - set(non_mview_ks_cfs))
        keyspace_table = random.choice(all_ks_cfs)
        keyspace, table = keyspace_table.split('.')
        cur_compaction_strategy = get_compaction_strategy(node=self.target_node, keyspace=keyspace,
                                                          table=table)
        if cur_compaction_strategy != CompactionStrategy.INCREMENTAL:
            new_compaction_strategy = CompactionStrategy.INCREMENTAL
        else:
            new_compaction_strategy = random.choice([strategy for strategy in list(
                CompactionStrategy) if strategy != CompactionStrategy.INCREMENTAL])
        new_compaction_strategy_as_dict = {'class': new_compaction_strategy.value}

        if new_compaction_strategy in [CompactionStrategy.INCREMENTAL, CompactionStrategy.SIZE_TIERED]:
            for param in list_additional_params:
                new_compaction_strategy_as_dict.update(param)
        alter_command_prefix = 'ALTER TABLE ' if keyspace_table not in mview_ks_cfs else 'ALTER MATERIALIZED VIEW '
        cmd = alter_command_prefix + \
            " {keyspace_table} WITH compaction = {new_compaction_strategy_as_dict};".format(**locals())
        self.log.debug("Toggle table ICS query to execute: {}".format(cmd))
        try:
            self.target_node.run_cqlsh(cmd)
        except (UnexpectedExit, Libssh2UnexpectedExit) as unexpected_exit:
            if "Unable to find compaction strategy" in str(unexpected_exit):
                err_msg = "for this nemesis to work, you need ICS supported scylla version."
                raise UnsupportedNemesis(err_msg) from unexpected_exit
            raise unexpected_exit

        self.cluster.wait_for_schema_agreement()

    def modify_table_compaction(self):
        """
            The compaction property defines the compaction strategy class for this table.
            default: compaction = {'class': 'SizeTieredCompactionStrategy'}
        """
        # TODO: Sub-properties for each of compaction strategies should also be tested
        strategies = ("SizeTieredCompactionStrategy", "DateTieredCompactionStrategy",
                      "TimeWindowCompactionStrategy", "LeveledCompactionStrategy")
        prop_val = {"class": random.choice(strategies)}
        self._modify_table_property(name="compaction", val=str(prop_val))

    def modify_table_compression(self):
        """
            The compression algorithm. Valid values are LZ4Compressor, SnappyCompressor, DeflateCompressor and
            ZstdCompressor
            default: compression = {}
        """
        algos = ("",  # no compression
                 "LZ4Compressor",
                 "SnappyCompressor",
                 "DeflateCompressor",
                 "ZstdCompressor")
        algo = random.choice(algos)
        prop_val = {"sstable_compression": algo}
        if algo:
            prop_val["chunk_length_kb"] = random.choice(["4K", "64KB", "1M"])
            prop_val["crc_check_chance"] = random.random()
        self._modify_table_property(name="compression", val=str(prop_val))

    def modify_table_crc_check_chance(self):
        """
            default: crc_check_chance = 1.0
        """
        self._modify_table_property(name="crc_check_chance", val=random.random())

    def modify_table_dclocal_read_repair_chance(self):  # pylint: disable=invalid-name
        """
            The probability that a successful read operation triggers a read repair.
            Unlike the repair controlled by read_repair_chance, this repair is limited to
            replicas in the same DC as the coordinator. The value must be between 0 and 1
            default: dclocal_read_repair_chance = 0.1
        """
        self._modify_table_property(name="dclocal_read_repair_chance", val=random.choice([0, 0.2, 0.5, 0.9]))

    def modify_table_default_time_to_live(self):  # pylint: disable=invalid-name
        """
            The value of this property is a number of seconds. If it is set, Cassandra applies a
            default TTL marker to each column in the table, set to this value. When the table TTL
            is exceeded, Cassandra tombstones the table.
            default: default_time_to_live = 0
        """
        # Select table without columns with "counter" type for this nemesis - issue #1037:
        #    Modify_table nemesis chooses first non-system table, and modify default_time_to_live of it.
        #    But table with counters doesn't support this
        self._modify_table_property(name="default_time_to_live", val=random.randint(864000, 630720000),
                                    filter_out_table_with_counter=True)  # max allowed TTL - 20 years (630720000)

    def modify_table_max_index_interval(self):
        """
            If the total memory usage of all index summaries reaches this value, Cassandra decreases
            the index summaries for the coldest SSTables to the maximum set by max_index_interval.
            The max_index_interval is the sparsest possible sampling in relation to memory pressure.
            default: max_index_interval = 2048
        """
        self._modify_table_property(name="max_index_interval", val=random.choice([1024, 4096, 8192]))

    def modify_table_min_index_interval(self):
        """
            The minimum gap between index entries in the index summary. A lower min_index_interval
            means the index summary contains more entries from the index, which allows Cassandra
            to search fewer index entries to execute a read. A larger index summary may also use
            more memory. The value for min_index_interval is the densest possible sampling of the index.
            default: min_index_interval = 128
        """
        self._modify_table_property(name="min_index_interval", val=random.choice([128, 256, 512]))

    def modify_table_memtable_flush_period_in_ms(self):  # pylint: disable=invalid-name
        """
            The number of milliseconds before Cassandra flushes memtables associated with this table.
            default: memtable_flush_period_in_ms = 0
        """
        self._modify_table_property(name="memtable_flush_period_in_ms", val=random.randint(1, 5000))

    def modify_table_read_repair_chance(self):
        """
            The probability that a successful read operation will trigger a read repair.of read repairs
            being invoked. Unlike the repair controlled by dc_local_read_repair_chance, this repair is
            not limited to replicas in the same DC as the coordinator. The value must be between 0 and 1
            default: read_repair_chance = 0.0
        """
        self._modify_table_property(name="read_repair_chance", val=random.choice([0, 0.2, 0.5, 0.9]))

    def modify_table_speculative_retry(self):
        """
            Use the speculative retry property to configure rapid read protection. In a normal read,
            Cassandra sends data requests to just enough replica nodes to satisfy the consistency
            level. In rapid read protection, Cassandra sends out extra read requests to other replicas,
            even after the consistency level has been met. The speculative retry property specifies
            the trigger for these extra read requests.
                ALWAYS: Send extra read requests to all other replicas after every read.
                Xpercentile: Cassandra constantly tracks each table's typical read latency (in milliseconds).
                             If you set speculative retry to Xpercentile, Cassandra sends redundant read
                             requests if the coordinator has not received a response after X percent of the
                             table's typical latency time.
                Nms: Send extra read requests to all other replicas if the coordinator node has not received
                     any responses within N milliseconds.
                NONE: Do not send extra read requests after any read.
            default: speculative_retry = '99.0PERCENTILE';
        """
        options = ("'ALWAYS'",
                   "'%spercentile'" % random.randint(1, 99),
                   "'%sms'" % random.randint(1, 1000))
        self._modify_table_property(name="speculative_retry", val=random.choice(options))

    def disrupt_toggle_table_ics(self):
        self.toggle_table_ics()

    def disrupt_modify_table(self):
        # randomly select and run one of disrupt_modify_table* methods
        disrupt_func_name = random.choice([dm for dm in dir(self) if dm.startswith("modify_table")])
        disrupt_func = getattr(self, disrupt_func_name)
        disrupt_func()

    def disrupt_mgmt_backup_specific_keyspaces(self):
        self._mgmt_backup(backup_specific_tables=True)

    def disrupt_mgmt_backup(self):
        self._mgmt_backup(backup_specific_tables=False)

    def _mgmt_backup(self, backup_specific_tables):
        if not self.cluster.params.get('use_mgmt') and not self.cluster.params.get('use_cloud_manager'):
            raise UnsupportedNemesis('Scylla-manager configuration is not defined!')
        mgr_cluster = self.cluster.get_cluster_manager()
        if self.cluster.params.get('use_cloud_manager'):
            auto_backup_task = mgr_cluster.backup_task_list[0]
            #  An example of the auto generated backup task of cloud manager is:
            #  │ backup/8e40b8b4-5394-42d3-9884-a4ce8ab69687
            #  │ --dc 'AWS_US_EAST_1' -L AWS_US_EAST_1:s3:scylla-cloud-backup-9952-10120-4q4w4d --retention 14
            #  --rate-limit AWS_US_EAST_1:100 --snapshot-parallel '<nil>' --upload-parallel '<nil>'
            #  │ 06 Jun 21 18:10:05 UTC (+1d)  │ NEW
            location = auto_backup_task.arguments.split('-L')[1].split()[0]
        else:
            if not self.cluster.params.get('backup_bucket_location'):
                raise UnsupportedNemesis('backup bucket location configuration is not defined!')

            backup_bucket_backend = self.cluster.params.get("backup_bucket_backend")
            backup_bucket_location = self.cluster.params.get("backup_bucket_location")
            location = f"{backup_bucket_backend}:{backup_bucket_location.split()[0]}"
        if backup_specific_tables:
            non_test_keyspaces = self.cluster.get_test_keyspaces()
            mgr_task = mgr_cluster.create_backup_task(location_list=[location, ], keyspace_list=non_test_keyspaces)
        else:
            mgr_task = mgr_cluster.create_backup_task(location_list=[location, ])

        assert mgr_task is not None, "Backup task wasn't created"

        status = mgr_task.wait_and_get_final_status(timeout=54000, step=5, only_final=True)
        if status == TaskStatus.DONE:
            self.log.info("Task: %s is done.", mgr_task.id)
        elif status in (TaskStatus.ERROR, TaskStatus.ERROR_FINAL):
            assert False, f'Backup task {mgr_task.id} failed'
        else:
            mgr_task.stop()
            assert False, f'Backup task {mgr_task.id} timed out - while on status {status}'

    @latency_calculator_decorator
    def disrupt_mgmt_repair_cli(self):
        if not self.cluster.params.get('use_mgmt') and not self.cluster.params.get('use_cloud_manager'):
            raise UnsupportedNemesis('Scylla-manager configuration is not defined!')
        mgr_cluster = self.cluster.get_cluster_manager()
        mgr_task = mgr_cluster.create_repair_task()
        task_final_status = mgr_task.wait_and_get_final_status(timeout=86400)  # timeout is 24 hours
        assert task_final_status == TaskStatus.DONE, 'Task: {} final status is: {}.'.format(
            mgr_task.id, str(mgr_task.status))
        self.log.info('Task: {} is done.'.format(mgr_task.id))

    def disrupt_abort_repair(self):
        """
        Start repair target_node in background, then try to abort the repair streaming.
        """
        self.log.debug("Start repair target_node in background")

        @raise_event_on_failure
        def silenced_nodetool_repair_to_fail():
            try:
                self.target_node.run_nodetool("repair", verbose=False,
                                              warning_event_on_exception=(UnexpectedExit, Libssh2UnexpectedExit),
                                              error_message="Repair failed as expected. ",
                                              publish_event=False)
            except (UnexpectedExit, Libssh2UnexpectedExit):
                self.log.info('Repair failed as expected')
            except Exception:
                self.log.error('Repair failed due to the unknown error')
                raise

        def repair_streaming_exists():
            active_repair_cmd = 'curl -s -X GET --header "Content-Type: application/json" --header ' \
                                '"Accept: application/json" "http://127.0.0.1:10000/storage_service/active_repair/"'
            result = self.target_node.remoter.run(active_repair_cmd)
            active_repairs = re.match(r".*\[(\d)+\].*", result.stdout)
            if active_repairs:
                self.log.debug("Found '%s' active repairs", active_repairs.group(1))
                return True
            return False

        with ThreadPoolExecutor(max_workers=1, thread_name_prefix='NodeToolRepairThread') as thread_pool:
            thread = thread_pool.submit(silenced_nodetool_repair_to_fail)
            wait.wait_for(func=repair_streaming_exists,
                          timeout=300,
                          step=1,
                          throw_exc=True,
                          text='Wait for repair starts')

            self.log.debug("Abort repair streaming by storage_service/force_terminate_repair API")

            with DbEventsFilter(db_event=DatabaseLogEvent.DATABASE_ERROR,
                                line="repair's stream failed: streaming::stream_exception",
                                node=self.target_node), \
                    DbEventsFilter(db_event=DatabaseLogEvent.RUNTIME_ERROR,
                                   line="Can not find stream_manager",
                                   node=self.target_node), \
                    DbEventsFilter(db_event=DatabaseLogEvent.RUNTIME_ERROR,
                                   line="is aborted",
                                   node=self.target_node), \
                    DbEventsFilter(db_event=DatabaseLogEvent.RUNTIME_ERROR,
                                   line="Failed to repair",
                                   node=self.target_node):
                self.target_node.remoter.run(
                    "curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json'"
                    " http://127.0.0.1:10000/storage_service/force_terminate_repair"
                )
                thread.result(timeout=120)
                time.sleep(10)  # to make sure all failed logs/events, are ignored correctly

        self.log.debug("Execute a complete repair for target node")
        self.repair_nodetool_repair()

    # NOTE: enable back when 'https://github.com/scylladb/scylla/issues/8136' issue is fixed
    def disable_disrupt_validate_hh_short_downtime(self):  # pylint: disable=invalid-name
        """
            Validates that hinted handoff mechanism works: there were no drops and errors
            during short stop of one of the nodes in cluster
        """
        if self.cluster.params.get('hinted_handoff') == 'disabled':
            raise UnsupportedNemesis('For this nemesis to work, `hinted_handoff` needs to be set to `enabled`')

        start_time = time.time()
        self.target_node.stop_scylla()
        time.sleep(10)
        self.target_node.start_scylla()

        # Wait until all other nodes see the target node as UN
        # Only then we can expect that hint sending started on all nodes
        def target_node_reported_un_by_others():
            for node in self.cluster.nodes:
                if node is not self.target_node:
                    self.cluster.check_nodes_up_and_normal(nodes=[self.target_node], verification_node=node)
            return True

        wait.wait_for(func=target_node_reported_un_by_others,
                      timeout=300,
                      step=5,
                      throw_exc=True,
                      text='Wait for target_node to be seen as UN by others')

        time.sleep(120)  # Wait to complete hints sending
        assert self.tester.hints_sending_in_progress() is False, "Hints are sent too slow"
        self.tester.verify_no_drops_and_errors(starting_from=start_time)

    def _validate_snapshot(self, nodetool_cmd: str, snapshot_content: namedtuple):
        """
            The snapshot may be taken for a few options:
            - for all keyspaces (with all their tables) - nodetool snapshot cmd without parameters:
                nodetool snapshot
            - for one kespace (with all its tables) - nodetool snapshot cmd with "-ks" parameter:
                nodetool snapshot -ks system
            - for a few keyspaces (with all their tables) - nodetool snapshot cmd with "-ks" parameter:
                nodetool snapshot -ks system, system_schema
            - for one kespace and few tables - nodetool snapshot cmd with "-cf" parameter like:
                nodetool snapshot test -cf cf1,cf2

            By parsing of nodetool_cmd is recognized with type of snapshot was created.
            This function check if all expected keyspace/tables are in the snapshot
        """
        snapshot_params = nodetool_cmd.split()
        ks_cf = self.cluster.get_any_ks_cf_list(db_node=self.target_node, filter_empty_tables=False)
        keyspace_table = []
        if len(snapshot_params) > 1:
            if snapshot_params[1] == "-kc":
                for ks in snapshot_params[2].split(','):
                    keyspace_table.extend([k_c.split('.') for k_c in ks_cf if k_c.startswith(f"{ks}.")])
            else:
                keyspace = snapshot_params[1]
                keyspace_table.extend([[keyspace, cf] for cf in snapshot_params[3].split(',')])
        else:
            keyspace_table.extend([k_c.split('.') for k_c in ks_cf])

        snapshot_content_list = [[elem.keyspace_name, elem.table_name] for elem in snapshot_content]
        if sorted(keyspace_table) != sorted(snapshot_content_list):
            raise AssertionError(f"Snapshot content not as expected. \n"
                                 f"Expected content: {sorted(keyspace_table)} \n "
                                 f"Actual snapshot content: {sorted(snapshot_content_list)}")

    def disrupt_snapshot_operations(self):  # pylint: disable=too-many-statements
        """
        Extend this nemesis to run 'nodetool snapshot' more options including multiple tables.
        Random choose between:
        - create snapshot of all keyspaces
        - create snapshot of one keyspace
        - create snapshot of few keyspaces
        - create snapshot of few tables, including MVs
        """

        def _full_snapshot():
            self.log.info("Take all keyspaces snapshot")
            return 'snapshot'

        def _ks_snapshot(one_ks):
            self.log.info(f"Take {'one keyspace' if one_ks else 'few keyspaces'} snapshot")
            # Prefer to take snapshot of test keyspace. Try to find it
            ks_cf = self.cluster.get_non_system_ks_cf_list(db_node=self.target_node)
            if not ks_cf:
                # If test table wasn't found - take system keyspace snapshot
                ks_cf = self.cluster.get_any_ks_cf_list(db_node=self.target_node)

            if one_ks:
                keyspace_table = random.choice(ks_cf)
                keyspace, _ = keyspace_table.split('.')
            else:
                keyspaces = {ks.split('.')[0] for ks in ks_cf}
                if len(keyspaces) == 1:
                    ks_cf = self.cluster.get_any_ks_cf_list(db_node=self.target_node)
                    keyspaces = {ks.split('.')[0] for ks in ks_cf}
                keyspace = ','.join(keyspaces)

            if not keyspace:
                # it's unexpected situation. We have system keyspaces, so the keyspace(s) should be found always.
                # If not - raise exception to investigate the issue
                raise Exception(f'Something wrong happened, not empty keyspace(s) was not'
                                f'found in the list:\n{ks_cf}')

            return f'snapshot -kc {keyspace}'

        def _few_tables():
            def get_ks_with_few_tables(keyspace_table):
                ks_occurrences = Counter([ks.split('.')[0] for ks in keyspace_table])
                repeated_ks = [ks for ks, count in ks_occurrences.items() if count > 1]
                return repeated_ks

            self.log.info("Take few tables snapshot")
            # Prefer to take snapshot of test table. Try to find it
            ks_cf = self.cluster.get_non_system_ks_cf_list(db_node=self.target_node)

            if not ks_cf or len(ks_cf) == 1:
                # If test table wasn't found - take system table snapshot
                ks_cf = self.cluster.get_any_ks_cf_list(db_node=self.target_node)

            ks_with_few_tables = get_ks_with_few_tables(ks_cf)
            if not ks_with_few_tables:
                # If non-system keyspace with few tables wasn't found - take system table snapshot
                ks_cf = self.cluster.get_any_ks_cf_list(db_node=self.target_node)
                ks_with_few_tables = get_ks_with_few_tables(ks_cf)

            if not ks_with_few_tables:
                self.log.warning('Keyspace with few tables has not been found')
                return _full_snapshot()

            selected_keyspace = random.choice(ks_with_few_tables)
            tables = ','.join([k_c.split('.')[1] for k_c in ks_cf if k_c.startswith(f"{selected_keyspace}.")])

            if not tables:
                # it's unexpected situation. We have system keyspaces with few tables, so the keyspaces should be found.
                # If not - raise exception to investigate the issue
                raise Exception(f'Something wrong happened, tables for "{selected_keyspace}" keyspace were not'
                                f'found in the list:\n{ks_cf}')

            return f'snapshot {selected_keyspace} -cf {tables}'

        functions_map = [(_few_tables,), (_full_snapshot,), (_ks_snapshot, True), (_ks_snapshot, False)]
        snapshot_option = random.choice(functions_map)

        try:
            nodetool_cmd = snapshot_option[0]() if len(snapshot_option) == 1 else snapshot_option[0](snapshot_option[1])
            if not nodetool_cmd:
                raise ValueError("Failed to get nodetool command.")
        except Exception as exc:  # pylint: disable=broad-except
            raise ValueError(f"Failed to get nodetool command. Error: {exc}") from exc

        self.log.debug(f'Take snapshot with command: {nodetool_cmd}')
        result = self.target_node.run_nodetool(nodetool_cmd)
        self.log.debug(result)
        if "snapshot name" not in result.stdout:
            raise Exception(f"Snapshot name wasn't found in {nodetool_cmd} output:\n{result.stdout}")

        snapshot_name = re.findall(r'(\d+)', result.stdout.split("snapshot name")[1])[0]
        result = self.target_node.run_nodetool('listsnapshots')
        self.log.debug(result)
        if snapshot_name in result.stdout:
            self.log.info('Snapshot %s created' % snapshot_name)
            snapshots_content = parse_nodetool_listsnapshots(listsnapshots_output=result.stdout)
            snapshot_content = snapshots_content.get(snapshot_name)
            self._validate_snapshot(nodetool_cmd=nodetool_cmd, snapshot_content=snapshot_content)
            self.log.info('Snapshot %s validated successfully' % snapshot_name)
        else:
            raise Exception(f"Snapshot {snapshot_name} wasn't found in: \n{result.stdout}")

        result = self.target_node.run_nodetool('clearsnapshot')
        self.log.debug(result)

    # NOTE: '2023.1.rc0' is set in advance, not guaranteed to match when appears
    @scylla_versions(("4.6.rc0", None), ("2023.1.rc0", None))
    def disrupt_show_toppartitions(self):
        # NOTE: new API is supported only starting with Scylla 4.6
        #       In Scylla 4.5 it exists but disabled.
        return self._disrupt_show_toppartitions(allow_new_api=True)

    @scylla_versions(("4.3.rc1", "4.5"), ("2020.1.rc0", "2022.1"))
    def disrupt_show_toppartitions(self):  # pylint: disable=function-redefined
        return self._disrupt_show_toppartitions(allow_new_api=False)

    def _disrupt_show_toppartitions(self, allow_new_api: bool):
        self.log.debug(
            "Running 'disrupt_show_toppartitions' method using %s API.",
            "new and old" if allow_new_api else "old")
        result = self.target_node.run_nodetool(sub_cmd='help', args='toppartitions')
        if 'Unknown command toppartitions' in result.stdout:
            raise UnsupportedNemesis("nodetool doesn't support toppartitions")
        ks_cf_list = self.cluster.get_any_ks_cf_list(self.target_node)
        if not ks_cf_list:
            raise UnsupportedNemesis('User-defined Keyspace and ColumnFamily are not found.')

        top_partition_api_cmds = [OldApiTopPartitionCmd]
        if allow_new_api:
            top_partition_api_cmds.append(NewApiTopPartitionCmd)
        for top_partition_api_cmd in top_partition_api_cmds:
            top_partition_api = top_partition_api_cmd(ks_cf_list)
            # workaround for issue #4519
            self.target_node.run_nodetool('cfstats')
            top_partition_api.generate_cmd_arg_values()
            result = self.target_node.run_nodetool(
                sub_cmd='toppartitions', args=top_partition_api.get_cmd_args())
            top_partition_api.verify_output(result.stdout)

    def get_rate_limit_for_network_disruption(self) -> Optional[str]:
        if not self.monitoring_set.nodes:
            return None

        # get the last 10min avg network bandwidth used, and limit  30% to 70% of it
        prometheus_stats = PrometheusDBStats(host=self.monitoring_set.nodes[0].external_address)
        query = 'avg(node_network_receive_bytes_total{instance=~".*?%s.*?", device="eth0"})' % \
                self.target_node.ip_address
        now = time.time()
        results = prometheus_stats.query(query=query, start=now - 600, end=now)
        assert results, "no results for node_network_receive_bytes_total metric in Prometheus "
        avg_bitrate_per_node = max([float(avg_rate) for _, avg_rate in results[0]["values"]])
        avg_mpbs_per_node = avg_bitrate_per_node / 1024 / 1024

        if avg_mpbs_per_node > 10:
            min_limit = int(round(avg_mpbs_per_node * 0.30))
            max_limit = int(round(avg_mpbs_per_node * 0.70))
            rate_limit_suffix = "mbps"
        else:
            avg_kbps_per_node = avg_bitrate_per_node / 1024
            min_limit = int(round(avg_kbps_per_node * 0.30))
            max_limit = int(round(avg_kbps_per_node * 0.70))
            rate_limit_suffix = "kbps"

        return "{}{}".format(random.randrange(min_limit, max_limit), rate_limit_suffix)

    def disrupt_network_random_interruptions(self):  # pylint: disable=invalid-name
        # pylint: disable=too-many-locals
        if not self.cluster.extra_network_interface:
            raise UnsupportedNemesis("for this nemesis to work, you need to set `extra_network_interface: True`")

        if not self.target_node.install_traffic_control():
            raise UnsupportedNemesis("Traffic control package not installed on system")

        rate_limit: Optional[str] = self.get_rate_limit_for_network_disruption()
        if not rate_limit:
            self.log.warning("NetworkRandomInterruption won't limit network bandwidth due to lack of monitoring nodes.")

        # random packet loss - between 1% - 15%
        loss_percentage = random.randrange(1, 15)

        # random packet corruption - between 1% - 15%
        corrupt_percentage = random.randrange(1, 15)

        # random packet delay - between 1s - 30s
        delay_in_secs = random.randrange(1, 30)

        list_of_tc_options = [
            ("NetworkRandomInterruption_{}pct_loss".format(loss_percentage), "--loss {}%".format(loss_percentage)),
            ("NetworkRandomInterruption_{}pct_corrupt".format(corrupt_percentage),
             "--corrupt {}%".format(corrupt_percentage)),
            ("NetworkRandomInterruption_{}sec_delay".format(delay_in_secs),
             "--delay {}s --delay-distro 500ms".format(delay_in_secs))]
        if rate_limit:
            list_of_tc_options.append(
                ("NetworkRandomInterruption_{}_limit".format(rate_limit), "--rate {}".format(rate_limit)))

        list_of_timeout_options = [10, 60, 120, 300, 500]
        option_name, selected_option = random.choice(list_of_tc_options)
        wait_time = random.choice(list_of_timeout_options)

        InfoEvent(option_name).publish()
        self.log.debug("NetworkRandomInterruption: [%s] for %dsec", selected_option, wait_time)
        self.target_node.traffic_control(None)
        try:
            self.target_node.traffic_control(selected_option)
            time.sleep(wait_time)
        finally:
            self.target_node.traffic_control(None)
            self._wait_all_nodes_un()

    def disrupt_network_block(self):
        if not self.cluster.extra_network_interface:
            raise UnsupportedNemesis("for this nemesis to work, you need to set `extra_network_interface: True`")

        if not self.target_node.install_traffic_control():
            raise UnsupportedNemesis("Traffic control package not installed on system")

        selected_option = "--loss 100%"
        list_of_timeout_options = [10, 60, 120, 300, 500]
        wait_time = random.choice(list_of_timeout_options)
        self.log.debug("BlockNetwork: [%s] for %dsec", selected_option, wait_time)
        self.target_node.traffic_control(None)
        try:
            self.target_node.traffic_control(selected_option)
            time.sleep(wait_time)
        finally:
            self.target_node.traffic_control(None)
            self._wait_all_nodes_un()

    @retrying(n=15, sleep_time=5, allowed_exceptions=ClusterNodesNotReady)
    def _wait_all_nodes_un(self):
        for node in self.cluster.nodes:
            self.cluster.check_nodes_up_and_normal(verification_node=node)

    def disrupt_remove_node_then_add_node(self):  # pylint: disable=too-many-branches
        """
        https://docs.scylladb.com/operating-scylla/procedures/cluster-management/remove_node/

        1. Terminate node
        2. Run full repair
        3. Nodetool removenode
        4. Add new node
        5. Run nodetool cleanup (on each node) for each keyspace
        """
        if self.cluster.params.get("db_type") == 'cloud_scylla':
            raise UnsupportedNemesis("Skipping this nemesis due this job run from Siren cloud with 2019 version!")
        if self._is_it_on_kubernetes():
            raise UnsupportedNemesis("On K8S nodes get removed differently. Skipping.")

        node_to_remove = self.target_node
        up_normal_nodes = self.cluster.get_nodes_up_and_normal(verification_node=node_to_remove)
        # node_to_remove must be different than node
        verification_node = random.choice([n for n in self.cluster.nodes if n is not node_to_remove])

        # node_to_remove is single/last seed in cluster, before
        # it will be terminated, choose new seed node
        num_of_seed_nodes = len(self.cluster.seed_nodes)
        if node_to_remove.is_seed and num_of_seed_nodes < 2:
            new_seed_node = random.choice([n for n in self.cluster.nodes if n is not node_to_remove])
            new_seed_node.set_seed_flag(True)
            self.cluster.update_seed_provider()

        # get node's host_id
        removed_node_status = self.cluster.get_node_status_dictionary(
            ip_address=node_to_remove.ip_address, verification_node=verification_node)
        assert removed_node_status is not None, "failed to get host_id using nodetool status"
        host_id = removed_node_status["host_id"]

        # node stop and make sure its "DN"
        node_to_remove.stop_scylla_server(verify_up=True, verify_down=True)

        # terminate node
        self._terminate_cluster_node(node_to_remove)

        def remove_node():
            # nodetool removenode 'host_id'
            rnd_node = random.choice([n for n in self.cluster.nodes if n is not self.target_node])
            self.log.info("Running removenode command on {}, Removing node with the following host_id: {}"
                          .format(rnd_node.ip_address, host_id))
            res = rnd_node.run_nodetool("removenode {}".format(host_id), ignore_status=True, verbose=True)
            return res.exit_status

        # full cluster repair
        up_normal_nodes.remove(node_to_remove)
        # Repairing will result in a best effort repair due to the terminated node,
        # and as a result requires ignoring repair errors
        with DbEventsFilter(db_event=DatabaseLogEvent.RUNTIME_ERROR,
                            line="failed to repair"):
            for node in up_normal_nodes:
                try:
                    self.repair_nodetool_repair(node=node, publish_event=False)
                except Exception as details:  # pylint: disable=broad-except
                    self.log.error(f"failed to execute repair command "
                                   f"on node {node} due to the following error: {str(details)}")

            # WORKAROUND: adding here the continuation of the nemesis to avoid the late filter messages above failing
            # the entire nemesis.

            exit_status = remove_node()

            # if remove node command failed by any reason,
            # we will remove the terminated node from
            # dead_nodes_list, so the health validator terminate the job
            if exit_status != 0:
                self.log.error(f"nodetool removenode command exited with status {exit_status}")
                self.log.debug(
                    f"Remove failed node {node_to_remove} from dead node list {self.cluster.dead_nodes_list}")
                node = next((n for n in self.cluster.dead_nodes_list if n.ip_address == node_to_remove.ip_address),
                            None)
                if node:
                    self.cluster.dead_nodes_list.remove(node)
                else:
                    self.log.debug(f"Node {node.name} with ip {node.ip_address} was not found in dead_nodes_list")

            # verify node is removed by nodetool status
            removed_node_status = self.cluster.get_node_status_dictionary(
                ip_address=node_to_remove.ip_address, verification_node=verification_node)
            assert removed_node_status is None, \
                "Node was not removed properly (Node status:{})".format(removed_node_status)

            # add new node
            new_node = self._add_and_init_new_cluster_node(rack=self.target_node.rack)
            # in case the removed node was not last seed.
            if node_to_remove.is_seed and num_of_seed_nodes > 1:
                new_node.set_seed_flag(True)
                self.cluster.update_seed_provider()
            # after add_node, the left nodes have data that isn't part of their tokens anymore.
            # In order to eliminate cases that we miss a "data loss" bug because of it, we cleanup this data.
            # This fix important when just user profile is run in the test and "keyspace1" doesn't exist.
            try:
                test_keyspaces = self.cluster.get_test_keyspaces()
                for node in self.cluster.nodes:
                    for keyspace in test_keyspaces:
                        node.run_nodetool(sub_cmd='cleanup', args=keyspace)
            finally:
                self.unset_current_running_nemesis(new_node)

    # Temporary disable due to  https://github.com/scylladb/scylla/issues/6522
    def _disrupt_network_reject_inter_node_communication(self):
        """
        Generates random firewall rule to drop/reject packets for inter-node communications, port 7000 and 7001
        """
        name = 'RejectInterNodeNetwork'
        textual_matching_rule, matching_rule = self._iptables_randomly_get_random_matching_rule()
        textual_pkt_action, pkt_action = self._iptables_randomly_get_disrupting_target()
        wait_time = random.choice([10, 60, 120, 300, 500])
        InfoEvent(f'{name} {textual_matching_rule} that belongs to '
                  'inter node communication connections (port=7000 and 7001) will be'
                  f' {textual_pkt_action} for {wait_time}s').publish()

        # because of https://github.com/scylladb/scylla/issues/5802, we ignore YCSB client errors here
        with ignore_alternator_client_errors():
            return self._run_commands_wait_and_cleanup(
                self.target_node,
                name=name,
                start_commands=[
                    f'sudo iptables -t filter -A INPUT -p tcp --dport 7000 {matching_rule} -j {pkt_action}',
                    f'sudo iptables -t filter -A INPUT -p tcp --dport 7001 {matching_rule} -j {pkt_action}'
                ],
                cleanup_commands=[
                    f'sudo iptables -t filter -D INPUT -p tcp --dport 7000 {matching_rule} -j {pkt_action}',
                    f'sudo iptables -t filter -D INPUT -p tcp --dport 7001 {matching_rule} -j {pkt_action}'
                ],
                wait_time=wait_time
            )

    def disrupt_network_reject_node_exporter(self):
        """
        Generates random firewall rule to drop/reject packets for node exporter connections, port 9100
        """
        if self._is_it_on_kubernetes():
            raise UnsupportedNemesis("Not implemented for the K8S backend.")
        name = 'RejectNodeExporterNetwork'
        textual_matching_rule, matching_rule = self._iptables_randomly_get_random_matching_rule()
        textual_pkt_action, pkt_action = self._iptables_randomly_get_disrupting_target()
        wait_time = random.choice([10, 60, 120, 300, 500])
        InfoEvent(f'{name} {textual_matching_rule} that belongs to '
                  f'node-exporter(port=9100) connections will be {textual_pkt_action} for {wait_time}s').publish()
        return self._run_commands_wait_and_cleanup(
            self.target_node,
            name=name,
            start_commands=[f'sudo iptables -t filter -A INPUT -p tcp --dport 9100 {matching_rule} -j {pkt_action}'],
            cleanup_commands=[f'sudo iptables -t filter -D INPUT -p tcp --dport 9100 {matching_rule} -j {pkt_action}'],
            wait_time=wait_time
        )

    def disrupt_network_reject_thrift(self):
        """
        Generates random firewall rule to drop/reject packets for thrift connections, port 9100
        """
        if self._is_it_on_kubernetes():
            raise UnsupportedNemesis("Not implemented for the K8S backend.")
        name = 'RejectThriftNetwork'
        textual_matching_rule, matching_rule = self._iptables_randomly_get_random_matching_rule()
        textual_pkt_action, pkt_action = self._iptables_randomly_get_disrupting_target()
        wait_time = random.choice([10, 60, 120, 300, 500])
        InfoEvent(f'{name} {textual_matching_rule} that belongs to '
                  f'Thrift(port=9160) connections will be {textual_pkt_action} for {wait_time}s').publish()
        return self._run_commands_wait_and_cleanup(
            self.target_node,
            name=name,
            start_commands=[f'sudo iptables -t filter -A INPUT -p tcp --dport 9160 {matching_rule} -j {pkt_action}'],
            cleanup_commands=[f'sudo iptables -t filter -D INPUT -p tcp --dport 9160 {matching_rule} -j {pkt_action}'],
            wait_time=wait_time
        )

    @staticmethod
    def _iptables_randomly_get_random_matching_rule():
        """
        Randomly generates iptables matching rule to match packets in random manner
        """
        match_type = random.choice(
            ['statistic', 'statistic', 'statistic', 'limit', 'limit', 'limit', '']
            #  Make no matching rule less probable
        )

        if match_type == 'statistic':
            mode = random.choice(['random', 'nth'])
            if match_type == 'random':
                probability = random.choice(['0.0001', '0.001', '0.01', '0.1', '0.3', '0.6', '0.8', '0.9'])
                return f'randomly chosen packet with {probability} probability', \
                       f'-m statistic --mode {mode} --probability {probability}'
            elif match_type == 'nth':
                every = random.choice(['2', '4', '8', '16', '32', '64', '128'])
                return f'every {every} packet', \
                       f'-m statistic --mode {mode} --every {every} --packet 0'
        elif match_type == 'limit':
            period = random.choice(['second', 'minute'])
            pkts_per_period = random.choice({
                'second': [1, 5, 10],
                'minute': [2, 10, 40, 80]
            }.get(period))
            return f'string of {pkts_per_period} very first packets every {period}', \
                   f'-m limit --limit {pkts_per_period}/{period}'
        elif match_type == 'connbytes':
            bytes_from = random.choice(['100', '200', '400', '800', '1600', '3200', '6400', '12800', '1280000'])
            return f'every packet from connection that total byte counter exceeds {bytes_from}', \
                   f'-m connbytes --connbytes-mode bytes --connbytes-dir both --connbytes {bytes_from}'
        return 'every packet', ''

    @staticmethod
    def _iptables_randomly_get_disrupting_target():
        """
        Randomly generates iptables target that can cause disruption
        """
        target_type = random.choice(['REJECT', 'DROP'])
        if target_type == 'REJECT':
            reject_with = random.choice([
                'icmp-net-unreachable',
                'icmp-host-unreachable',
                'icmp-port-unreachable',
                'icmp-proto-unreachable',
                'icmp-net-prohibited',
                'icmp-host-prohibited',
                'icmp-admin-prohibited'
            ])
            return f'rejected with {reject_with}', \
                   f'{target_type} --reject-with {reject_with}'
        return 'dropped', f'{target_type}'

    def _run_commands_wait_and_cleanup(  # pylint: disable=too-many-arguments
            self, node, name: str, start_commands: List[str],
            cleanup_commands: List[str] = None, wait_time: int = 0):
        """
        Runs command/commands on target node wait and run cleanup commands
            :param node: target node
            :param name: Name of Nemesis for logging
            :param start_commands: commands to run on start
            :param cleanup_commands: commands to run to cleanup
            :param wait_time: waiting time
        """
        cmd_executed = {}
        if cleanup_commands is None:
            cleanup_commands = []
        for cmd_num, cmd in enumerate(start_commands):
            try:
                node.remoter.run(cmd)
                self.log.debug(f"{name}: executed: {cmd}")
                cmd_executed[cmd_num] = True
                if wait_time:
                    time.sleep(wait_time)
            except Exception as exc:  # pylint: disable=broad-except
                cmd_executed[cmd_num] = False
                self.log.error(
                    f"{name}: failed to execute start command "
                    f"{cmd} on node {node} due to the following error: {str(exc)}")
        if not cmd_executed:
            return
        for cmd_num, cmd in enumerate(cleanup_commands):
            try:
                node.remoter.run(cmd)
            except Exception as exc:  # pylint: disable=broad-except
                self.log.debug(f"{name}: failed to execute cleanup command "
                               f"{cmd} on node {node} due to the following error: {str(exc)}")

    def disrupt_network_start_stop_interface(self):  # pylint: disable=invalid-name
        if not self.cluster.extra_network_interface:
            raise UnsupportedNemesis("for this nemesis to work, you need to set `extra_network_interface: True`")

        list_of_timeout_options = [10, 60, 120, 300, 500]
        wait_time = random.choice(list_of_timeout_options)
        self.log.debug("Taking down eth1 for %dsec", wait_time)

        try:
            self.target_node.stop_network_interface()
            time.sleep(wait_time)
        finally:
            self.target_node.start_network_interface()
            self._wait_all_nodes_un()

    def break_streaming_task_and_rebuild(self, task='decommission'):  # pylint: disable=too-many-statements
        """
        Stop streaming task in middle and rebuild the data on the node.
        """

        def decommission_post_action():
            target_is_seed = self.target_node.is_seed
            try:
                self.cluster.verify_decommission(self.target_node)
            except NodeStayInClusterAfterDecommission:
                self.log.debug('The decommission of target node is successfully interrupted')
                return None
            except Exception as exc:  # pylint: disable=broad-except
                self.log.error('Unexpected exception raised in checking decommission status: %s', exc)

            self.log.info('Decommission might complete before stopping it. Re-add a new node')
            new_node = self._add_and_init_new_cluster_node(rack=self.target_node.rack)
            if new_node.is_seed != target_is_seed:
                new_node.set_seed_flag(target_is_seed)
                self.cluster.update_seed_provider()
            self.unset_current_running_nemesis(new_node)
            return new_node

        def streaming_task_thread(nodetool_task='rebuild'):
            """
            Execute some nodetool command to start persistent streaming
            task: decommission | rebuild | repair
            """
            if nodetool_task in ['repair', 'rebuild']:
                self._destroy_data_and_restart_scylla()

            try:
                self.target_node.run_nodetool(nodetool_task, warning_event_on_exception=(Exception,))
            except Exception:  # pylint: disable=broad-except
                self.log.debug('%s is stopped' % nodetool_task)

        self.task_used_streaming = None
        streaming_logs_stream = self.target_node.follow_system_log(
            patterns=["range_streamer - Unbootstrap starts|range_streamer - Rebuild starts"])
        repair_logs_stream = self.target_node.follow_system_log(patterns=['repair - Repair 1 out of'])
        streaming_error_logs_stream = self.target_node.follow_system_log(patterns=['streaming.*err'])

        streaming_thread = threading.Thread(target=streaming_task_thread, kwargs={'nodetool_task': task},
                                            name='StreamingThread', daemon=True)
        streaming_thread.start()

        def is_streaming_started():
            streaming_logs = list(streaming_logs_stream)
            self.log.debug(streaming_logs)
            if streaming_logs:
                self.task_used_streaming = True

            # In latest master, repair always won't use streaming
            repair_logs = list(repair_logs_stream)
            self.log.debug(repair_logs)
            return len(streaming_logs) > 0 or len(repair_logs) > 0

        wait.wait_for(func=is_streaming_started, timeout=600, step=1,
                      text='Wait for streaming starts', throw_exc=False)
        sleep_time = random.randint(10, 600)
        self.log.debug('wait for random between 10s to 10m --> %s seconds', sleep_time)
        time.sleep(sleep_time)

        self.log.debug('Interrupt the task by hard reboot')

        new_node = None
        with DbEventsFilter(db_event=DatabaseLogEvent.RUNTIME_ERROR,
                            line="This node was decommissioned and will not rejoin",
                            node=self.target_node), \
                DbEventsFilter(db_event=DatabaseLogEvent.RUNTIME_ERROR,
                               line="Fail to send STREAM_MUTATION_DONE",
                               node=self.target_node), \
                DbEventsFilter(db_event=DatabaseLogEvent.DATABASE_ERROR,
                               line="streaming::stream_exception",
                               node=self.target_node), \
                DbEventsFilter(db_event=DatabaseLogEvent.RUNTIME_ERROR,
                               line="got error in row level repair",
                               node=self.target_node):
            self.target_node.reboot(hard=True, verify_ssh=True)
            streaming_thread.join(60)

            if task == 'decommission':
                new_node = decommission_post_action()

        if self.task_used_streaming:
            err = list(streaming_error_logs_stream)
            self.log.debug(err)
        else:
            self.log.debug('Streaming is not used. In latest Scylla, it is optional to use streaming for rebuild and '
                           'decommission, and repair will not use streaming.')
        self.log.info('Recover the target node by a final rebuild')
        wait_db_up_timeout = 300
        if self._is_it_on_kubernetes():
            # NOTE: on K8S nodes come up longer.
            wait_db_up_timeout = 1800
        node_to_rebuild = new_node if new_node else self.target_node
        node_to_rebuild.wait_db_up(verbose=True, timeout=wait_db_up_timeout)
        node_to_rebuild.wait_jmx_up(timeout=wait_db_up_timeout)
        node_to_rebuild.run_nodetool('rebuild')

    def disrupt_decommission_streaming_err(self):
        """
        Stop decommission in middle to trigger some streaming fails, then rebuild the data on the node.
        If the node is decommission unexpectedly, need to re-add a new node to cluster.
        """
        if self._is_it_on_kubernetes():
            raise UnsupportedNemesis(
                "This nemesis logic is not compatible with K8S approach "
                "for handling Scylla member's decommissioning.")
        self.break_streaming_task_and_rebuild(task='decommission')

    def disrupt_rebuild_streaming_err(self):
        """
        Stop rebuild in middle to trigger some streaming fails, then rebuild the data on the node.
        """
        self.break_streaming_task_and_rebuild(task='rebuild')

    def disrupt_repair_streaming_err(self):
        """
        Stop repair in middle to trigger some streaming fails, then rebuild the data on the node.
        """
        self.break_streaming_task_and_rebuild(task='repair')

    def _corrupt_data_file(self):
        """Randomly corrupt data file by dd"""
        ks_cfs = self.cluster.get_non_system_ks_cf_list(db_node=self.target_node)
        if not ks_cfs:
            raise UnsupportedNemesis(
                'Non-system keyspace and table are not found. Nemesis can\'t be run')

        # Corrupt data file
        data_file_pattern = self._choose_file_for_destroy(ks_cfs)
        res = self.target_node.remoter.run('sudo find {}-Data.db'.format(data_file_pattern))
        for sstable_file in res.stdout.split():
            self.target_node.remoter.run('sudo dd if=/dev/urandom of={} count=1024'.format(sstable_file))
            self.log.debug('File {} was corrupted by dd'.format(sstable_file))

    def disrupt_corrupt_then_scrub(self):
        """
        Try to rebuild the sstables of all test keyspaces by scrub, the corrupted partitions
        will be skipped.
        """
        self.log.debug("Rebuild sstables by scrub with `--skip-corrupted`, corrupted partitions will be skipped.")
        with ignore_scrub_invalid_errors():
            for ks in self.cluster.get_test_keyspaces():
                self.target_node.run_nodetool("scrub", args=f"--skip-corrupted {ks}")

    @latency_calculator_decorator
    def add_new_node(self, rack=0):
        return self._add_and_init_new_cluster_node(rack=rack)

    @latency_calculator_decorator
    def decommission_node(self, node):
        self.cluster.decommission(node)

    def decommission_nodes(self, add_nodes_number, rack, is_seed: Optional[Union[bool, DefaultValue]] = DefaultValue,
                           dc_idx: Optional[int] = None):
        for _ in range(add_nodes_number):
            if self._is_it_on_kubernetes():
                self.set_target_node(rack=rack, is_seed=is_seed, allow_only_last_node_in_rack=True)
            else:
                self.set_target_node(is_seed=is_seed, dc_idx=dc_idx)
            self.log.info("Next node will be removed %s", self.target_node)

            try:
                InfoEvent(f'StartEvent - ShrinkCluster started decommissioning a node {self.target_node}').publish()
                self.decommission_node(self.target_node)
                InfoEvent(f'FinishEvent - ShrinkCluster has done decommissioning a node {self.target_node}').publish()
            except Exception as exc:  # pylint: disable=broad-except
                InfoEvent(f'FinishEvent - ShrinkCluster failed decommissioning a node {self.target_node} with error '
                          f'{str(exc)}').publish()

    def disrupt_grow_shrink_cluster(self):
        self._grow_cluster(rack=0)
        self._shrink_cluster(rack=0)

    def disrupt_grow_shrink_new_rack(self):
        rack = max(self.cluster.racks) + 1
        self._grow_cluster(rack)
        self._shrink_cluster(rack)

    def _grow_cluster(self, rack=0):
        if rack > 0:
            if not self._is_it_on_kubernetes():
                raise UnsupportedNemesis("SCT rack functionality is implemented only on kubernetes")
            self.log.info("Rack deletion is not supported on kubernetes yet. "
                          "Please see https://github.com/scylladb/scylla-operator/issues/287")

        add_nodes_number = self.tester.params.get('nemesis_add_node_cnt')
        self.log.info("Start grow cluster on %s nodes", add_nodes_number)
        InfoEvent(message=f"Start grow cluster on {add_nodes_number} nodes").publish()
        for _ in range(add_nodes_number):
            InfoEvent(message=f'GrowCluster - Add New node to {rack} rack').publish()
            added_node = self.add_new_node(rack=rack)
            self.unset_current_running_nemesis(added_node)
            InfoEvent(message=f'GrowCluster - Done adding New node {added_node.name}').publish()
        self.log.info("Finish cluster grow")
        time.sleep(self.interval)

    def _shrink_cluster(self, rack=0):
        add_nodes_number = self.tester.params.get('nemesis_add_node_cnt')
        self.log.info("Start shrink cluster by %s nodes", add_nodes_number)
        InfoEvent(message=f'Start shrink cluster by {add_nodes_number} nodes').publish()
        # Check that number of nodes is enough for decommission:
        cur_num_nodes_in_dc = len([n for n in self.cluster.nodes if n.dc_idx == self.target_node.dc_idx])
        initial_db_size = self.tester.params.get("n_db_nodes")
        if isinstance(initial_db_size, int):
            decommission_nodes_number = min(cur_num_nodes_in_dc - initial_db_size, add_nodes_number)
        else:
            initial_db_size_in_dc = int(initial_db_size.split(" ")[self.target_node.dc_idx])
            decommission_nodes_number = min(cur_num_nodes_in_dc - initial_db_size_in_dc, add_nodes_number)

        if decommission_nodes_number < 1:
            error = "Not enough nodes for decommission"
            self.log.warning("Shrink cluster skipped. Error: %s", error)
            raise Exception(error)

        self.log.info("Start shrink cluster by %s nodes", decommission_nodes_number)
        # Currently on kubernetes first two nodes of each rack are getting seed status
        # Because of such behavior only way to get them decommission is to enable decommissioning
        # TBD: After https://github.com/scylladb/scylla-operator/issues/292 is fixed remove is_seed parameter
        self.decommission_nodes(
            decommission_nodes_number,
            rack,
            is_seed=None if self._is_it_on_kubernetes() else DefaultValue,
            dc_idx=self.target_node.dc_idx)
        num_of_nodes = len(self.cluster.nodes)
        self.log.info("Cluster shrink finished. Current number of nodes %s", num_of_nodes)
        InfoEvent(message=f'Cluster shrink finished. Current number of nodes {num_of_nodes}').publish()

    def disrupt_hot_reloading_internode_certificate(self):
        """
        https://github.com/scylladb/scylla/issues/6067
        Scylla has the ability to hot reload SSL certificates.
        This test will create and reload new certificates for the inter node communication.
        """
        if not self.cluster.params.get('server_encrypt'):
            raise UnsupportedNemesis('Server Encryption is not enabled, hence skipping')

        @timeout_decor(timeout=20, allowed_exceptions=(LogContentNotFound,))
        def check_ssl_reload_log(node_system_log):
            if not list(node_system_log):
                raise LogContentNotFound('Reload SSL message not found in node log')
            return True

        ssl_files_location = json.loads(
            self.target_node.get_scylla_config_param("server_encryption_options"))["certificate"]
        in_place_crt = self.target_node.remoter.run(f"cat {ssl_files_location}",
                                                    ignore_status=True).stdout
        update_certificates()
        node_system_logs = {}
        with DbEventsFilter(
                db_event=DatabaseLogEvent.DATABASE_ERROR,
                line="error GnuTLS:-34, Base64 decoding error"):
            # TBD: To be removed after https://github.com/scylladb/scylla/issues/7909#issuecomment-758062545 is resolved
            for node in self.cluster.nodes:
                node_system_logs[node] = node.follow_system_log(
                    patterns=f'messaging_service - Reloaded {{{ssl_files_location}}}')
                node.remoter.send_files(src='data_dir/ssl_conf/db.crt', dst='/tmp')
                node.remoter.run(f"sudo cp -f /tmp/db.crt {ssl_files_location}")
                new_crt = node.remoter.run(f"cat {ssl_files_location}").stdout
                if in_place_crt == new_crt:
                    raise Exception('The CRT file was not replaced')

            for node in self.cluster.nodes:
                if not check_ssl_reload_log(node_system_logs[node]):
                    raise Exception('SSL auto Reload did not happen')

    @latency_calculator_decorator
    def steady_state_latency(self, sleep_time=None):
        if not sleep_time:
            sleep_time = self.cluster.params.get('nemesis_interval') * 60
        InfoEvent(message=f'StartEvent - start a sleep of {sleep_time} as Steady State').publish()
        time.sleep(sleep_time)
        InfoEvent(message='FinishEvent - Steady State sleep has been finished').publish()

    def disrupt_run_unique_sequence(self):
        sleep_time_between_ops = self.cluster.params.get('nemesis_sequence_sleep_between_ops')
        sleep_time_between_ops = sleep_time_between_ops if sleep_time_between_ops else 8
        sleep_time_between_ops = sleep_time_between_ops * 60
        if not self.has_steady_run:
            self.steady_state_latency()
            self.has_steady_run = True
        InfoEvent(message='StartEvent - start a repair by ScyllaManager').publish()
        self.disrupt_mgmt_repair_cli()
        InfoEvent(message='FinishEvent - Manager repair has finished').publish()
        time.sleep(sleep_time_between_ops)
        InfoEvent(message='Starting terminate_and_replace disruption').publish()
        self.disrupt_terminate_and_replace_node()
        InfoEvent(message='Finished terminate_and_replace disruption').publish()
        time.sleep(sleep_time_between_ops)
        InfoEvent(message='Starting grow_shrink disruption').publish()
        self.disrupt_grow_shrink_cluster()
        InfoEvent(message="Finished grow_shrink disruption").publish()

    def disrupt_memory_stress(self):
        """
        Try to run stress-ng to preempt allocated memory of scylla process,
        we don't monitor swap usage in /proc/$scylla_pid/status, just make sure
        no coredump, serious db error occur during the heavy load of memory.
        """
        if self.target_node.distro.is_rhel_like:
            self.target_node.install_epel()
            self.target_node.remoter.sudo('yum install -y stress-ng')
        elif self.target_node.distro.is_ubuntu:
            # Install stress on Ubuntu: https://snapcraft.io/install/stress-ng/ubuntu
            self.target_node.remoter.sudo('snap install stress-ng')
        else:
            raise UnsupportedNemesis(f"{self.target_node.distro} OS not supported!")
        self.log.info('Try to allocate 120% available memory, the allocated memory will be swaped out')
        self.target_node.remoter.run(
            "stress-ng --vm-bytes $(awk '/MemAvailable/{printf \"%d\\n\", $2 * 1.2;}' < /proc/meminfo)k --vm-keep -m 1 -t 100")

        self.log.info('Try to allocate 90% total memory, the allocated memory will be swaped out')
        self.target_node.remoter.run(
            "stress-ng --vm-bytes $(awk '/MemTotal/{printf \"%d\\n\", $2 * 0.9;}' < /proc/meminfo)k --vm-keep -m 1 -t 100")

    def disrupt_toggle_cdc_feature_properties_on_table(self):
        """Manipulate cdc feature settings

        Find table with CDC enabled (skip nemesis if not found).
        Randomly select on CDC parameter.
        Toggle the selected parameter state (True/False) or select a random value for TTL

        """
        ks_tables_with_cdc = self.cluster.get_all_tables_with_cdc(self.target_node)
        self.log.debug(f"Found next tables with enabled cdc feature: {ks_tables_with_cdc}")

        if not ks_tables_with_cdc:
            self.log.warning("CDC is not enabled on any table. Skipping")
            UnsupportedNemesis("CDC is not enabled on any table. Skipping")
            return

        ks, table = random.choice(ks_tables_with_cdc).split(".")

        self.log.debug(f"Get table {ks}.{table} cdc extension state")
        with self.cluster.cql_connection_patient(node=self.target_node) as session:
            cdc_settings = cdc.options.get_table_cdc_properties(session, ks, table)
        self.log.debug(f"table {ks}.{table} cdc extension state: {cdc_settings}")

        self.log.debug("Choose random cdc property to toggle")
        cdc_property = random.choice(cdc.options.get_cdc_settings_names())
        self.log.debug(f"Next cdc property will be changed {cdc_property}")

        cdc_settings[cdc_property] = cdc.options.toggle_cdc_property(cdc_property, cdc_settings[cdc_property])
        self.log.debug(f"New table cdc extension state: {cdc_settings}")

        self.log.info(f"Apply new cdc settigs for table {ks}.{table}: {cdc_settings}")
        self._alter_table_with_cdc_properties(ks, table, cdc_settings)
        self.log.debug(f"Verify new cdc settings on table {ks}.{table}")
        self._verify_cdc_feature_status(ks, table, cdc_settings)
        InfoEvent(f"{ks}.{table} have new cdc settings {cdc_settings}").publish()

    def disrupt_run_cdcstressor_tool(self):
        ks_tables_with_cdc = self.cluster.get_all_tables_with_cdc(self.target_node)
        if not ks_tables_with_cdc:
            self.log.warning("CDC is not enabled on any table. Skipping")
            UnsupportedNemesis("CDC is not enabled on any table. Skipping")
            return
        ks, table = random.choice(ks_tables_with_cdc).split(".")
        self._run_cdc_stressor_tool(ks, table)

    def _run_cdc_stressor_tool(self, ks, table):
        cdc_stressor_cmd = self.tester.params.get("stress_cdclog_reader_cmd")

        if " -duration" not in cdc_stressor_cmd:
            read_time = random.randint(5, 20)
            cdc_stressor_cmd += f" -duration {read_time}m "

        cdc_reader_thread = self.tester.run_cdclog_reader_thread(stress_cmd=cdc_stressor_cmd,
                                                                 stress_num=1,
                                                                 keyspace_name=ks,
                                                                 base_table_name=table)

        self.tester.verify_cdclog_reader_results(cdc_reader_thread, update_es=False)

    def _alter_table_with_cdc_properties(self, keyspace: str, table: str, cdc_settings: dict) -> None:
        """Alter base table with cdc properties

        Build valid query and alter table with cdc properties
        :param keyspace: keyspace name
        :type keyspace: str
        :param table: base table name
        :type table: str
        :param cdc_enabled: is cdc enabled for base table, defaults to True
        :type cdc_enabled: bool, optional
        :param preimage: is preimage enabled for base table, defaults to False
        :type preimage: bool, optional
        :param postimage: is postimage enabled for base table, defaults to False
        :type postimage: bool, optional
        :param ttl: set ttl for scylla_cdc_log table, defaults to None
        :type ttl: Optional[int], optional
        """
        cmd = f"ALTER TABLE {keyspace}.{table} WITH cdc = {cdc_settings};"
        self.log.debug(f"Alter command: {cmd}")
        with self.cluster.cql_connection_patient(self.target_node) as session:
            session.execute(cmd)
        # wait applying cdc configuration
        time.sleep(30)

    def _verify_cdc_feature_status(self, keyspace: str, table: str, cdc_settings: dict) -> None:

        output = self.target_node.run_cqlsh(f"desc keyspace {keyspace}")
        self.log.debug(output.stdout)

        with self.cluster.cql_connection_patient(node=self.target_node) as session:
            actual_cdc_settings = cdc.options.get_table_cdc_properties(session, keyspace, table)

        if not cdc_settings["enabled"]:
            assert actual_cdc_settings["enabled"] is False, \
                f"CDC options was not disabled. Current: {actual_cdc_settings} expected: {cdc_settings}"
        else:
            assert actual_cdc_settings == cdc_settings, \
                f"CDC extension settings are differs. Current: {actual_cdc_settings} expected: {cdc_settings}"

    def _add_new_node_in_new_dc(self) -> BaseNode:
        new_node = self.cluster.add_nodes(1, dc_idx=0, enable_auto_bootstrap=True)[0]  # add node
        with new_node.remote_scylla_yaml() as scylla_yml:
            scylla_yml.rpc_address = new_node.ip_address
            scylla_yml.seed_provider = [SeedProvider(class_name='org.apache.cassandra.locator.SimpleSeedProvider',
                                                     parameters=[{"seeds": self.tester.db_cluster.seed_nodes_ips}])]
        new_node.remoter.sudo(shell_script_cmd(
            "echo dc_suffix=_nemesis_dc >> /etc/scylla/cassandra-rackdc.properties"))
        self.cluster.wait_for_init(node_list=[new_node], timeout=900,
                                   check_node_health=False)
        self.cluster.wait_for_nodes_up_and_normal(nodes=[new_node])
        self.monitoring_set.reconfigure_scylla_monitoring()
        return new_node

    def _write_read_data_to_multi_dc_keyspace(self, datacenters: List[str]) -> None:
        InfoEvent(message='Writing and reading data with new dc').publish()
        write_cmd = f"cassandra-stress write no-warmup cl=ALL n=10000 -schema 'keyspace=keyspace_new_dc " \
                    f"replication(strategy=NetworkTopologyStrategy,{datacenters[0]}=3,{datacenters[1]}=1) " \
                    f"compression=LZ4Compressor compaction(strategy=SizeTieredCompactionStrategy)' -port jmx=6868 " \
                    f"-mode cql3 native compression=lz4 -rate threads=5 -pop seq=1..10000 -log interval=5"
        write_thread = self.tester.run_stress_thread(stress_cmd=write_cmd, use_single_loader=True)
        self.tester.verify_stress_thread(cs_thread_pool=write_thread)
        self._verify_multi_dc_keyspace_data(consistency_level="ALL")

    def _verify_multi_dc_keyspace_data(self, consistency_level: str = "ALL"):
        read_cmd = f"cassandra-stress read no-warmup cl={consistency_level} n=10000 -schema 'keyspace=keyspace_new_dc " \
                   f"compression=LZ4Compressor' -port jmx=6868 -mode cql3 native compression=lz4 -rate threads=5 " \
                   f"-pop seq=1..10000 -log interval=5"
        read_thread = self.tester.run_stress_thread(stress_cmd=read_cmd, use_single_loader=True)
        self.tester.verify_stress_thread(cs_thread_pool=read_thread)

    def _switch_to_network_replication_strategy(self, keyspaces: List[str]) -> None:
        """Switches replication strategy to NetworkTopology for given keyspaces.
        """
        node = self.cluster.nodes[0]
        nodes_by_region = self.tester.db_cluster.nodes_by_region()
        region = list(nodes_by_region.keys())[0]
        dc_name = self.tester.db_cluster.get_nodetool_info(nodes_by_region[region][0])['Data Center']
        for keyspace in keyspaces:
            replication_strategy = ReplicationStrategy.get(node, keyspace)
            if not isinstance(replication_strategy, SimpleReplicationStrategy):
                # no need to switch as already is NetworkTopology
                continue
            self.log.info(f"Switching replication strategy to Network for '{keyspace}' keyspace")
            if keyspace == "system_auth" and replication_strategy.replication_factor != len(nodes_by_region[region]):
                self.log.warning(f"system_auth keyspace is not replicated on all nodes "
                                 f"({replication_strategy.replication_factor}/{len(nodes_by_region[region])}).")
            network_replication = NetworkTopologyReplicationStrategy(
                **{dc_name: replication_strategy.replication_factor})
            network_replication.apply(node, keyspace)

    def disrupt_add_remove_dc(self) -> None:
        if self._is_it_on_kubernetes():
            raise UnsupportedNemesis("Operator doesn't support multi-DC yet. Skipping.")
        InfoEvent(message='Starting New DC Nemesis').publish()
        node = self.cluster.nodes[0]
        system_keyspaces = ["system_auth", "system_distributed", "system_traces"]
        self._switch_to_network_replication_strategy(self.cluster.get_test_keyspaces() + system_keyspaces)
        with temporary_replication_strategy_setter(node) as replication_strategy_setter:
            new_node = self._add_new_node_in_new_dc()
            datacenters = list(self.tester.db_cluster.get_nodetool_status().keys())
            status = self.tester.db_cluster.get_nodetool_status()
            new_dc_list = [dc for dc in list(status.keys()) if dc.endswith("_nemesis_dc")]
            assert new_dc_list, "new datacenter was not registered"
            new_dc_name = new_dc_list[0]
            for keyspace in system_keyspaces:
                strategy = ReplicationStrategy.get(node, keyspace)
                assert isinstance(strategy, NetworkTopologyReplicationStrategy), \
                    "Should have been already switched to NetworkStrategy"
                strategy.replication_factors.update({new_dc_name: 1})
                replication_strategy_setter(**{keyspace: strategy})
            InfoEvent(message='execute rebuild on new datacenter').publish()
            new_node.run_nodetool(sub_cmd=f"rebuild -- {datacenters[0]}")
            InfoEvent(message='Running full cluster repair on each node').publish()
            for node in self.cluster.nodes:
                node.run_nodetool(sub_cmd="repair -pr", publish_event=True)
            self._write_read_data_to_multi_dc_keyspace(datacenters)
        self.cluster.decommission(new_node)
        self.monitoring_set.reconfigure_scylla_monitoring()
        datacenters = list(self.tester.db_cluster.get_nodetool_status().keys())
        assert not [dc for dc in datacenters if dc.endswith("_nemesis_dc")], "new datacenter was not unregistered"
        self._verify_multi_dc_keyspace_data(consistency_level="QUORUM")

    # def disrupt_auditing_ddl_log_into_table(self):
    #     pass
    #
    # def distupt_auditing_query_log_into_syslog(self):
    #     pass

    def disrupt_auditing_dml_table(self):
        """
        Enables auditing feature. This feature is available only on enterprise clusters.
        Auditing configuration-
        audit: "table"
        audit_categories: "DML"
        """
        if not self.cluster.nodes[0].is_enterprise:
            raise UnsupportedNemesis('Auditing feature is supported only for enterprise. Skipping the test')

        # need to make sure that there is a keyspace with a table filled with data
        keyspaces = self._get_user_keyspaces()
        if not keyspaces:
            raise NoKeyspaceFound('No user keyspaces were found. Skipping the test')
        ks = random.choice(keyspaces) if keyspaces else keyspaces
        logging.info(msg=f"ks={ks}")

        keyspace_table = None
        if not keyspace_table:
            raise NoTableFound(f'No tables were found in keyspace {keyspaces}')

        dict_for_scylla_yaml = {"audit": "table", "audit_tables": "",
                                "audit_categories": "DML", "audit_keyspaces": "mykespace"}
        for node in self.cluster.nodes:
            with node.remote_scylla_yaml() as scylla_yaml:
                scylla_yaml.update(dict_for_scylla_yaml)
                node.restart_scylla_server(verify_up_before=True, verify_up_after=True)

        # TODO: need to check that audit log table was created:
        # SELECT * FROM audit.audit_log ;
        # query_verify = f"SELECT * FROM keyspace1.standard1 WHERE key={key}"
        query = "SELECT * FROM audit.audit_log"
        result = self.target_node.run_cqlsh(query)
        logging.info(msg=f"result={result}")


def disrupt_method_wrapper(method):  # pylint: disable=too-many-statements
    """
    Log time elapsed for method to run

    :param method: Remote method to wrap.
    :return: Wrapped method.
    """

    def argus_create_nemesis_info(nemesis: Nemesis, class_name: str,
                                  method_name: str, start_time: int | float) -> NemesisRunInfo | None:
        try:
            run = ArgusTestRun.get()
            node_desc = NodeDescription(ip=nemesis.target_node.public_ip_address,
                                        name=nemesis.target_node.name, shards=nemesis.target_node.scylla_shards)
            nemesis_info = NemesisRunInfo(class_name=class_name, name=method_name,
                                          duration=0, start_time=int(start_time), target_node=node_desc,
                                          status=NemesisStatus.RUNNING)
            run.run_info.results.add_nemesis(nemesis_info)
            run.save()
            return nemesis_info
        except Exception:  # pylint: disable=broad-except
            nemesis.log.error("Error saving nemesis_info to Argus", exc_info=True)
        return None

    def argus_finalize_nemesis_info(nemesis: Nemesis, nemesis_info: NemesisRunInfo, nemesis_event: DisruptionEvent):
        try:
            run = ArgusTestRun.get()
            if nemesis_event.severity == Severity.ERROR:
                nemesis_info.complete(nemesis_event.full_traceback)
            elif nemesis_event.is_skipped:
                nemesis_info.complete(nemesis_event.skip_reason)
                nemesis_info.status = NemesisStatus.SKIPPED
            else:
                nemesis_info.complete()
            nemesis_info.duration = nemesis_info.end_time - nemesis_info.start_time
            run.save()
        except Exception:  # pylint: disable=broad-except
            nemesis.log.error("Error saving nemesis_info to Argus", exc_info=True)

    def data_validation_prints(args):
        try:
            if hasattr(args[0].tester, 'data_validator') and args[0].tester.data_validator:
                with args[0].cluster.cql_connection_patient(
                        args[0].cluster.nodes[0], keyspace=args[0].tester.data_validator.keyspace_name) as session:
                    args[0].tester.data_validator.validate_range_not_expected_to_change(session, during_nemesis=True)
                    args[0].tester.data_validator.validate_range_expected_to_change(session, during_nemesis=True)
                    args[0].tester.data_validator.validate_deleted_rows(session, during_nemesis=True)
        except Exception as err:  # pylint: disable=broad-except
            args[0].log.debug(f'Data validator error: {err}')

    @wraps(method)
    def wrapper(*args, **kwargs):  # pylint: disable=too-many-statements
        # pylint: disable=too-many-locals
        # pylint: disable=too-many-branches
        args[0].set_target_node()
        method_name = method.__name__
        args[0].current_disruption = "".join(p.capitalize() for p in method_name.replace("disrupt_", "").split("_"))
        args[0].cluster.check_cluster_health()
        num_nodes_before = len(args[0].cluster.nodes)
        start_time = time.time()
        args[0].log.debug('Start disruption at `%s`', datetime.datetime.fromtimestamp(start_time))
        class_name = args[0].get_class_name()
        if class_name.find('Chaos') < 0:
            args[0].metrics_srv.event_start(class_name)
        result = None
        status = True
        # pylint: disable=protected-access
        args[0]._set_current_disruption(f"{args[0].current_disruption} {args[0].target_node}")
        args[0].set_current_running_nemesis(node=args[0].target_node)
        log_info = {
            'operation': args[0].current_disruption,
            'start': int(start_time),
            'end': 0,
            'duration': 0,
            'node': str(args[0].target_node),
            'subtype': 'end',
        }
        # TODO: Temporary print. Will be removed later
        data_validation_prints(args=args)

        with DisruptionEvent(nemesis_name=args[0].get_disrupt_name(),
                             node=args[0].target_node, publish_event=True) as nemesis_event:
            nemesis_info = argus_create_nemesis_info(nemesis=args[0], class_name=class_name,
                                                     method_name=method_name, start_time=start_time)
            try:
                result = method(*args, **kwargs)
            except (UnsupportedNemesis, MethodVersionNotFound) as exp:
                skip_reason = str(exp)
                log_info.update({'subtype': 'skipped', 'skip_reason': skip_reason})
                nemesis_event.skip(skip_reason=skip_reason)
            except Exception as details:  # pylint: disable=broad-except
                nemesis_event.add_error([str(details)])
                nemesis_event.full_traceback = traceback.format_exc()
                nemesis_event.severity = Severity.ERROR
                args[0].error_list.append(str(details))
                args[0].log.error('Unhandled exception in method %s', method, exc_info=True)
                log_info.update({'error': str(details), 'full_traceback': traceback.format_exc()})
                status = False

        end_time = time.time()
        time_elapsed = int(end_time - start_time)
        log_info.update({
            'end': int(end_time),
            'duration': time_elapsed,
        })
        args[0].duration_list.append(time_elapsed)
        args[0].operation_log.append(copy.deepcopy(log_info))
        args[0].log.debug('%s duration -> %s s', args[0].current_disruption, time_elapsed)

        if class_name.find('Chaos') < 0:
            args[0].metrics_srv.event_stop(class_name)
        disrupt = args[0].get_disrupt_name()
        del log_info['operation']

        try:  # So that the nemesis thread won't stop due to elasticsearch failure
            args[0].update_stats(disrupt, status, log_info)
        except ElasticSearchConnectionTimeout as err:
            args[0].log.warning(f"Connection timed out when attempting to update elasticsearch statistics:\n"
                                f"{err}")
        except Exception as err:  # pylint: disable=broad-except
            args[0].log.warning(f"Unexpected error when attempting to update elasticsearch statistics:\n"
                                f"{err}")
        args[0].log.info(f"log_info: {log_info}")
        nemesis_event.duration = time_elapsed
        args[0].cluster.check_cluster_health()
        num_nodes_after = len(args[0].cluster.nodes)
        if num_nodes_before != num_nodes_after:
            args[0].log.error('num nodes before %s and nodes after %s does not match' %
                              (num_nodes_before, num_nodes_after))
        if nemesis_info:
            argus_finalize_nemesis_info(nemesis=args[0], nemesis_info=nemesis_info, nemesis_event=nemesis_event)
        # TODO: Temporary print. Will be removed later
        data_validation_prints(args=args)
        return result

    return wrapper


class SslHotReloadingNemesis(Nemesis):
    disruptive = False

    def disrupt(self):
        self.disrupt_hot_reloading_internode_certificate()


class PauseLdapNemesis(Nemesis):
    disruptive = False
    limited = True

    def disrupt(self):
        self.disrupt_ldap_connection_toggle()


class ToggleLdapConfiguration(Nemesis):
    disruptive = True
    limited = True

    def disrupt(self):
        self.disrupt_disable_enable_ldap_authorization()


class NoOpMonkey(Nemesis):
    kubernetes = True

    def disrupt(self):
        time.sleep(300)


class AddRemoveDcNemesis(Nemesis):
    disruptive = False
    kubernetes = False
    run_with_gemini = False
    limited = True
    topology_changes = True

    def disrupt(self):
        self.disrupt_add_remove_dc()


class GrowShrinkClusterNemesis(Nemesis):
    disruptive = True
    kubernetes = True
    topology_changes = True

    def disrupt(self):
        self.disrupt_grow_shrink_cluster()


class AddRemoveRackNemesis(Nemesis):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.disrupt_grow_shrink_new_rack()


class StopWaitStartMonkey(Nemesis):
    disruptive = True
    kubernetes = True
    limited = True

    def disrupt(self):
        self.disrupt_stop_wait_start_scylla_server(600)


class StopStartMonkey(Nemesis):
    disruptive = True
    kubernetes = True
    limited = True

    def disrupt(self):
        self.disrupt_stop_start_scylla_server()


# Disabling this nemesis due to mulitple known issues like (https://github.com/scylladb/scylla/issues/5080).
# When this issue will be solved, we can re-enable this nemesis.
# class RestartThenRepairNodeMonkey(Nemesis):
#     disruptive = True
#     kubernetes = True
#
#     def disrupt(self):
#         self.disrupt_restart_then_repair_node()


class MultipleHardRebootNodeMonkey(Nemesis):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.disrupt_multiple_hard_reboot_node()


class HardRebootNodeMonkey(Nemesis):
    disruptive = True
    kubernetes = True
    limited = True

    def disrupt(self):
        self.disrupt_hard_reboot_node()


class SoftRebootNodeMonkey(Nemesis):
    disruptive = True
    kubernetes = True
    limited = True

    def disrupt(self):
        self.disrupt_soft_reboot_node()


class DrainerMonkey(Nemesis):
    disruptive = True
    kubernetes = True
    limited = True
    topology_changes = True

    def disrupt(self):
        self.disrupt_nodetool_drain()


class CorruptThenRepairMonkey(Nemesis):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.disrupt_destroy_data_then_repair()


class CorruptThenRebuildMonkey(Nemesis):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.disrupt_destroy_data_then_rebuild()


class DecommissionMonkey(Nemesis):
    disruptive = True
    limited = True
    topology_changes = True

    def disrupt(self):
        self.disrupt_nodetool_decommission()


class DecommissionSeedNode(Nemesis):
    disruptive = True
    topology_changes = True

    def disrupt(self):
        self.disrupt_nodetool_seed_decommission()


class NoCorruptRepairMonkey(Nemesis):
    disruptive = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.disrupt_no_corrupt_repair()


class MajorCompactionMonkey(Nemesis):
    disruptive = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.disrupt_major_compaction()


class RefreshMonkey(Nemesis):
    disruptive = False
    run_with_gemini = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.disrupt_nodetool_refresh(big_sstable=False)


class LoadAndStreamMonkey(Nemesis):
    disruptive = False
    run_with_gemini = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.disrupt_load_and_stream()


class RefreshBigMonkey(Nemesis):
    disruptive = False
    run_with_gemini = False
    kubernetes = True

    def disrupt(self):
        self.disrupt_nodetool_refresh(big_sstable=True)


class EnospcMonkey(Nemesis):
    disruptive = True
    kubernetes = True
    limited = True

    def disrupt(self):
        self.disrupt_nodetool_enospc()


class EnospcAllNodesMonkey(Nemesis):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.disrupt_nodetool_enospc(all_nodes=True)


class NodeToolCleanupMonkey(Nemesis):
    disruptive = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.disrupt_nodetool_cleanup()


class TruncateMonkey(Nemesis):
    disruptive = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.disrupt_truncate()


class TruncateLargeParititionMonkey(Nemesis):
    disruptive = False
    kubernetes = True

    def disrupt(self):
        self.disrupt_truncate_large_partition()


class DeleteByPartitionsMonkey(Nemesis):
    disruptive = False
    kubernetes = True

    def disrupt(self):
        self.disrupt_delete_10_full_partitions()


class DeleteByRowsRangeMonkey(Nemesis):
    disruptive = False
    kubernetes = True

    def disrupt(self):
        self.disrupt_delete_by_rows_range()


class ChaosMonkey(Nemesis):

    def disrupt(self):
        self.call_random_disrupt_method()


class CategoricalMonkey(Nemesis):
    """Randomly picks disruptions to execute using the given categorical distribution.

    Each disruption is assigned a weight. The probability that a disruption D with weight W
    will be executed is W / T, where T is the sum of weights of all disruptions.

    The distribution is passed into the monkey's constructor as a dictionary.
    Keys in the dictionary are names of the disruption methods (from the `Nemesis` class)
    e.g. `disrupt_hard_reboot_node`. The value for each key is the weight of this disruption.
    You can omit the ``disrupt_'' prefix from the key, e.g. `hard_reboot_node`.

    A default weight can be passed; it will be assigned to each disruption that is not listed.
    In particular if the default weight is 0 then the unlisted disruptions won't be executed.
    """

    @staticmethod
    def get_disruption_distribution(dist: dict, default_weight: float) -> Tuple[List[Callable], List[float]]:
        def is_nonnegative_number(val):
            try:
                val = float(val)
            except ValueError:
                return False
            else:
                return val >= 0

        def prefixed(pref: str, val: str) -> str:
            if val.startswith(pref):
                return val
            return pref + val

        all_methods = CategoricalMonkey.get_disrupt_methods()

        population: List[Callable] = []
        weights: List[float] = []
        listed_methods: Set[str] = set()

        for name, weight in dist.items():
            name = str(name)
            prefixed_name = prefixed('disrupt_', name)
            if prefixed_name not in all_methods:
                raise ValueError(f"'{name}' is not a valid disruption. All methods: {all_methods.keys()}")

            if not is_nonnegative_number(weight):
                raise ValueError("Each disruption weight must be a non-negative number."
                                 " '{weight}' is not a valid weight.")

            weight = float(weight)
            if weight > 0:
                population.append(all_methods[prefixed_name])
                weights.append(weight)
            listed_methods.add(prefixed_name)

        if default_weight > 0:
            for method_name, method in all_methods.items():
                if method_name not in listed_methods:
                    population.append(method)
                    weights.append(default_weight)

        if not population:
            raise ValueError("There must be at least one disruption with a positive weight.")

        return population, weights

    @staticmethod
    def get_disrupt_methods() -> Dict[str, Callable]:
        return {attr[0]: attr[1] for attr in inspect.getmembers(CategoricalMonkey) if
                attr[0].startswith('disrupt_') and
                callable(attr[1])}

    def __init__(self, tester_obj, termination_event, dist: dict, default_weight: float = 1):
        super().__init__(tester_obj, termination_event)
        self.disruption_distribution = CategoricalMonkey.get_disruption_distribution(dist, default_weight)

    def disrupt(self):
        self._random_disrupt()

    def _random_disrupt(self):
        population, weights = self.disruption_distribution
        assert len(population) == len(weights) and population

        method = random.choices(population, weights=weights)[0]
        bound_method = method.__get__(self, CategoricalMonkey)
        self.execute_disrupt_method(bound_method)


class LimitedChaosMonkey(Nemesis):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.disrupt_methods_list = self.get_list_of_methods_compatible_with_backend(limited=True)

    def disrupt(self):
        # Limit the nemesis scope:
        #  - NodeToolCleanupMonkey
        #  - DecommissionMonkey
        #  - DrainerMonkey
        #  - RefreshMonkey
        #  - StopStartMonkey
        #  - MajorCompactionMonkey
        #  - ModifyTableMonkey
        #  - EnospcMonkey
        #  - StopWaitStartMonkey
        #  - HardRebootNodeMonkey
        #  - SoftRebootNodeMonkey
        #  - TruncateMonkey
        #  - TopPartitions
        #  - MgmtRepair
        #  - NoCorruptRepairMonkey
        #  - SnapshotOperations
        #  - AbortRepairMonkey
        #  - MgmtBackup
        #  - MgmtBackupSpecificKeyspaces
        #  - AddDropColumnMonkey
        #  - PauseLdapNemesis
        #  - ToggleLdapConfiguration
        self.call_random_disrupt_method(disrupt_methods=self.disrupt_methods_list)


CLOUD_LIMITED_CHAOS_MONKEY = ['disrupt_nodetool_cleanup',
                              'disrupt_nodetool_drain', 'disrupt_nodetool_refresh',
                              'disrupt_stop_start_scylla_server', 'disrupt_major_compaction',
                              'disrupt_modify_table', 'disrupt_nodetool_enospc',
                              'disrupt_stop_wait_start_scylla_server',
                              'disrupt_soft_reboot_node',
                              'disrupt_truncate']


class ScyllaCloudLimitedChaosMonkey(Nemesis):

    def disrupt(self):
        # Limit the nemesis scope to only one relevant to scylla cloud, where we defined we don't have AWS api access:
        self.call_random_disrupt_method(disrupt_methods=CLOUD_LIMITED_CHAOS_MONKEY)


class AllMonkey(Nemesis):

    def disrupt(self):
        self.call_random_disrupt_method(predefined_sequence=True)


class MdcChaosMonkey(Nemesis):

    def disrupt(self):
        self.call_random_disrupt_method(
            disrupt_methods=['disrupt_destroy_data_then_repair', 'disrupt_no_corrupt_repair',
                             'disrupt_nodetool_decommission'])


class UpgradeNemesis(Nemesis):

    # # upgrade a single node
    # def upgrade_node(self, node):
    #     repo_file = self.cluster.params.get('repo_file', None,  'scylla.repo.upgrade')
    #     new_version = self.cluster.params.get('new_version', None,  '')
    #     upgrade_node_packages = self.cluster.params.get('upgrade_node_packages')
    #     self.log.info('Upgrading a Node')
    #
    #     # We assume that if update_db_packages is not empty we install packages from there.
    #     # In this case we don't use upgrade based on repo_file(ignored sudo yum update scylla...)
    #     orig_ver = node.remoter.run('rpm -qa scylla-server')
    #     if upgrade_node_packages:
    #         # update_scylla_packages
    #         node.remoter.send_files(upgrade_node_packages, '/tmp/scylla', verbose=True)
    #         # node.remoter.run('sudo yum update -y --skip-broken', connect_timeout=900)
    #         node.remoter.run('sudo yum install python34-PyYAML -y')
    #         # replace the packages
    #         node.remoter.run('rpm -qa scylla\*')
    #         node.run_nodetool("snapshot")
    #         # update *development* packages
    #         node.remoter.run('sudo rpm -UvhR --oldpackage /tmp/scylla/*development*', ignore_status=True)
    #         # and all the rest
    #         node.remoter.run('sudo rpm -URvh --replacefiles /tmp/scylla/*.rpm | true')
    #         node.remoter.run('rpm -qa scylla\*')
    #     elif repo_file:
    #         scylla_repo = get_data_dir_path(repo_file)
    #         node.remoter.send_files(scylla_repo, '/tmp/scylla.repo', verbose=True)
    #         node.remoter.run('sudo cp /etc/yum.repos.d/scylla.repo ~/scylla.repo-backup')
    #         node.remoter.run('sudo cp /tmp/scylla.repo /etc/yum.repos.d/scylla.repo')
    #         # backup the data
    #         node.remoter.run('sudo cp /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml-backup')
    #         node.run_nodetool("snapshot")
    #         node.remoter.run('sudo chown root.root /etc/yum.repos.d/scylla.repo')
    #         node.remoter.run('sudo chmod 644 /etc/yum.repos.d/scylla.repo')
    #         node.remoter.run('sudo yum clean all')
    #         ver_suffix = '-{}'.format(new_version) if new_version else ''
    #         node.remoter.run('sudo yum install scylla{0} scylla-server{0} scylla-jmx{0} scylla-tools{0}'
    #                          ' scylla-conf{0} scylla-kernel-conf{0} scylla-debuginfo{0} -y'.format(ver_suffix))
    #     # flush all memtables to SSTables
    #     node.run_nodetool("drain", timeout=3600, coredump_on_timeout=True)
    #     node.remoter.run('sudo systemctl restart scylla-server.service')
    #     node.wait_db_up(verbose=True)
    #     new_ver = node.remoter.run('rpm -qa scylla-server')
    #     if orig_ver == new_ver:
    #         self.log.error('scylla-server version isn\'t changed')

    def disrupt(self):
        self.log.info('Upgrade Nemesis begin')
        # get the number of nodes
        nodes_num = len(self.cluster.nodes)
        # prepare an array containing the indexes
        indexes = list(range(nodes_num))
        # shuffle it so we will upgrade the nodes in a
        # random order
        random.shuffle(indexes)

        # upgrade all the nodes in random order
        for i in indexes:
            node = self.cluster.nodes[i]
            self.upgrade_node(node)  # pylint: disable=no-member

        self.log.info('Upgrade Nemesis end')


class UpgradeNemesisOneNode(UpgradeNemesis):

    def disrupt(self):
        self.log.info('UpgradeNemesisOneNode begin')
        self.upgrade_node(self.cluster.node_to_upgrade)  # pylint: disable=no-member

        self.log.info('UpgradeNemesisOneNode end')


class RollbackNemesis(Nemesis):

    def rollback_node(self, node):
        self.log.info('Rollbacking a Node')
        orig_ver = node.remoter.run('rpm -qa scylla-server')
        node.remoter.run('sudo cp ~/scylla.repo-backup /etc/yum.repos.d/scylla.repo')
        # backup the data
        node.run_nodetool("snapshot")
        node.remoter.run('sudo chown root.root /etc/yum.repos.d/scylla.repo')
        node.remoter.run('sudo chmod 644 /etc/yum.repos.d/scylla.repo')
        node.remoter.run('sudo yum clean all')
        node.remoter.run(
            'sudo yum downgrade scylla scylla-server scylla-jmx scylla-tools scylla-conf scylla-kernel-conf scylla-debuginfo -y')
        # flush all memtables to SSTables
        node.run_nodetool("drain", timeout=3600, coredump_on_timeout=True)
        node.remoter.run('sudo cp {0}-backup {0}'.format(SCYLLA_YAML_PATH))
        node.remoter.run('sudo systemctl restart scylla-server.service')
        node.wait_db_up(verbose=True)
        new_ver = node.remoter.run('rpm -qa scylla-server')
        self.log.debug('original scylla-server version is %s, latest: %s' % (orig_ver, new_ver))
        if orig_ver == new_ver:
            raise ValueError('scylla-server version isn\'t changed')

    def disrupt(self):
        self.log.info('Rollback Nemesis begin')
        # get the number of nodes
        nodes_num = len(self.cluster.nodes)
        # prepare an array containing the indexes
        indexes = list(range(nodes_num))
        # shuffle it so we will rollback the nodes in a
        # random order
        random.shuffle(indexes)

        # rollback all the nodes in random order
        for i in indexes:
            node = self.cluster.nodes[i]
            self.rollback_node(node)

        self.log.info('Rollback Nemesis end')


class ModifyTableMonkey(Nemesis):
    disruptive = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.disrupt_modify_table()


class AddDropColumnMonkey(Nemesis):
    disruptive = False
    run_with_gemini = False
    networking = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.disrupt_add_drop_column()


class ToggleTableIcsMonkey(Nemesis):
    kubernetes = True

    def disrupt(self):
        self.disrupt_toggle_table_ics()


class MgmtBackup(Nemesis):
    disruptive = False
    limited = True

    def disrupt(self):
        self.disrupt_mgmt_backup()


class MgmtBackupSpecificKeyspaces(Nemesis):
    disruptive = False
    limited = True

    def disrupt(self):
        self.disrupt_mgmt_backup_specific_keyspaces()


class MgmtRepair(Nemesis):
    disruptive = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.log.info('disrupt_mgmt_repair_cli Nemesis begin')
        self.disrupt_mgmt_repair_cli()
        self.log.info('disrupt_mgmt_repair_cli Nemesis end')
        # For Manager APIs test, use: self.disrupt_mgmt_repair_api()


class AbortRepairMonkey(Nemesis):
    disruptive = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.disrupt_abort_repair()


class NodeTerminateAndReplace(Nemesis):
    disruptive = True
    # It should not be run on kubernetes, since it is a manual procedure
    # While on kubernetes we put it all on scylla-operator
    kubernetes = False
    topology_changes = True

    def disrupt(self):
        self.disrupt_terminate_and_replace_node()


class OperatorNodeTerminateAndReplace(Nemesis):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.disrupt_terminate_and_replace_node_kubernetes()


class OperatorNodeTerminateDecommissionAdd(Nemesis):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.disrupt_terminate_decommission_add_node_kubernetes()


class OperatorNodeReplace(Nemesis):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.disrupt_replace_node_kubernetes()


class OperatorNodetoolFlushAndReshard(Nemesis):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.disrupt_nodetool_flush_and_reshard_on_kubernetes()


class ScyllaKillMonkey(Nemesis):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.disrupt_kill_scylla()


class ValidateHintedHandoffShortDowntime(Nemesis):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.disable_disrupt_validate_hh_short_downtime()


class SnapshotOperations(Nemesis):
    disruptive = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.disrupt_snapshot_operations()


class NodeRestartWithResharding(Nemesis):
    disruptive = True
    kubernetes = True
    topology_changes = True

    def disrupt(self):
        self.disrupt_restart_with_resharding()


class ClusterRollingRestart(Nemesis):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.disrupt_rolling_restart_cluster(random_order=False)


class RollingRestartConfigChangeInternodeCompression(Nemesis):
    disruptive = True
    full_cluster_restart = True

    def disrupt(self):
        self.disrupt_rolling_config_change_internode_compression()


class ClusterRollingRestartRandomOrder(Nemesis):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.disrupt_rolling_restart_cluster(random_order=True)


class SwitchBetweenPasswordAuthAndSaslauthdAuth(Nemesis):
    disruptive = True  # the nemesis has rolling restart

    def disrupt(self):
        self.disrupt_switch_between_password_authenticator_and_saslauthd_authenticator_and_back()


class TopPartitions(Nemesis):
    disruptive = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.disrupt_show_toppartitions()


class RandomInterruptionNetworkMonkey(Nemesis):
    disruptive = True
    networking = True
    run_with_gemini = False

    def disrupt(self):
        self.disrupt_network_random_interruptions()


class BlockNetworkMonkey(Nemesis):
    disruptive = True
    networking = True
    run_with_gemini = False

    def disrupt(self):
        self.disrupt_network_block()


class RejectInterNodeNetworkMonkey(Nemesis):
    disruptive = True
    networking = True
    run_with_gemini = False

    def disrupt(self):
        self._disrupt_network_reject_inter_node_communication()


class RejectNodeExporterNetworkMonkey(Nemesis):
    disruptive = True
    networking = True
    run_with_gemini = False

    def disrupt(self):
        self.disrupt_network_reject_node_exporter()


class RejectThriftNetworkMonkey(Nemesis):
    disruptive = True
    networking = True
    run_with_gemini = False

    def disrupt(self):
        self.disrupt_network_reject_thrift()


class StopStartInterfacesNetworkMonkey(Nemesis):
    disruptive = True
    networking = True
    run_with_gemini = False

    def disrupt(self):
        self.disrupt_network_start_stop_interface()


class DisruptiveMonkey(Nemesis):
    # Limit the nemesis scope:
    #  - ValidateHintedHandoffShortDowntime
    #  - CorruptThenRepairMonkey
    #  - CorruptThenRebuildMonkey
    #  - RestartThenRepairNodeMonkey
    #  - StopStartMonkey
    #  - MultipleHardRebootNodeMonkey
    #  - HardRebootNodeMonkey
    #  - SoftRebootNodeMonkey
    #  - StopWaitStartMonkey
    #  - NodeTerminateAndReplace
    #  - EnospcMonkey
    #  - DecommissionMonkey
    #  - NodeRestartWithResharding
    #  - DrainerMonkey

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.disrupt_methods_list = self.get_list_of_methods_compatible_with_backend(disruptive=True)

    def disrupt(self):
        self.call_random_disrupt_method(disrupt_methods=self.disrupt_methods_list)


class NonDisruptiveMonkey(Nemesis):
    # Limit the nemesis scope:
    #  - NodeToolCleanupMonkey
    #  - SnapshotOperations
    #  - RefreshMonkey
    #  - RefreshBigMonkey -
    #  - NoCorruptRepairMonkey
    #  - MgmtRepair

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.disrupt_methods_list = self.get_list_of_methods_compatible_with_backend(disruptive=False)

    def disrupt(self):
        self.call_random_disrupt_method(disrupt_methods=self.disrupt_methods_list)


class NetworkMonkey(Nemesis):
    # Limit the nemesis scope:
    #  - RandomInterruptionNetworkMonkey
    #  - StopStartInterfacesNetworkMonkey
    #  - BlockNetworkMonkey
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.disrupt_methods_list = self.get_list_of_methods_compatible_with_backend(networking=True)

    def disrupt(self):
        self.call_random_disrupt_method(disrupt_methods=self.disrupt_methods_list)


class GeminiChaosMonkey(Nemesis):
    # Limit the nemesis scope to use with gemini
    # - StopStartMonkey
    # - RestartThenRepairNodeMonkey
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.disrupt_methods_list = self.get_list_of_methods_compatible_with_backend(run_with_gemini=True)

    def disrupt(self):
        self.call_random_disrupt_method(disrupt_methods=self.disrupt_methods_list)


class GeminiNonDisruptiveChaosMonkey(Nemesis):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        run_with_gemini = set(self.get_list_of_methods_compatible_with_backend(run_with_gemini=True))
        non_disruptive = set(self.get_list_of_methods_compatible_with_backend(disruptive=False))
        self.disrupt_methods_list = run_with_gemini.intersection(non_disruptive)

    def disrupt(self):
        self.call_random_disrupt_method(disrupt_methods=self.disrupt_methods_list)


class KubernetesScyllaOperatorMonkey(Nemesis):
    # All Nemesis that could be run on kubernetes backend
    disruptive = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        ignore_methods = (
            # NOTE: placeholder for methods which must not run on K8S backends
        )
        self.disrupt_methods_list = list(
            set(self.get_list_of_methods_compatible_with_backend()) - set(ignore_methods))

    def disrupt(self):
        self.call_random_disrupt_method(disrupt_methods=self.disrupt_methods_list)


class ScyllaOperatorBasicOperationsMonkey(Nemesis):
    """
    Selected number of nemesis that is focused on scylla-operator functionality
    """
    disruptive = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.disrupt_methods_list = [
            'disrupt_nodetool_flush_and_reshard_on_kubernetes',
            'disrupt_rolling_restart_cluster',
            'disrupt_grow_shrink_cluster',
            'disrupt_grow_shrink_new_rack',
            'disrupt_stop_start_scylla_server',
            'disrupt_terminate_and_replace_node_kubernetes',
            'disrupt_terminate_decommission_add_node_kubernetes',
            'disrupt_replace_node_kubernetes',
            'disrupt_mgmt_repair_cli',
            'disrupt_mgmt_backup_specific_keyspaces',
            'disrupt_mgmt_backup',
        ]

    def disrupt(self):
        self.call_random_disrupt_method(disrupt_methods=self.disrupt_methods_list, predefined_sequence=True)


class NemesisSequence(Nemesis):
    disruptive = True
    networking = False
    run_with_gemini = False

    def disrupt(self):
        self.disrupt_run_unique_sequence()


class TerminateAndRemoveNodeMonkey(Nemesis):
    """Remove a Node from a Scylla Cluster (Down Scale)"""
    disruptive = True
    # It should not be run on kubernetes, since it is a manual procedure
    # While on kubernetes we put it all on scylla-operator
    kubernetes = False
    topology_changes = True

    def disrupt(self):
        self.disrupt_remove_node_then_add_node()


class SisyphusMonkey(Nemesis):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.build_list_of_disruptions_to_execute()
        self.shuffle_list_of_disruptions()

    def disrupt(self):
        self.call_next_nemesis()


class ToggleCDCMonkey(Nemesis):
    disruptive = False

    def disrupt(self):
        self.disrupt_toggle_cdc_feature_properties_on_table()


class CDCStressorMonkey(Nemesis):
    disruptive = False

    def disrupt(self):
        self.disrupt_run_cdcstressor_tool()


class DecommissionStreamingErrMonkey(Nemesis):
    disruptive = True
    topology_changes = True

    def disrupt(self):
        self.disrupt_decommission_streaming_err()


class RebuildStreamingErrMonkey(Nemesis):
    disruptive = True

    def disrupt(self):
        self.disrupt_rebuild_streaming_err()


class RepairStreamingErrMonkey(Nemesis):
    disruptive = True

    def disrupt(self):
        self.disrupt_repair_streaming_err()


DEPRECATED_LIST_OF_NEMESISES = [UpgradeNemesis, UpgradeNemesisOneNode, RollbackNemesis]

COMPLEX_NEMESIS = [NoOpMonkey, ChaosMonkey,
                   LimitedChaosMonkey,
                   ScyllaCloudLimitedChaosMonkey,
                   AllMonkey, MdcChaosMonkey,
                   DisruptiveMonkey, NonDisruptiveMonkey, GeminiNonDisruptiveChaosMonkey,
                   GeminiChaosMonkey, NetworkMonkey, KubernetesScyllaOperatorMonkey, SisyphusMonkey,
                   CategoricalMonkey]


class CorruptThenScrubMonkey(Nemesis):
    disruptive = False

    def disrupt(self):
        self.disrupt_corrupt_then_scrub()


class MemoryStressMonkey(Nemesis):
    disruptive = True

    def disrupt(self):
        self.disrupt_memory_stress()


class ResetLocalSchemaMonkey(Nemesis):
    disruptive = False

    def disrupt(self):
        self.disrupt_resetlocalschema()


class StartStopMajorCompaction(Nemesis):
    disruptive = False

    def disrupt(self):
        self.disrupt_start_stop_major_compaction()


class StartStopScrubCompaction(Nemesis):
    disruptive = False

    def disrupt(self):
        self.disrupt_start_stop_scrub_compaction()


class StartStopCleanupCompaction(Nemesis):
    disruptive = False

    def disrupt(self):
        self.disrupt_start_stop_cleanup_compaction()


class StartStopValidationCompaction(Nemesis):
    disruptive = False

    def disrupt(self):
        self.disrupt_start_stop_validation_compaction()


class EnableAuditingDmlTable(Nemesis):
    """
    class for auditing nemesis. The feature is available only on enterprise versions.
    Auditing configuration-
    audit: "table"
    audit_categories: "DML"
    """
    disruptive = False

    def disrupt(self):
        self.disrupt_auditing_dml_table()
