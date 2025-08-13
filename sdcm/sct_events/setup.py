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

import json
import time
import logging
from typing import Union, Optional
from pathlib import Path

from sdcm.sct_config import SCTConfiguration
from sdcm.sct_events import Severity
from sdcm.sct_events.event_handler import start_events_handler
from sdcm.sct_events.grafana import start_grafana_pipeline
from sdcm.sct_events.filters import DbEventsFilter, EventsSeverityChangerFilter
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.loaders import CassandraStressEvent, CassandraStressLogEvent
from sdcm.sct_events.file_logger import start_events_logger
from sdcm.sct_events.events_device import start_events_main_device
from sdcm.sct_events.events_analyzer import start_events_analyzer
from sdcm.sct_events.event_counter import start_events_counter
from sdcm.sct_events.events_processes import \
    EVENTS_MAIN_DEVICE_ID, EVENTS_FILE_LOGGER_ID, EVENTS_ANALYZER_ID, \
    EVENTS_GRAFANA_ANNOTATOR_ID, EVENTS_GRAFANA_AGGREGATOR_ID, EVENTS_GRAFANA_POSTMAN_ID, \
    EventsProcessesRegistry, create_default_events_process_registry, get_events_process, EVENTS_HANDLER_ID, EVENTS_COUNTER_ID
from sdcm.utils.issues import SkipPerIssues


EVENTS_DEVICE_START_DELAY = 1  # seconds
EVENTS_SUBSCRIBERS_START_DELAY = 3  # seconds
EVENTS_PROCESS_STOP_TIMEOUT = 10  # seconds

LOGGER = logging.getLogger(__name__)


def start_events_device(log_dir: Optional[Union[str, Path]] = None,
                        _registry: Optional[EventsProcessesRegistry] = None) -> None:
    if _registry is None:
        if log_dir is None:
            raise RuntimeError("Should provide log_dir or instance of EventsProcessesRegistry")
        _registry = create_default_events_process_registry(log_dir=log_dir)

    start_events_main_device(_registry=_registry)

    time.sleep(EVENTS_DEVICE_START_DELAY)

    start_events_logger(_registry=_registry)
    start_grafana_pipeline(_registry=_registry)
    start_events_analyzer(_registry=_registry)
    start_events_handler(_registry=_registry)
    start_events_counter(_registry=_registry)

    time.sleep(EVENTS_SUBSCRIBERS_START_DELAY)


def stop_events_device(_registry: Optional[EventsProcessesRegistry] = None) -> None:
    LOGGER.debug("Stop all events consumers...")
    processes = (
        EVENTS_FILE_LOGGER_ID,
        EVENTS_GRAFANA_ANNOTATOR_ID,
        EVENTS_GRAFANA_AGGREGATOR_ID,
        EVENTS_GRAFANA_POSTMAN_ID,
        EVENTS_COUNTER_ID,
        EVENTS_HANDLER_ID,
        EVENTS_ANALYZER_ID,
        EVENTS_MAIN_DEVICE_ID,
    )
    events_stat = {}
    for name in processes:
        if (proc := get_events_process(name, _registry=_registry)) and proc.is_alive():
            LOGGER.debug("Signaling %s to terminate and wait for finish...", name)
            proc.stop(timeout=EVENTS_PROCESS_STOP_TIMEOUT)
            events_stat[f"{proc._registry}[{name}]"] = proc.events_counter
    LOGGER.debug("All events consumers stopped.")

    if events_stat:
        LOGGER.info("Statistics of sent/received events (by device): %s", json.dumps(events_stat, indent=4))


def enable_default_filters(sct_config: SCTConfiguration):

    # Default filters.
    if SkipPerIssues('scylladb/scylla-enterprise#1272', params=sct_config):
        EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                    event_class=DatabaseLogEvent.DATABASE_ERROR,
                                    regex=r'.*workload prioritization - update_service_levels_from_distributed_data: an '
                                          r'error occurred while retrieving configuration').publish()

    if SkipPerIssues('scylladb/scylla-enterprise#2710', params=sct_config):
        EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                    event_class=DatabaseLogEvent.DATABASE_ERROR,
                                    regex='ldap_connection - Seastar read failed: std::system_error '
                                    '(error system:104, read: Connection reset by peer)').publish()

    if sct_config.get('cluster_backend').startswith("k8s"):
        # cause of issue https://github.com/scylladb/scylla-cluster-tests/issues/6119
        EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                    event_class=DatabaseLogEvent.RUNTIME_ERROR,
                                    regex=r'.*sidecar/controller.go.*std::runtime_error '
                                          r'\(Operation decommission is in progress, try again\)').publish()

    if SkipPerIssues([
        'scylladb/scylladb#16206',
        'scylladb/scylladb#16259',
        'scylladb/scylladb#15598',
    ], params=sct_config):
        EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                    event_class=DatabaseLogEvent,
                                    regex=r".*view - Error applying view update.*").publish()

    # By default audit is disabled in 20223.1 by https://github.com/scylladb/scylla-enterprise/pull/3094.
    # But it won't be disabled in 2022.1 and 2022.2.
    # This message generates too much noise for us. We do not need it will fail the test. Create WARNING message, not ERROR.
    # This event will be created in branch 2023.1, not in 2022.x
    # issue that should be track https://github.com/scylladb/scylla-enterprise/issues/3148
    if SkipPerIssues('scylladb/scylla-enterprise#3148', params=sct_config):
        EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                    event_class=CassandraStressLogEvent.ConsistencyError,
                                    regex=r".*Authentication error on host.*Cannot achieve consistency level for cl ONE").publish()

    # Change Startup failure severity to WARNING to avoid critical failure in tests.
    # Apply only for Cloud clusters, where every failed node startup is retried by Cloud leading to the normal
    # cluster state at the point when SCT starts to run tests.
    if sct_config.get('use_cloud_manager') and SkipPerIssues('scylladb/siren#13281', params=sct_config):
        EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                    event_class=DatabaseLogEvent.DATABASE_ERROR,
                                    regex=r".*Startup failed.*").publish()

    if sct_config.get('new_scylla_repo') or sct_config.get('new_version'):
        # scylladb/scylla-enterprise#3814
        # scylladb/scylla-enterprise#3092
        # skip audit related issue when upgrading from older versions it's default in
        # TODO: remove when branch 2022.2 is deprecated
        EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                    event_class=DatabaseLogEvent.DATABASE_ERROR,
                                    regex=r".*audit - Unexpected exception when writing login.*"
                                          r"Cannot achieve consistency level for cl ONE").publish()

    DbEventsFilter(db_event=DatabaseLogEvent.BACKTRACE, line='Rate-limit: supressed').publish()
    DbEventsFilter(db_event=DatabaseLogEvent.BACKTRACE, line='Rate-limit: suppressed').publish()
    DbEventsFilter(db_event=DatabaseLogEvent.WARNING, line='abort_requested_exception').publish()

    # As per discussion in https://github.com/scylladb/scylla-enterprise/issues/4691#issuecomment-2348867638 and
    # https://github.com/scylladb/scylla-cluster-tests/issues/8693#issuecomment-2358147285
    # the 'aborting on shard' error is acceptable when RPC connections are dropped
    EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                event_class=DatabaseLogEvent.ABORTING_ON_SHARD,
                                regex=r'.*Parent connection [\d]+ is aborting on shard').publish()

    # As written in https://github.com/scylladb/scylladb/issues/20950#issuecomment-2411387784
    # raft error messages with connection close could be ignored during topology operations,
    # upgrades and any place where the race between raft global barrier and gossipier could
    # take place. So ignore such messages globally for any sct test.
    # TODO: this should be removed after gossiper will be removed.
    DbEventsFilter(db_event=DatabaseLogEvent.RUNTIME_ERROR,
                   line=r".*raft_topology - topology change coordinator fiber got error std::runtime_error"
                        r" \(raft topology: exec_global_command\(barrier\) failed with seastar::rpc::closed_error"
                        r" \(connection is closed\)\)").publish()


def enable_teardown_filters():
    # If a nemesis happens to start a cassandra stress container just as teardown starts,
    # it is possible the container is removed faster than the nemesis can be stopped,
    # and it will try to use it and fail.
    EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                event_class=CassandraStressEvent,
                                regex=r'.*Error response from daemon: No such container.*',
                                extra_time_to_expiration=60).publish()


__all__ = ("start_events_device", "stop_events_device", "enable_default_filters")
