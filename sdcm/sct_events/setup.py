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
import atexit
import logging
from typing import Union, Optional
from pathlib import Path

from sdcm.sct_events import Severity
from sdcm.sct_events.grafana import start_grafana_pipeline
from sdcm.sct_events.filters import DbEventsFilter, EventsSeverityChangerFilter
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.file_logger import start_events_logger
from sdcm.sct_events.events_device import start_events_main_device
from sdcm.sct_events.events_analyzer import start_events_analyzer
from sdcm.sct_events.events_processes import \
    EVENTS_MAIN_DEVICE_ID, EVENTS_FILE_LOGGER_ID, EVENTS_ANALYZER_ID, \
    EVENTS_GRAFANA_ANNOTATOR_ID, EVENTS_GRAFANA_AGGREGATOR_ID, EVENTS_GRAFANA_POSTMAN_ID, \
    EventsProcessesRegistry, create_default_events_process_registry, get_events_process


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

    time.sleep(EVENTS_SUBSCRIBERS_START_DELAY)

    # Default filters.
    EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                event_class=DatabaseLogEvent.DATABASE_ERROR,
                                regex=r'.*workload prioritization - update_service_levels_from_distributed_data: an '
                                      r'error occurred while retrieving configuration').publish()
    EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                event_class=DatabaseLogEvent.DATABASE_ERROR,
                                regex='cdc - Could not update CDC description table with generation').publish()
    EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                event_class=DatabaseLogEvent.DATABASE_ERROR,
                                regex='ldap_connection - Seastar read failed: std::system_error '
                                '(error system:104, read: Connection reset by peer)').publish()
    DbEventsFilter(db_event=DatabaseLogEvent.BACKTRACE, line='Rate-limit: supressed').publish()
    DbEventsFilter(db_event=DatabaseLogEvent.BACKTRACE, line='Rate-limit: suppressed').publish()

    atexit.register(stop_events_device, _registry=_registry)


def stop_events_device(_registry: Optional[EventsProcessesRegistry] = None) -> None:
    LOGGER.debug("Stop all events consumers...")
    processes = (
        EVENTS_FILE_LOGGER_ID,
        EVENTS_GRAFANA_ANNOTATOR_ID,
        EVENTS_GRAFANA_AGGREGATOR_ID,
        EVENTS_GRAFANA_POSTMAN_ID,
        EVENTS_ANALYZER_ID,
        EVENTS_MAIN_DEVICE_ID,
    )
    events_stat = {}
    for name in processes:
        if (proc := get_events_process(name, _registry=_registry)) and proc.is_alive():
            LOGGER.debug("Signaling %s to terminate and wait for finish...", name)
            proc.stop(timeout=EVENTS_PROCESS_STOP_TIMEOUT)
            events_stat[f"{proc._registry}[{name}]"] = proc.events_counter  # pylint: disable=protected-access
    LOGGER.debug("All events consumers stopped.")

    if events_stat:
        LOGGER.info("Statistics of sent/received events (by device): %s", json.dumps(events_stat, indent=4))


__all__ = ("start_events_device", "stop_events_device", )
