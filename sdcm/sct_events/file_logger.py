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

import re
import json
import logging
import collections
import multiprocessing
from typing import Tuple, Optional, Callable, Any, Dict, List, cast
from pathlib import Path
from functools import partial
from itertools import chain
from collections import deque as tail

from sdcm.sct_events import Severity
from sdcm.sct_events.base import SctEvent
from sdcm.sct_events.system import TestResultEvent
from sdcm.sct_events.events_device import get_events_main_device
from sdcm.sct_events.events_processes import \
    EVENTS_FILE_LOGGER_ID, EventsProcessesRegistry, BaseEventsProcess, \
    start_events_process, get_events_process, verbose_suppress


EVENTS_LOG: str = "events.log"
SUMMARY_LOG: str = "summary.log"
CRITICAL_LOG: str = "critical.log"
ERROR_LOG: str = "error.log"
WARNING_LOG: str = "warning.log"
NORMAL_LOG: str = "normal.log"
DEBUG_LOG: str = "debug.log"

LINE_START_RE = re.compile(r"^\d{4}-\d{2}-\d{2} ")  # date in YYYY-MM-DD format

LOGGER = logging.getLogger(__name__)


class head(list):
    def __init__(self, maxlen: Optional[int] = None):
        super().__init__()
        if maxlen is None:
            self.append = super().append
        self.maxlen = maxlen

    def append(self, item: Any) -> None:
        if len(self) < self.maxlen:
            super().append(item)


class EventsFileLogger(BaseEventsProcess[Tuple[str, Any], None], multiprocessing.Process):
    def __init__(self, _registry: EventsProcessesRegistry):
        base_dir: Path = get_events_main_device(_registry=_registry).events_log_base_dir

        self.events_log = base_dir / EVENTS_LOG
        self.events_logs_by_severity = {
            Severity.CRITICAL: base_dir / CRITICAL_LOG,
            Severity.ERROR:    base_dir / ERROR_LOG,
            Severity.WARNING:  base_dir / WARNING_LOG,
            Severity.NORMAL:   base_dir / NORMAL_LOG,
            Severity.DEBUG:    base_dir / DEBUG_LOG,
        }

        self.events_summary = collections.defaultdict(int)
        self.events_summary_log = base_dir / SUMMARY_LOG

        super().__init__(_registry=_registry)

    def run(self) -> None:
        LOGGER.debug("Writing to %s", self.events_log)

        for log_file in chain((self.events_log, self.events_summary_log, ), self.events_logs_by_severity.values(), ):
            log_file.touch()

        for event_tuple in self.inbound_events():
            with verbose_suppress("EventsFileLogger failed to process %s", event_tuple):
                _, event = event_tuple  # try to unpack event from EventsDevice
                self.write_event(event=event)

    def write_event(self, event: SctEvent) -> None:
        if event.source_timestamp:
            message = f"{event.formatted_event_timestamp} <{event.formatted_source_timestamp}>: {str(event).strip()}"
        else:
            message = f"{event.formatted_event_timestamp}: {str(event).strip()}"

        message_bin = message.encode("utf-8") + b"\n"

        if event.severity not in (Severity.DEBUG, Severity.WARNING):
            # Log event to the console
            tee = getattr(LOGGER, logging.getLevelName(event.log_level).lower())
            if tee and not isinstance(event, TestResultEvent):
                with verbose_suppress("%s: failed to tee %s to %s", self, event, tee):
                    tee(message)

        # Write event to events.log file
        if getattr(event, 'save_to_files', False):
            with verbose_suppress("%s: failed to write %s to %s", self, event, self.events_log):
                with self.events_log.open("ab+", buffering=0) as fobj:
                    fobj.write(message_bin)

            if log_file := self.events_logs_by_severity.get(event.severity):
                with verbose_suppress("%s: failed to write %s to %s", self, event, log_file):
                    with log_file.open("ab+", buffering=0) as fobj:
                        fobj.write(message_bin)

        # Update summary.log file (statistics.)
        self.events_summary[Severity(event.severity).name] += 1
        with verbose_suppress("%s: failed to update %s", self, self.events_summary_log):
            with self.events_summary_log.open("wb", buffering=0) as fobj:
                fobj.write(json.dumps(dict(self.events_summary), indent=4).encode("utf-8"))

    def get_events_by_category(self, limit: Optional[int] = None) -> Dict[str, List[str]]:
        output = {}
        for severity, log_file in self.events_logs_by_severity.items():
            # Get first `limit' events with CRITICAL severity and last `limit' for other severities.
            events_bucket = (head if severity is Severity.CRITICAL else tail)(maxlen=limit)
            event = []
            try:
                with log_file.open() as fobj:
                    for line in fobj:
                        if line := line.rstrip():
                            if LINE_START_RE.match(line):
                                if event:
                                    events_bucket.append("\n".join(event))
                                event.clear()
                            event.append(line)
                if event:
                    events_bucket.append("\n".join(event))
            except Exception as exc:  # noqa: BLE001
                error_msg = f"{self}: failed to read {log_file}: {exc}"
                LOGGER.error(error_msg)
                if not events_bucket:
                    events_bucket.append(error_msg)
            output[severity.name] = list(events_bucket)
        return output


start_events_logger = partial(start_events_process, EVENTS_FILE_LOGGER_ID, EventsFileLogger)
get_events_logger = cast(Callable[..., EventsFileLogger], partial(get_events_process, EVENTS_FILE_LOGGER_ID))


def get_events_grouped_by_category(limit: Optional[int] = None,
                                   _registry: Optional[EventsProcessesRegistry] = None) -> Dict[str, List[str]]:
    return get_events_logger(_registry=_registry).get_events_by_category(limit=limit)


def get_logger_event_summary(_registry: Optional[EventsProcessesRegistry] = None) -> dict:
    events_summary_log = get_events_logger(_registry=_registry).events_summary_log
    with verbose_suppress("Failed to read %s", events_summary_log):
        with events_summary_log.open() as fobj:
            return json.load(fobj)
    return {}


__all__ = ("EventsFileLogger",
           "start_events_logger", "get_events_logger", "get_events_grouped_by_category", "get_logger_event_summary", )
