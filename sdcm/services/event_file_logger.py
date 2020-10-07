import collections
import json
import os
import re
from pathlib import Path

from sdcm.sct_events import Severity, TestResultEvent, SctEvent
from sdcm.services.event_device import EventsDevice
from sdcm.services.base import DetachProcessService


class EventsFileLogger(DetachProcessService):  # pylint: disable=too-many-instance-attributes
    stop_priority = 90
    _fd_events = None
    _fd_events_summary = None
    _interval = 0
    tags = ['core']

    def __init__(self, log_dir, events_device: EventsDevice, flush_logs: bool = False):
        self._events_device = events_device
        self._test_pid = os.getpid()
        self.event_log_base_dir = Path(log_dir, 'events_log')
        self.events_filename = Path(self.event_log_base_dir, 'events.log')
        self.critical_events_filename = Path(self.event_log_base_dir, 'critical.log')
        self.error_events_filename = Path(self.event_log_base_dir, 'error.log')
        self.warning_events_filename = Path(self.event_log_base_dir, 'warning.log')
        self.normal_events_filename = Path(self.event_log_base_dir, 'normal.log')
        self.events_summary_filename = Path(self.event_log_base_dir, 'summary.log')
        self.flush_logs = flush_logs

        for log_file in [self.critical_events_filename, self.error_events_filename,
                         self.warning_events_filename, self.normal_events_filename,
                         self.events_summary_filename]:
            log_file.touch()

        self.level_to_file_mapping = {
            Severity.CRITICAL: self.critical_events_filename,
            Severity.ERROR: self.error_events_filename,
            Severity.WARNING: self.warning_events_filename,
            Severity.NORMAL: self.normal_events_filename,
        }
        self._fds = {}
        self.level_summary = collections.defaultdict(int)
        super().__init__()

    def _flush_fds(self):
        if self._fd_events:
            self._fd_events.flush()
        for log_file in self._fds.values():
            log_file.flush()
        if self._fd_events_summary:
            self._fd_events_summary.flush()

    def _service_on_before_start(self):
        self._fd_events = self.events_filename.open('a+')
        self._fd_events_summary = self.events_summary_filename.open('w')
        for severity, log_file in self.level_to_file_mapping.items():
            self._fds[severity] = log_file.open('a+')

    def _save_summary(self):
        self._fd_events_summary.seek(0)
        json.dump(dict(self.level_summary), self._fd_events_summary, indent=4)

    def _service_body(self):
        self._log.info("writing to %s", self.events_filename)
        for _, message_data in self._events_device.subscribe_events(stop_event=self._termination_event):
            try:
                msg = self._dump_event_into_files(message_data, use_fds=True)
                if not isinstance(message_data, TestResultEvent):
                    self._log.info(msg)
            except Exception:  # pylint: disable=broad-except
                self._log.exception("Failed to write event to event.log")
            if self.flush_logs:
                self._flush_fds()
        self._flush_fds()

    def dump_event_into_files(self, message_data: SctEvent) -> str:
        return self._dump_event_into_files(message_data, use_fds=False)

    def _dump_event_into_files(self, message_data: SctEvent, use_fds: bool) -> str:
        msg = "{}: {}".format(message_data.formatted_timestamp, str(message_data).strip())
        if use_fds:
            self._fd_events.write(msg + '\n')
        else:
            with open(self.events_filename, 'a+') as log_file:
                log_file.write(msg + '\n')
        # update each level log file
        if use_fds:
            self._fds[message_data.severity].write(msg + '\n')
        else:
            with open(self.level_to_file_mapping[message_data.severity], 'a+') as events_level_file:
                events_level_file.write(msg + '\n')
        # update the summary file
        self.level_summary[Severity(message_data.severity).name] += 1
        if use_fds:
            self._save_summary()
        else:
            with self.events_summary_filename.open('w') as summary_file:
                json.dump(dict(self.level_summary), summary_file, indent=4)
        return msg

    def get_events_by_category(self, limit=0):
        output = {}
        line_start = re.compile('^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] ')
        for severity, events_file_path in self.level_to_file_mapping.items():
            try:
                with events_file_path.open() as events_file:
                    output[severity.name] = events_bucket = []
                    event = ''
                    for event_data in events_file:
                        event_data = event_data.strip(' \n\t\r')
                        if not event_data:
                            continue
                        if not line_start.match(event_data):
                            event += '\n' + event_data
                            continue
                        if event:
                            if limit and len(events_bucket) >= limit:
                                events_bucket.pop(0)
                            events_bucket.append(event)
                        event = event_data
                    if event:
                        if limit and len(events_bucket) >= limit:
                            events_bucket.pop(0)
                        events_bucket.append(event)
            except Exception as exc:  # pylint: disable=broad-except
                self._log.info("failed to read %s file, due to the %s", events_file_path, str(exc))
                if not output.get(severity.name, None):
                    output[severity.name] = [f"failed to read {events_file_path} file, due to the {str(exc)}"]
        return output

    def kill(self):
        super().kill()
