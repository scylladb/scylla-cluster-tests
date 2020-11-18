import threading
import logging
import re
import json
from typing import Iterable
from datetime import datetime

from sdcm.utils.file import File
from sdcm.utils.decorators import timeout
from sdcm.sct_events import SctEvent, Severity


class ScyllaOperatorLogEvent(SctEvent):
    def __init__(self, timestamp=None, namespace=None, cluster=None, message=None, error=None, trace_id=None):
        super().__init__()
        self.severity = Severity.ERROR
        self.namespace = namespace
        self.cluster = cluster
        self.message = message
        self.timestamp = timestamp
        self.error = error
        self.trace_id = trace_id

    def __str__(self):
        cluster = f'/{self.cluster}' if self.cluster else ''
        return f"{super().__str__()} {self.trace_id} {self.namespace}{cluster}: {self.message}, {self.error}"


class ScyllaOperatorRestartEvent(SctEvent):
    def __init__(self, restart_count):
        super().__init__()
        self.severity = Severity.ERROR
        self.restart_count = restart_count

    def __str__(self):
        return f"{super().__str__()}: Scylla operator has been restarted, restart_count={self.restart_count}"


class ScyllaOperatorLogMonitoring(threading.Thread):
    lookup_time = 0.1
    log = logging.getLogger('ScyllaOperatorLogMonitoring')
    patterns = [re.compile('^\s*{\s*"L"\s*:\s*"ERROR"')]
    event_data_mapping = {
        'T': 'timestamp',
        'N': 'namespace',
        'M': 'message',
        'cluster': 'cluster',
        'error': 'error',
        '_trace_id': 'trace_id'
    }

    def __init__(self, kluster: 'KubernetesCluster'):
        self.termination_event = threading.Event()
        self.current_position = 0
        self.kluster = kluster
        super().__init__(daemon=True)

    @timeout(timeout=300, message='Wait for file to be available')
    def _get_file_follower(self) -> Iterable[str]:
        return File(self.kluster.scylla_operator_log, 'r').read_lines_filtered(*self.patterns)

    def run(self):
        file_follower = self._get_file_follower()
        while not self.termination_event.wait(self.lookup_time):
            self.check_logs(file_follower)
        self.check_logs(file_follower)

    def check_logs(self, file_follower: Iterable[str]):
        for log_record in file_follower:
            try:
                log_data = json.loads(log_record)
            except Exception as exc:
                self.log.error(f'Failed to interpret following log line:\n{log_record}\n, due to the: {str(exc)}')
                continue
            event_data = {}
            for log_en_name, attr_name in self.event_data_mapping.items():
                log_en_data = log_data.get(log_en_name, None)
                if log_en_data is None:
                    continue
                if attr_name == 'timestamp':
                    log_en_data = datetime.strptime(log_en_data, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
                event_data[attr_name] = log_en_data
            evt = 'Event generation failed'
            try:
                evt = ScyllaOperatorLogEvent(**event_data)
                evt.publish()
            except Exception as exc:
                self.log.error(f'Failed to publish event {str(evt)}\n, due to the: {str(exc)}')

    def stop(self):
        self.termination_event.set()


class ScyllaOperatorStatusMonitoring(threading.Thread):
    status_check_period = 10
    log = logging.getLogger('ScyllaOperatorStatusMonitoring')

    def __init__(self, kluster: 'KubernetesCluster'):
        self.termination_event = threading.Event()
        self.kluster = kluster
        self.last_restart_count = 0
        super().__init__(daemon=True)

    def run(self):
        while not self.termination_event.wait(self.status_check_period):
            self.check_pod_status()
        self.check_pod_status()

    def check_pod_status(self):
        restart_happened = False
        try:
            status = self.kluster.operator_pod_status
            if status is None:
                return
            current_restart_count = status.container_statuses[0].restart_count
            if self.last_restart_count != current_restart_count:
                restart_happened = True
                self.last_restart_count = current_restart_count
        except Exception as exc:
            self.log.warning(f'Failed to get scylla operator status, due to the: {str(exc)}')
        if restart_happened:
            try:
                ScyllaOperatorRestartEvent(self.last_restart_count).publish()
            except Exception as exc:
                self.log.warning(f'Failed to publish event: {str(exc)}')

    def stop(self):
        self.termination_event.set()
