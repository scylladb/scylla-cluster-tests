import json
import time
from datetime import datetime
from typing import Union
from sdcm.sct_events import SpotTerminationEvent
from sdcm.services.base import NodeThreadService


class AWSSpotMonitoringThread(NodeThreadService):
    _interval = 15

    def _service_body(self) -> Union[int, float]:
        try:
            result = self._node.remoter.run(
                'curl http://169.254.169.254/latest/meta-data/spot/instance-action', verbose=False)
            status = result.stdout.strip()
        except Exception as details:  # pylint: disable=broad-except
            self._log.warning('Error during getting spot termination notification %s', details)
            return 0
        if '404 - Not Found' in status:
            return 0
        self._log.warning('Got spot termination notification from AWS %s', status)
        terminate_action = json.loads(status)
        terminate_action_timestamp = time.mktime(
            datetime.strptime(terminate_action['time'], "%Y-%m-%dT%H:%M:%SZ").timetuple())
        next_check_delay = terminate_action['time-left'] = terminate_action_timestamp - time.time()
        SpotTerminationEvent(node=self, message=terminate_action)
        return max(next_check_delay - self._interval, 0)


class GCESpotMonitoringThread(NodeThreadService):
    _interval = 15

    def __init__(self, node):
        self._preempted_last_state = False
        super().__init__(node)

    def _service_body(self) -> Union[int, float]:
        """Check if a spot instance termination was initiated by the cloud.

        There are few different methods how to detect this event in GCE:

            https://cloud.google.com/compute/docs/instances/create-start-preemptible-instance#detecting_if_an_instance_was_preempted

        but we use internal metadata because the getting of zone operations is not implemented in Apache Libcloud yet.
        """
        try:
            result = self._node.remoter.run(
                'curl "http://metadata.google.internal/computeMetadata/v1/instance/preempted'
                '?wait_for_change=true&timeout_sec=%d" -H "Metadata-Flavor: Google"'
                % self._interval, verbose=False)
            status = result.stdout.strip()
        except Exception as details:  # pylint: disable=broad-except
            self._log.warning('Error during getting spot termination notification %s', details)
            return 0
        preempted = status.lower() == 'true'
        if preempted and not self._preempted_last_state:
            self._log.warning('Got spot termination notification from GCE')
            SpotTerminationEvent(node=self, message='Instance was preempted.')
        self._preempted_last_state = preempted
        return self._interval
