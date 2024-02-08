import copy

from sdcm.cluster_gce import GCENode
from sdcm.utils.gce_utils import GceLoggingClient
from unit_tests.test_events import BaseEventsTest


class FakeGceLogClient(GceLoggingClient):

    def __init__(self):
        pass

    def get_system_events(self, from_: float, until: float):
        """This is example output from GCE logging system in dictionary form."""
        entry = {
            'protoPayload': {
                '@type': 'type.googleapis.com/google.cloud.audit.AuditLog',
                'status': {
                    'message': 'Instance terminated by Compute Engine.'
                },
                'authenticationInfo': {
                    'principalEmail': 'system@google.com'
                },
                'serviceName': 'compute.googleapis.com',
                'methodName': 'compute.instances.hostError',
                'resourceName': 'projects/skilled-adapter-452/zones/us-east1-d/instances/longevity-10gb-3h-master-db-node-fac7b27a-0-6',
                'request': {
                    '@type': 'type.googleapis.com/compute.instances.hostError'
                }
            },
            'insertId': 'fjpkr2e6yi26',
            'resource': {
                'type': 'gce_instance',
                'labels': {
                        'zone': 'us-east1-d',
                        'project_id': 'skilled-adapter-452',
                        'instance_id': '2074079198322937303'
                }
            },
            'timestamp': '2022-06-30T14:07:23.102868Z',
            'severity': 'INFO',
            'logName': 'projects/skilled-adapter-452/logs/cloudaudit.googleapis.com%2Fsystem_event',
            'operation': {
                'id': 'systemevent-1656598023967-5e2aac8c11261-3553f149-42656705',
                'producer': 'compute.instances.hostError',
                'first': True,
                'last': True
            },
            'receiveTimestamp': '2022-06-30T14:07:23.983450844Z'
        }
        host_error = copy.deepcopy(entry)
        auto_restart_entry = copy.deepcopy(entry)
        auto_restart_entry['protoPayload']['methodName'] = 'compute.instances.automaticRestart'
        some_entry = copy.deepcopy(entry)
        some_entry['protoPayload']['methodName'] = 'compute.instances.some'
        print(host_error)
        return [host_error, auto_restart_entry, some_entry]


class FakeGceNode(GCENode):

    def __init__(self, logging_client: GceLoggingClient):
        self._gce_logging_client = logging_client
        self._last_logs_fetch_time = 1656590843.0


class TestGceErrorLog(BaseEventsTest):

    @staticmethod
    def logging_client():
        use_real_gce = False
        if use_real_gce:
            return GceLoggingClient(instance_name="longevity-10gb-3h-master-db-node-fac7b27a-0-6", zone="us-east1-d")
        else:
            return FakeGceLogClient()

    def test_host_error_log_entry_creates_sct_error_event(self):
        node = FakeGceNode(logging_client=self.logging_client())
        with self.wait_for_n_events(self.get_events_logger(), count=3, timeout=10):
            node.check_spot_termination()
        error_events = self.get_event_log_file("error.log")
        assert "compute.instances.hostError on node longevity-10gb-3h-master-db-node-fac7b27a-0-6 " \
               "at 2022-06-30" in error_events
        assert "compute.instances.automaticRestart on node longevity-10gb-3h-master-db-node-fac7b27a-0-6 " \
               "at 2022-06-30 " in error_events
        warning_events = self.get_event_log_file("warning.log")
        assert "compute.instances.some on node longevity-10gb-3h-master-db-node-fac7b27a-0-6 " \
               "at 2022-06-30" in warning_events
