from time import sleep

from sdcm.gce_log_monitor import GceLoggingClient, log_gce_errors_as_events

from unit_tests.test_events import BaseEventsTest


class FakeGceLogClient(GceLoggingClient):  # pylint: disable=too-few-public-methods

    def __init__(self):  # pylint: disable=super-init-not-called
        pass

    def get_error_logs(self, since: int):
        """This is example output from GCE logging system in dictionary form."""
        return [
            {
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
        ]


class TestGceErrorLog(BaseEventsTest):

    @staticmethod
    def logging_client():
        use_real_gce = False
        if use_real_gce:
            return GceLoggingClient()
        else:
            return FakeGceLogClient()

    def test_host_error_log_entry_creates_sct_error_event(self):
        with self.wait_for_n_events(self.get_events_logger(), count=1, timeout=1):
            log_gce_errors_as_events(test_id="fac7b27a", test_start_time=1656590843.0,
                                     logging_client=self.logging_client())

        log_content = self.get_event_log_file("error.log")
        assert "compute.instances.hostError on node longevity-10gb-3h-master-db-node-fac7b27a-0-6 " \
               "at 2022-06-30 16:07:23.102868+02:00: Instance terminated by Compute Engine." in log_content

    def test_no_event_is_created_for_not_matching_test_id(self):
        log_content_before = self.get_event_log_file("error.log")
        log_gce_errors_as_events(test_id="other_test_id", test_start_time=1656590843.0,
                                 logging_client=self.logging_client())
        sleep(0.1)
        log_content = self.get_event_log_file("error.log")
        assert log_content == log_content_before, "No error should be logged for mismatched test"
