import copy
import logging
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from sdcm.cluster_gce import SPOT_TERMINATION_CHECK_DELAY, GCENode
from sdcm.utils.gce_utils import GceLoggingClient
from unit_tests.lib.fake_events import FakeEventsMixin

MINICLOUD_ENV_VARS = ("AWS_ENDPOINT_URL", "GCE_ENDPOINT_URL", "SCT_MINICLOUD_ENDPOINT_URL")
MINICLOUD_PARAMS = {"minicloud_endpoint_url": "http://localhost:5000"}
QUERY_FAILURE_WARNING = "Failed to query GCE maintenance/preemption events from Cloud Logging"
MINICLOUD_SKIP_INFO = "Minicloud does not serve Cloud Logging"


@pytest.fixture(name="no_minicloud_env")
def no_minicloud_env_fixture(monkeypatch):
    for var in MINICLOUD_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


class FakeGceLogClient(GceLoggingClient):
    def __init__(self):
        pass

    def get_system_events(self, from_: float, until: float):
        """This is example output from GCE logging system in dictionary form."""
        entry = {
            "protoPayload": {
                "@type": "type.googleapis.com/google.cloud.audit.AuditLog",
                "status": {"message": "Instance terminated by Compute Engine."},
                "authenticationInfo": {"principalEmail": "system@google.com"},
                "serviceName": "compute.googleapis.com",
                "methodName": "compute.instances.hostError",
                "resourceName": "projects/skilled-adapter-452/zones/us-east1-d/instances/longevity-10gb-3h-master-db-node-fac7b27a-0-6",
                "request": {"@type": "type.googleapis.com/compute.instances.hostError"},
            },
            "insertId": "fjpkr2e6yi26",
            "resource": {
                "type": "gce_instance",
                "labels": {
                    "zone": "us-east1-d",
                    "project_id": "skilled-adapter-452",
                    "instance_id": "2074079198322937303",
                },
            },
            "timestamp": "2022-06-30T14:07:23.102868Z",
            "severity": "INFO",
            "logName": "projects/skilled-adapter-452/logs/cloudaudit.googleapis.com%2Fsystem_event",
            "operation": {
                "id": "systemevent-1656598023967-5e2aac8c11261-3553f149-42656705",
                "producer": "compute.instances.hostError",
                "first": True,
                "last": True,
            },
            "receiveTimestamp": "2022-06-30T14:07:23.983450844Z",
        }
        host_error = copy.deepcopy(entry)
        auto_restart_entry = copy.deepcopy(entry)
        auto_restart_entry["protoPayload"]["methodName"] = "compute.instances.automaticRestart"
        some_entry = copy.deepcopy(entry)
        some_entry["protoPayload"]["methodName"] = "compute.instances.some"
        migrate_entry = copy.deepcopy(entry)
        migrate_entry["protoPayload"]["methodName"] = "compute.instances.migrateOnHostMaintenance"
        terminate_entry = copy.deepcopy(entry)
        terminate_entry["protoPayload"]["methodName"] = "compute.instances.terminateOnHostMaintenance"
        print(host_error)
        return [host_error, auto_restart_entry, some_entry, migrate_entry, terminate_entry]


class FakeGceNode(GCENode):
    def __init__(self, logging_client: GceLoggingClient, params: dict | None = None):
        self._gce_logging_client = logging_client
        self._last_logs_fetch_time = 1656590843.0
        self.parent_cluster = SimpleNamespace(params=params or {})
        self.termination_event = threading.Event()
        self._spot_monitoring_thread = None
        self.log = logging.getLogger("FakeGceNode")


class TestGceErrorLog(FakeEventsMixin):
    @staticmethod
    def logging_client():
        use_real_gce = False
        if use_real_gce:
            return GceLoggingClient(instance_name="longevity-10gb-3h-master-db-node-fac7b27a-0-6", zone="us-east1-d")
        else:
            return FakeGceLogClient()

    def test_host_error_log_entry_creates_sct_error_event(self):
        node = FakeGceNode(logging_client=self.logging_client())
        node.check_spot_termination()

        # Check events captured in-memory by FakeEventsDevice
        events_by_category = self.events.get_events_by_category()
        error_events = "\n".join(events_by_category.get("ERROR", []))
        assert (
            "compute.instances.hostError on node longevity-10gb-3h-master-db-node-fac7b27a-0-6 "
            "at 2022-06-30" in error_events
        )
        assert (
            "compute.instances.automaticRestart on node longevity-10gb-3h-master-db-node-fac7b27a-0-6 "
            "at 2022-06-30 " in error_events
        )
        warning_events = "\n".join(events_by_category.get("WARNING", []))
        assert (
            "compute.instances.some on node longevity-10gb-3h-master-db-node-fac7b27a-0-6 "
            "at 2022-06-30" in warning_events
        )
        critical_events = "\n".join(events_by_category.get("CRITICAL", []))
        assert (
            "compute.instances.migrateOnHostMaintenance on node "
            "longevity-10gb-3h-master-db-node-fac7b27a-0-6 at 2022-06-30" in critical_events
        )
        assert (
            "compute.instances.terminateOnHostMaintenance on node "
            "longevity-10gb-3h-master-db-node-fac7b27a-0-6 at 2022-06-30" in critical_events
        )


def test_minicloud_skips_polling(caplog):
    client = MagicMock(spec=GceLoggingClient)
    node = FakeGceNode(logging_client=client, params=MINICLOUD_PARAMS)

    with caplog.at_level(logging.INFO, logger="FakeGceNode"):
        node.start_spot_monitoring_thread()

    assert node._spot_monitoring_thread is None
    client.get_system_events.assert_not_called()
    skip_records = [record for record in caplog.records if MINICLOUD_SKIP_INFO in record.getMessage()]
    assert [record.levelno for record in skip_records] == [logging.INFO]
    assert not [record for record in caplog.records if record.levelno >= logging.WARNING]


def test_real_gce_starts_spot_monitoring_thread(no_minicloud_env):
    client = MagicMock(spec=GceLoggingClient)
    client.get_system_events.return_value = []
    node = FakeGceNode(logging_client=client)
    node.termination_event.set()

    node.start_spot_monitoring_thread()

    assert node._spot_monitoring_thread is not None
    node._spot_monitoring_thread.join(timeout=5)
    assert not node._spot_monitoring_thread.is_alive()
    client.get_system_events.assert_called()


def test_cloud_logging_query_failure_logs_warning_and_keeps_fetch_time(caplog):
    client = MagicMock(spec=GceLoggingClient)
    client.get_system_events.side_effect = RuntimeError("404 entries.list not found")
    node = FakeGceNode(logging_client=client)
    last_fetch_time = node._last_logs_fetch_time

    with caplog.at_level(logging.WARNING, logger="FakeGceNode"):
        delay = node.check_spot_termination()

    assert delay == SPOT_TERMINATION_CHECK_DELAY
    assert node._last_logs_fetch_time == last_fetch_time
    warnings = [record for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert QUERY_FAILURE_WARNING in warnings[0].getMessage()
    assert "404 entries.list not found" in warnings[0].getMessage()
