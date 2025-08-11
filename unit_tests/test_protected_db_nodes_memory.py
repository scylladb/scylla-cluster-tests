import pytest
from unittest.mock import MagicMock, patch
from sdcm.teardown_validators.protected_db_nodes_memory import ProtectedDbNodesMemoryValidator


@pytest.fixture
def mock_tester():
    class Node:
        def __init__(self, is_protected):
            self.is_protected = is_protected

    class DbCluster:
        nodes = [Node(True), Node(False), Node(True)]
    tester = MagicMock()
    tester.db_cluster = DbCluster()
    tester.events_processes_registry = None
    return tester


@pytest.fixture
def validator(mock_tester):
    params = {"protected_db_nodes": [0, 2],
              "teardown_validators.protected_db_nodes_memory": {"enabled": True},
              }
    v = ProtectedDbNodesMemoryValidator(params=params, tester=mock_tester)
    return v


@patch.object(ProtectedDbNodesMemoryValidator, 'get_memory_usage')
@patch.object(ProtectedDbNodesMemoryValidator, 'take_grafana_memory_screenshot')
def test_validate_calls_memory_methods(mock_screenshot, mock_memory, validator):
    """Test that validate() calls get_memory_usage and take_grafana_memory_screenshot for each protected node."""
    validator.validate()
    assert mock_memory.call_count == 2
    assert mock_screenshot.call_count == 2


@patch('sdcm.teardown_validators.protected_db_nodes_memory.get_events_grouped_by_category')
def test_get_active_nemesis_names(mock_get_events, validator):
    """Test get_active_nemesis_names returns correct nemesis mapping for anomaly timestamps."""
    mock_get_events.return_value = {
        'NORMAL': [
            "2025-07-31 11:31:00.441: (DisruptionEvent Severity.NORMAL) period_type=begin event_id=decaa7b4-676c-4cd7-9233-092709778b4c: nemesis_name=MultipleHardRebootNode target_node=Node longevity-tls-50gb-3d-protecte-db-node-6f66d856-6 [13.50.166.248 | 10.0.13.56] (rack: RACK2)",
            "2025-07-31 11:36:47.550: (DisruptionEvent Severity.NORMAL) period_type=end event_id=decaa7b4-676c-4cd7-9233-092709778b4c duration=5m47s: nemesis_name=MultipleHardRebootNode target_node=Node longevity-tls-50gb-3d-protecte-db-node-6f66d856-6 [13.50.166.248 | 10.0.13.56] (rack: RACK2)",
            "2025-07-31 13:08:06.613: (DisruptionEvent Severity.NORMAL) period_type=begin event_id=4eaf4501-ae82-4873-99ac-9c530ef7dcb0: nemesis_name=MultipleHardRebootNode target_node=Node longevity-tls-50gb-3d-protecte-db-node-6f66d856-6 [13.50.166.248 | 10.0.13.56] (rack: RACK2)",
        ],
        'ERROR': [
            """2025-07-31 13:13:30.150: (DisruptionEvent Severity.ERROR) period_type=end event_id=4eaf4501-ae82-4873-99ac-9c530ef7dcb0 duration=5m23s: nemesis_name=MultipleHardRebootNode target_node=Node longevity-tls-50gb-3d-protecte-db-node-6f66d856-6 [13.50.166.248 | 10.0.13.56] (rack: RACK2) errors=Timeout occurred while waiting for end log line ['(?:storage_service|sstables_loader) - Done loading new SSTables for keyspace=keyspace1, table=standard1, load_and_stream=true.*status=(.*)'] on node: longevity-tls-50gb-3d-protecte-db-node-6f66d856-7
Traceback (most recent call last):
  File "/home/ubuntu/scylla-cluster-tests/sdcm/nemesis.py", line 5565, in wrapper
    result = method(*args, **kwargs)
  File "/home/ubuntu/scylla-cluster-tests/sdcm/nemesis.py", line 1585, in disrupt_load_and_stream
    SstableLoadUtils.run_load_and_stream(load_on_node, **kwargs)
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/ubuntu/scylla-cluster-tests/sdcm/utils/sstable/load_utils.py", line 112, in run_load_and_stream
    with wait_for_log_lines(node, start_line_patterns=[cls.LOAD_AND_STREAM_RUN_EXPR],
         ~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                            end_line_patterns=[cls.LOAD_AND_STREAM_DONE_EXPR.format(keyspace_name, table_name)],
                            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                            start_timeout=start_timeout, end_timeout=end_timeout):
                            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/usr/local/lib/python3.13/contextlib.py", line 148, in __exit__
    next(self.gen)
    ~~~~^^^^^^^^^^
  File "/home/ubuntu/scylla-cluster-tests/sdcm/wait.py", line 168, in wait_for_log_lines
    raise TimeoutError(end_ctx)
TimeoutError: Timeout occurred while waiting for end log line ['(?:storage_service|sstables_loader) - Done loading new SSTables for keyspace=keyspace1, table=standard1, load_and_stream=true.*status=(.*)'] on node: longevity-tls-50gb-3d-protecte-db-node-6f66d856-7"""
        ]
    }
    anomaly_timestamps = [1753952200.55, 1753961460.150, 1753967602]
    result = validator.get_active_nemesis_names(anomaly_timestamps)
    assert isinstance(result, dict)
    assert result == {1753952200.55: "No nemesis running",
                      1753961460.150: "MultipleHardRebootNode",
                      1753967602: "MultipleHardRebootNode"}


@patch('sdcm.teardown_validators.protected_db_nodes_memory.detect_isolation_forest_anomalies')
@patch.object(ProtectedDbNodesMemoryValidator, 'get_active_nemesis_names')
@patch('sdcm.teardown_validators.protected_db_nodes_memory.ValidatorEvent')
def test_detect_anomalies_triggers_event(mock_event, mock_nemesis, mock_anomalies, validator):
    """Test that detect_anomalies publishes an event when anomalies are detected."""
    mock_anomalies.return_value = [
        (0, (1620000000, 1000000), 'anomaly', 0.8),
        (1, (1620000600, 2000000), 'anomaly', 0.9)
    ]
    mock_nemesis.return_value = {
        1620000000: "NemesisA",
        1620000600: "NemesisB"
    }
    node = MagicMock()
    metric_name = "scylla_lsa_total_space_bytes"
    prom_values = [(1620000000, 1000000), (1620000600, 2000000)]
    validator.detect_anomalies(prom_values, node, metric_name)
    assert mock_event.call_count == 1
    args, kwargs = mock_event.call_args
    assert "Memory usage anomalies detected" in kwargs['message']
    assert kwargs['severity'].name == "ERROR"


@patch('sdcm.teardown_validators.protected_db_nodes_memory.detect_isolation_forest_anomalies')
@patch.object(ProtectedDbNodesMemoryValidator, 'get_active_nemesis_names')
@patch('sdcm.teardown_validators.protected_db_nodes_memory.ValidatorEvent')
def test_detect_anomalies_no_anomaly_no_event(mock_event, mock_nemesis, mock_anomalies, validator):
    """Test that detect_anomalies does not publish an event when no anomalies are detected."""
    mock_anomalies.return_value = []
    node = MagicMock()
    metric_name = "scylla_lsa_total_space_bytes"
    prom_values = [(1620000000, 1000000), (1620000600, 2000000)]
    validator.detect_anomalies(prom_values, node, metric_name)
    assert mock_event.call_count == 0
