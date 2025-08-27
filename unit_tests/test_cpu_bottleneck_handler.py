import pytest
from unittest.mock import MagicMock, patch
from sdcm.sct_events.handlers.cpu_bottleneck import CpuBottleneckHandler
from sdcm.sct_events.loaders import CassandraStressLogEvent
from sdcm.remote.remote_base import RemoteCmdRunnerBase

class DummyNode:
    def __init__(self, name):
        self.name = name
        self.ssh_login_info = 'dummy_ssh_info'
        self.remoter = MagicMock(spec=RemoteCmdRunnerBase)
    def install_package(self, pkg):
        self.installed = pkg

class DummyClusterTester:
    def __init__(self, nodes, params=None, test_id='testid'):
        self.db_cluster = MagicMock(spec="sdcm.cluster.BaseScyllaCluster")
        self.db_cluster.nodes = nodes
        self.params = params or {}
        self.test_id = test_id

@pytest.fixture
def dummy_event():
    event = MagicMock(spec=CassandraStressLogEvent)
    event.node = 'node1'
    return event

@pytest.fixture
def dummy_nodes():
    return [DummyNode('node1'), DummyNode('node2')]

@pytest.fixture
def dummy_tester(dummy_nodes):
    return DummyClusterTester(nodes=dummy_nodes, params={'cluster_backend': 'gce'}, test_id='tid')

@patch('sdcm.sct_events.handlers.cpu_bottleneck.upload_to_s3', return_value='s3://dummy-link')
@patch('sdcm.sct_events.handlers.cpu_bottleneck.create_proxy_argus_s3_url', return_value='proxy-url')
def test_handle_runs_perf_and_upload(mock_argus_url, mock_upload, dummy_event, dummy_tester, dummy_nodes):
    """
    Test that CpuBottleneckHandler runs perf commands and uploads results to S3.
    Verifies perf record/report are called and upload_to_s3 is triggered.
    """
    handler = CpuBottleneckHandler()
    # Simulate perf record/report
    for node in dummy_nodes:
        node.remoter.sudo.return_value = 'ok'
    handler.handle(dummy_event, dummy_tester)
    # Should call perf record/report and upload for node2 only
    dummy_nodes[1].remoter.sudo.assert_any_call(
        'perf record -a --call-graph=dwarf -F 99 -p $(pidof scylla) sleep 30', timeout=40)
    dummy_nodes[1].remoter.sudo.assert_any_call(
        'perf report -f --no-children --stdio > perf.report.txt', timeout=30)
    assert mock_upload.called
    assert mock_argus_url.called

@patch('sdcm.sct_events.handlers.cpu_bottleneck.upload_to_s3', return_value='')
def test_handle_upload_failure(mock_upload, dummy_event, dummy_tester, dummy_nodes):
    """
    Test that CpuBottleneckHandler handles upload failures gracefully.
    Verifies upload_to_s3 is called even if it returns an empty string.
    """
    handler = CpuBottleneckHandler()
    CpuBottleneckHandler._last_run = {}
    for node in dummy_nodes:
        node.remoter.sudo.return_value = 'ok'
    handler.handle(dummy_event, dummy_tester)
    assert mock_upload.called

@patch('sdcm.sct_events.handlers.cpu_bottleneck.upload_to_s3', return_value='s3://dummy-link')
def test_handle_skips_if_interval_not_passed(mock_upload, dummy_event, dummy_tester, dummy_nodes):
    """
    Test that CpuBottleneckHandler skips perf execution if minimum interval has not passed.
    Verifies perf is not called for nodes with recent _last_run.
    """
    handler = CpuBottleneckHandler()
    handler._last_run[dummy_nodes[1].name] = 1e10  # Simulate recent run
    handler.handle(dummy_event, dummy_tester)
    # Should not call perf for node2
    dummy_nodes[1].remoter.sudo.assert_not_called()
