import unittest.mock

import pytest

from sdcm import sct_config
from sdcm.sct_events import events_processes


@pytest.fixture(scope="function", autouse=True)
def fixture_mock_calls(monkeypatch):
    with unittest.mock.patch("sdcm.tester.validate_raft_on_nodes"):
        yield

    # clear the events processes registry after each test, so next test would be ableto start it fresh
    events_processes._EVENTS_PROCESSES = None


@pytest.fixture(scope="function", autouse=True)
def params(request: pytest.FixtureRequest, monkeypatch):
    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.delenv("USERPROFILE", raising=False)
    if sct_config_marker := request.node.get_closest_marker("sct_config"):
        config_files = sct_config_marker.kwargs.get('files')
        monkeypatch.setenv('SCT_CONFIG_FILES', config_files)

    monkeypatch.setenv('SCT_CLUSTER_BACKEND', "docker")
    monkeypatch.setenv('SCT_NEMESIS_INTERVAL', "1")
    params = sct_config.SCTConfiguration()

    yield params
