import threading
from types import SimpleNamespace
from unittest import mock

import pytest

from sdcm.remote.kubernetes_cmd_runner import KubernetesRunner


def _runner() -> KubernetesRunner:
    runner = KubernetesRunner.__new__(KubernetesRunner)
    runner._ws_lock = threading.RLock()
    runner._k8s_core_v1_api = mock.Mock()
    runner.context = SimpleNamespace(
        config=SimpleNamespace(k8s_pod_name="pod", k8s_container="scylla", k8s_namespace="scylla")
    )
    return runner


def test_exec_failure_without_response_body_is_a_connection_error():
    body_less = AttributeError("'NoneType' object has no attribute 'decode'")
    with mock.patch("sdcm.remote.kubernetes_cmd_runner.k8s.stream.stream", side_effect=body_less):
        with pytest.raises(ConnectionError, match="without a response body"):
            _runner().start("true", "/bin/sh", {})


def test_other_attribute_errors_still_raise():
    with mock.patch("sdcm.remote.kubernetes_cmd_runner.k8s.stream.stream", side_effect=AttributeError("other")):
        with pytest.raises(AttributeError, match="other"):
            _runner().start("true", "/bin/sh", {})
