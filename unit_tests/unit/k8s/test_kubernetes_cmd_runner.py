import threading
from types import SimpleNamespace
from unittest import mock

import pytest

from sdcm.remote.kubernetes_cmd_runner import KubernetesRunner


def build_runner() -> KubernetesRunner:
    """KubernetesRunner with a mocked core API and pod context, built without running its constructor."""
    runner = KubernetesRunner.__new__(KubernetesRunner)
    runner._ws_lock = threading.RLock()
    runner._k8s_core_v1_api = mock.Mock()
    runner.context = SimpleNamespace(
        config=SimpleNamespace(k8s_pod_name="pod", k8s_container="scylla", k8s_namespace="scylla")
    )
    return runner


def test_exec_failure_without_response_body_is_a_connection_error():
    """start() turns an exec failure without a response body into ConnectionError."""
    body_less = AttributeError("'NoneType' object has no attribute 'decode'")
    with (
        mock.patch("sdcm.remote.kubernetes_cmd_runner.k8s.stream.stream", side_effect=body_less),
        pytest.raises(ConnectionError, match="without a response body"),
    ):
        build_runner().start("true", "/bin/sh", {})


def test_other_attribute_errors_still_raise():
    """start() re-raises AttributeErrors that are not the missing-response-body case."""
    with (
        mock.patch("sdcm.remote.kubernetes_cmd_runner.k8s.stream.stream", side_effect=AttributeError("other")),
        pytest.raises(AttributeError, match="other"),
    ):
        build_runner().start("true", "/bin/sh", {})
