from unittest import mock

import pytest

from sdcm.cluster_k8s.mini_k8s import DEFAULT_KUBECTL_VERSION, MinimalClusterBase


@pytest.mark.parametrize(
    "ok,stdout,expected",
    [
        pytest.param(True, '{"clientVersion": {"gitVersion": "v1.31.4"}, "kustomizeVersion": "v5.4.2"}', "1.31.4"),
        pytest.param(False, "", DEFAULT_KUBECTL_VERSION, id="kubectl-missing"),
    ],
)
def test_local_kubectl_version(ok, stdout, expected):
    result = mock.Mock(ok=ok, stdout=stdout)
    with mock.patch("sdcm.cluster_k8s.mini_k8s.LOCALRUNNER") as runner:
        runner.run.return_value = result
        assert MinimalClusterBase.local_kubectl_version.func(None) == expected
