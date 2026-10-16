from unittest import mock

import pytest
import requests

from sdcm.cluster_k8s import (
    PROMETHEUS_OPERATOR_CONFIG_PATH,
    SCYLLA_OPERATOR_NAMESPACE,
    KubernetesCluster,
)

BUNDLE_URL = (
    "https://raw.githubusercontent.com/scylladb/scylla-operator/{}/examples/third-party/prometheus-operator.yaml"
)


def _cluster(**attrs) -> mock.Mock:
    return mock.Mock(spec=KubernetesCluster, **attrs)


@pytest.mark.parametrize(
    "chart_version,git_ref",
    [
        pytest.param("v1.22.0", "v1.22.0", id="release"),
        pytest.param("v1.22.0-13-g5aa939b-latest", "5aa939b", id="latest"),
    ],
)
def test_prometheus_operator_config_from_operator_repo(tmp_path, chart_version, git_ref):
    with mock.patch("sdcm.cluster_k8s.create_retry_session") as session:
        session.return_value.get.return_value = mock.Mock(text="kind: Bundle\n")
        path = KubernetesCluster._get_prometheus_operator_config(
            _cluster(scylla_operator_chart_version=chart_version), str(tmp_path)
        )
    session.return_value.get.assert_called_once_with(BUNDLE_URL.format(git_ref), timeout=120)
    assert open(path, encoding="utf-8").read() == "kind: Bundle\n"


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(requests.HTTPError("404 Not Found"), id="no-bundle-in-repo"),
        pytest.param(requests.ConnectionError("unreachable"), id="network-error"),
    ],
)
def test_prometheus_operator_config_falls_back_to_sct_bundle(tmp_path, error):
    with mock.patch("sdcm.cluster_k8s.create_retry_session") as session:
        session.return_value.get.return_value.raise_for_status.side_effect = error
        session.return_value.get.side_effect = None if isinstance(error, requests.HTTPError) else error
        path = KubernetesCluster._get_prometheus_operator_config(
            _cluster(scylla_operator_chart_version="v1.22.0"), str(tmp_path)
        )
    assert path == PROMETHEUS_OPERATOR_CONFIG_PATH


@pytest.mark.parametrize("crds_existed,restarted", [(False, True), (True, False)])
def test_deploy_prometheus_operator_restarts_scylla_operator_only_for_new_crds(crds_existed, restarted):
    cluster = _cluster()
    cluster.kubectl.side_effect = lambda cmd, **_: mock.Mock(ok=crds_existed if cmd.startswith("get crd") else True)
    with mock.patch("sdcm.cluster_k8s.time.sleep"):
        KubernetesCluster.deploy_prometheus_operator(cluster)
    restart = mock.call("rollout restart deployment scylla-operator", namespace=SCYLLA_OPERATOR_NAMESPACE)
    assert (restart in cluster.kubectl.call_args_list) is restarted


@pytest.mark.parametrize(
    "backend,stdout",
    [
        pytest.param("k8s-eks", "10.4.0.1\n10.4.0.2\n", id="two-grafana-pods-during-rollout"),
        pytest.param("k8s-local-kind", "10.4.0.1\n", id="service-ip"),
    ],
)
def test_get_grafana_ip_takes_first(backend, stdout):
    cluster = _cluster(cluster_backend=backend)
    cluster.kubectl.return_value = mock.Mock(stdout=stdout)
    assert KubernetesCluster.get_grafana_ip(cluster, "sct-cluster", "scylla") == "10.4.0.1"


@pytest.mark.parametrize(
    "backend,has_aws_credentials,secret_created",
    [
        pytest.param("k8s-local-kind", False, False, id="kind-without-credentials"),
        pytest.param("k8s-local-kind", True, True, id="kind-with-credentials"),
        pytest.param("k8s-eks", False, True, id="eks-always"),
        pytest.param("k8s-gke", False, True, id="gke-always"),
    ],
)
def test_docker_hub_auth_secret_needs_credentials_only_on_cloud(backend, has_aws_credentials, secret_created):
    cluster = _cluster(cluster_backend=backend, docker_hub_auth_secret="docker-auth")
    creds = {"username": "user", "password": "pass", "email": "user@example.com"}
    with (
        mock.patch("sdcm.cluster_k8s.aws_credentials_available", return_value=has_aws_credentials),
        mock.patch("sdcm.cluster_k8s.get_docker_hub_credentials", return_value=creds) as get_creds,
    ):
        KubernetesCluster.create_docker_hub_auth_secret(cluster, "scylla")
    assert get_creds.called is secret_created
    assert cluster.kubectl.called is secret_created
