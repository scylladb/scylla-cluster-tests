from unittest import mock

import pytest
import yaml

from sdcm.cluster_k8s.mini_k8s import (
    DEFAULT_KUBECTL_VERSION,
    POOL_LABEL_NAME,
    LocalKindCluster,
    MinimalClusterBase,
    scylla_worker_reserved_cpus,
)


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


@pytest.mark.parametrize(
    "workers,host_cpus,expected",
    [
        pytest.param(4, 16, ["0", "0-1", "0-2", "0-3"], id="cpu-per-worker"),
        pytest.param(4, 5, ["0", "0-1", "0-2", "0-3"], id="just-enough-cpus"),
        pytest.param(4, 4, [None] * 4, id="too-few-cpus"),
    ],
)
def test_scylla_worker_reserved_cpus(workers, host_cpus, expected):
    assert [scylla_worker_reserved_cpus(index, workers, host_cpus) for index in range(workers)] == expected


def _kind_cluster_spec(host_cpus: str) -> dict:
    kind = mock.Mock(spec=LocalKindCluster)
    for name in ("AUXILIARY_POOL_NAME", "SCYLLA_POOL_NAME", "LOADER_POOL_NAME", "MONITORING_POOL_NAME"):
        setattr(kind, name, getattr(LocalKindCluster, name))
    kind.params = {"k8s_n_auxiliary_nodes": 1, "n_db_nodes": [3], "n_loaders": [1], "k8s_n_monitor_nodes": 0}
    kind.host_node.remoter.run.return_value = mock.Mock(stdout=f"{host_cpus}\n")
    LocalKindCluster.start_k8s_software(kind)
    script = kind.host_node.remoter.run.call_args.args[0]
    return yaml.safe_load(script.split("EndOfSpec\n", 1)[1].split("EndOfSpec", 1)[0])


def test_kind_spec_reserves_different_cpus_per_scylla_worker():
    spec = _kind_cluster_spec(host_cpus="16")
    assert "cpuManagerPolicy: static" in spec["kubeadmConfigPatches"][0]
    reserved = {}
    for node in spec["nodes"]:
        if node["role"] != "worker":
            continue
        pool = node["labels"][POOL_LABEL_NAME]
        patches = [yaml.safe_load(patch) for patch in node.get("kubeadmConfigPatches", [])]
        reserved.setdefault(pool, []).extend(
            arg["value"] for patch in patches for arg in patch["nodeRegistration"]["kubeletExtraArgs"]
        )
    assert reserved == {
        LocalKindCluster.AUXILIARY_POOL_NAME: [],
        LocalKindCluster.SCYLLA_POOL_NAME: ["0", "0-1", "0-2"],
        LocalKindCluster.LOADER_POOL_NAME: [],
    }


def test_kind_spec_without_enough_cpus_has_no_reserved_cpus():
    spec = _kind_cluster_spec(host_cpus="3")
    assert not any("kubeadmConfigPatches" in node for node in spec["nodes"] if node["role"] == "worker")


def test_kind_load_image_falls_back_to_single_platform_archive():
    kind = mock.Mock(spec=LocalKindCluster)
    LocalKindCluster.kind_load_image(kind, "docker.io/scylladb/scylla:2026.1")
    cmd = kind.host_node.remoter.run.call_args.args[0]
    assert cmd.startswith("/var/tmp/kind load docker-image docker.io/scylladb/scylla:2026.1 || ")
    assert "docker save --platform linux/amd64 docker.io/scylladb/scylla:2026.1" in cmd
