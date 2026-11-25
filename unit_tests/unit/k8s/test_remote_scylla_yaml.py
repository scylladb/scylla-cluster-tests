import contextlib

import pytest
import yaml

from sdcm.cluster_k8s import SCYLLA_NAMESPACE, KubernetesCluster


class DummyCluster(KubernetesCluster):
    """KubernetesCluster stub that keeps scylla.yaml in an in-memory config map and skips real deployment."""

    POOL_LABEL_NAME = "dummy-pool"
    config_map_store = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config_map_store = {}

    @contextlib.contextmanager
    def scylla_config_map(self, namespace: str = SCYLLA_NAMESPACE):
        """Yield the in-memory config map dict, asserting it is requested from the Scylla namespace."""
        assert namespace == SCYLLA_NAMESPACE
        # Yield a real dict so context can mutate it
        yield self.config_map_store

    def deploy(self):
        """No-op: skip cluster deployment."""

    def create_kubectl_config(self):
        """No-op: skip kubectl config creation."""

    def create_token_update_thread(self):
        """Return None: no token update thread."""
        return None

    def deploy_node_pool(self, pool, wait_till_ready=True):
        """No-op: skip node pool deployment."""


@pytest.fixture()
def dummy_cluster():
    """DummyCluster on a local k8s backend, returned with its in-memory config map."""
    params = {
        "k8s_scylla_cluster_name": "test-cluster",
        "cluster_backend": "k8s-local",
    }
    cluster = DummyCluster(params=params, user_prefix="ut", region_name="region-1")

    return cluster, cluster.config_map_store


def test_remote_scylla_yaml_no_change_does_not_update(dummy_cluster, events):
    """Reading scylla.yaml without changes leaves the stored config and restart flag untouched.

    Note: events fixture is required to handle ScyllaYamlUpdateEvent publishing.
    """
    cluster, store = dummy_cluster
    # Pre-populate scylla.yaml with some content
    initial = {"endpoint_snitch": "org.apache.cassandra.locator.GossipingPropertyFileSnitch"}
    store["scylla.yaml"] = yaml.safe_dump(initial)

    # Use remote_scylla_yaml without changes
    with cluster.remote_scylla_yaml() as scylla_yml:
        # Access but don't modify
        _ = scylla_yml.endpoint_snitch

    # Should remain unchanged and restart not required
    assert yaml.safe_load(store["scylla.yaml"]) == initial
    assert cluster.scylla_restart_required is False


def test_remote_scylla_yaml_updates_and_sets_restart(dummy_cluster, events):
    """Modifying scylla.yaml via remote_scylla_yaml writes the config map and sets restart required.

    Note: events fixture is required to handle ScyllaYamlUpdateEvent publishing.
    """
    cluster, store = dummy_cluster

    # Initially empty config map
    assert "scylla.yaml" not in store

    # Update a value via context manager
    with cluster.remote_scylla_yaml() as scylla_yml:
        scylla_yml.endpoint_snitch = "org.apache.cassandra.locator.GossipingPropertyFileSnitch"
        scylla_yml.experimental_features = ["hinted_handoff"]

    # After exiting context, config map must contain YAML under 'scylla.yaml'
    assert "scylla.yaml" in store
    data = yaml.safe_load(store["scylla.yaml"]) or {}
    assert data.get("endpoint_snitch") == "org.apache.cassandra.locator.GossipingPropertyFileSnitch"
    assert data.get("experimental_features") == ["hinted_handoff"]
    assert cluster.scylla_restart_required is True


def test_remote_scylla_yaml_doesnt_include_explicit_fields(dummy_cluster, events):
    """Explicit fields are left out of the dumped scylla.yaml unless set.

    Note: events fixture is required to handle ScyllaYamlUpdateEvent publishing.
    """
    cluster, store = dummy_cluster

    # Set fields considered explicit by implementation
    with cluster.remote_scylla_yaml() as scylla_yml:
        scylla_yml.auto_snapshot = True

    dumped = yaml.safe_load(store["scylla.yaml"]) or {}
    # Explicit fields must NOT be present in dumped YAML when not explicitly set
    # the default implementation has those, and the k8s shouldn't have them by default, see https://github.com/scylladb/scylla-cluster-tests/pull/4030
    for key in ("partitioner", "commitlog_sync", "commitlog_sync_period_in_ms", "endpoint_snitch"):
        assert key not in dumped
