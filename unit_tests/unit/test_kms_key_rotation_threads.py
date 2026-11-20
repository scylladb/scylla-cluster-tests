import time
from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster import BaseCluster, BaseScyllaCluster
from sdcm.keystore import KeyStore
from unit_tests.lib.fake_cluster import DummyScyllaCluster

# 0.001 minute: the loops rotate every 0.06s, so a test sees several rotations in well under a second
SHORT_INTERVAL_MINUTES = 0.001
LONG_INTERVAL_MINUTES = 60


class KmsParams(dict):
    """SCT params of a test with KMS key rotation: the dict lookups plus the regions the AWS loop reads."""

    region_names = ["eu-west-1"]


@pytest.fixture
def cloud_kms():
    """Patch the KMS clients of all three backends, so the rotation loops never reach a cloud API."""
    with (
        patch("sdcm.cluster.AwsKms") as aws_kms,
        patch("sdcm.cluster.AzureService") as azure_service,
        patch("sdcm.cluster.AzureKmsProvider"),
        patch.object(KeyStore, "get_gcp_credentials", return_value={"project_id": "test-project"}),
        patch.object(KeyStore, "get_gcp_kms_config", return_value={"keyring_location": "global"}),
        patch("sdcm.cluster.GcpKmsProvider"),
        patch("sdcm.cluster.GcpKms") as gcp_kms,
    ):
        yield {
            "aws": aws_kms.return_value.rotate_kms_key,
            "azure": azure_service.return_value.rotate_vault_key,
            "gce": gcp_kms.return_value.rotate_key,
        }


def make_db_cluster(backend: str, interval_minutes: float) -> DummyScyllaCluster:
    """Build a cluster without nodes, so the AWS loop skips the sstables check after each rotation."""
    db_cluster = DummyScyllaCluster([])
    params = KmsParams(
        cluster_backend=backend,
        enable_kms_key_rotation=True,
        kms_key_rotation_interval=interval_minutes,
        append_scylla_yaml={"kms_hosts": {"auto": {"master_key": "alias/testid-auto"}}},
        azure_region_name=["eastus"],
    )
    # DummyScyllaCluster skips BaseScyllaCluster.__init__, which creates the stop event and the threads
    # list: run it, without the BaseCluster.__init__ that provisions nodes
    with patch.object(BaseCluster, "__init__", return_value=None):
        BaseScyllaCluster.__init__(db_cluster, params=params)
    return db_cluster


def start_rotation_threads(db_cluster: DummyScyllaCluster) -> None:
    """Start the threads the way the tester does: each method returns early on the other backends."""
    db_cluster.start_kms_key_rotation_thread()
    db_cluster.start_azure_kms_key_rotation_thread()
    db_cluster.start_gcp_key_rotation_thread()


BACKENDS = [pytest.param("aws", id="aws"), pytest.param("azure", id="azure"), pytest.param("gce", id="gce")]


@pytest.mark.parametrize("backend", BACKENDS)
def test_kms_key_rotation_thread_rotates_until_stopped(cloud_kms, backend):
    """The loop keeps rotating the key while the test runs, and no rotation happens after the stop."""
    rotate_key: MagicMock = cloud_kms[backend]
    db_cluster = make_db_cluster(backend, SHORT_INTERVAL_MINUTES)

    start_rotation_threads(db_cluster)
    deadline = time.monotonic() + 5
    while rotate_key.call_count < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    db_cluster.stop_kms_key_rotation_threads(timeout=5)
    rotations_at_stop = rotate_key.call_count
    time.sleep(SHORT_INTERVAL_MINUTES * 60 * 3)

    assert rotations_at_stop >= 2
    assert len(db_cluster.kms_key_rotation_threads) == 1
    assert not db_cluster.kms_key_rotation_threads[0].is_alive()
    assert rotate_key.call_count == rotations_at_stop


@pytest.mark.parametrize("backend", BACKENDS)
def test_kms_key_rotation_thread_stop_does_not_wait_for_the_interval(cloud_kms, backend):
    """Teardown must not wait up to an interval (60 minutes by default) for the loop to wake up."""
    rotate_key: MagicMock = cloud_kms[backend]
    db_cluster = make_db_cluster(backend, LONG_INTERVAL_MINUTES)

    start_rotation_threads(db_cluster)
    started_at = time.monotonic()
    db_cluster.stop_kms_key_rotation_threads(timeout=5)

    assert time.monotonic() - started_at < 5
    assert not db_cluster.kms_key_rotation_threads[0].is_alive()
    rotate_key.assert_not_called()


def test_aws_kms_key_rotation_stop_during_rotation_skips_the_nodes_check(cloud_kms):
    """A stop that arrives during the rotation call must not start an SSH check of the nodes in teardown."""
    db_cluster = make_db_cluster("aws", SHORT_INTERVAL_MINUTES)
    db_node = MagicMock(_is_zero_token_node=False)
    db_node.db_up.return_value = False
    db_cluster.nodes = [db_node]
    cloud_kms["aws"].side_effect = lambda **_: db_cluster.kms_key_rotation_stop_event.set()

    db_cluster.start_kms_key_rotation_thread()
    db_cluster.kms_key_rotation_threads[0].join(timeout=5)

    assert not db_cluster.kms_key_rotation_threads[0].is_alive()
    cloud_kms["aws"].assert_called_once()
    db_node.db_up.assert_not_called()
