import itertools
from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster import BaseCluster, BaseScyllaCluster
from sdcm.keystore import KeyStore
from unit_tests.lib.fake_cluster import DummyScyllaCluster


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


@pytest.fixture(params=["aws", "azure", "gce"])
def backend(request) -> str:
    """Run the test once per backend that has a KMS key rotation loop."""
    return request.param


def make_db_cluster(backend: str, interval_minutes: float = 0.001) -> DummyScyllaCluster:
    """Build a cluster without nodes, so the AWS loop skips the sstables check after each rotation.

    The default interval, 0.001 minute, makes the loops rotate every 0.06s.
    """
    db_cluster = DummyScyllaCluster([])
    params = KmsParams(
        cluster_backend=backend,
        enable_kms_key_rotation=True,
        kms_key_rotation_interval=interval_minutes,
        append_scylla_yaml={"kms_hosts": {"auto": {"master_key": "alias/testid-auto"}}},
        azure_region_name=["eastus"],
    )
    # Unlike DummyDbCluster, which sets the stop event and the threads list by hand, run the real
    # BaseScyllaCluster.__init__ that creates them, without the BaseCluster.__init__ that provisions nodes
    with patch.object(BaseCluster, "__init__", return_value=None):
        BaseScyllaCluster.__init__(db_cluster, params=params)
    return db_cluster


def start_rotation_threads(db_cluster: DummyScyllaCluster) -> None:
    """Start the threads the way the tester does: each method returns early on the other backends."""
    db_cluster.start_kms_key_rotation_thread()
    db_cluster.start_azure_kms_key_rotation_thread()
    db_cluster.start_gcp_key_rotation_thread()


def stop_on_call(db_cluster: DummyScyllaCluster, call_number: int):
    """Side effect of a mock that asks the rotation loops to stop on its given call, as teardown does."""
    calls = itertools.count(1)

    def side_effect(*_, **__):
        if next(calls) == call_number:
            db_cluster.kms_key_rotation_stop_event.set()

    return side_effect


def test_kms_key_rotation_thread_rotates_every_interval_until_stopped(cloud_kms, backend):
    """The loop rotates the key again after each interval, and no rotation starts after the stop."""
    rotate_key: MagicMock = cloud_kms[backend]
    db_cluster = make_db_cluster(backend)
    rotate_key.side_effect = stop_on_call(db_cluster, call_number=2)

    start_rotation_threads(db_cluster)
    db_cluster.kms_key_rotation_threads[0].join(timeout=5)

    assert len(db_cluster.kms_key_rotation_threads) == 1
    assert not db_cluster.kms_key_rotation_threads[0].is_alive()
    assert rotate_key.call_count == 2


def test_kms_key_rotation_thread_stop_does_not_wait_for_the_interval(cloud_kms, backend):
    """Teardown must not wait up to an interval (60 minutes by default) for the loop to wake up."""
    rotate_key: MagicMock = cloud_kms[backend]
    db_cluster = make_db_cluster(backend, interval_minutes=60)

    start_rotation_threads(db_cluster)
    db_cluster.stop_kms_key_rotation_threads(timeout=5)

    assert not db_cluster.kms_key_rotation_threads[0].is_alive()
    rotate_key.assert_not_called()


def test_aws_kms_key_rotation_stop_during_rotation_skips_the_nodes_check(cloud_kms):
    """A stop that arrives during the rotation call must not start an SSH check of the nodes in teardown."""
    db_cluster = make_db_cluster("aws")
    db_node = MagicMock(_is_zero_token_node=False)
    db_node.db_up.return_value = False
    db_cluster.nodes = [db_node]
    cloud_kms["aws"].side_effect = stop_on_call(db_cluster, call_number=1)

    db_cluster.start_kms_key_rotation_thread()
    db_cluster.kms_key_rotation_threads[0].join(timeout=5)

    assert not db_cluster.kms_key_rotation_threads[0].is_alive()
    cloud_kms["aws"].assert_called_once()
    db_node.db_up.assert_not_called()


def test_aws_kms_key_rotation_check_gets_the_teardown_stop_event(cloud_kms, monkeypatch):
    """Teardown must not wait for a check of every sstable, so the check gets the event that stops it."""
    db_cluster = make_db_cluster("aws")
    db_node = MagicMock(_is_zero_token_node=False)
    db_cluster.nodes = [db_node]
    db_cluster.test_config = MagicMock()
    db_cluster.test_config.tester_obj().nemesis_allocator.run_nemesis().__enter__.return_value = db_node
    monkeypatch.setattr(db_cluster, "get_ks_cf_with_most_sstables", lambda db_node: "keyspace1.standard1")

    with patch("sdcm.cluster.SstableUtils") as sstable_utils_class:
        check = sstable_utils_class.return_value.check_that_sstables_are_encrypted
        check.side_effect = stop_on_call(db_cluster, call_number=1)
        db_cluster.start_kms_key_rotation_thread()
        db_cluster.kms_key_rotation_threads[0].join(timeout=5)
    # Ends the loop even when the check was never called, so a failure leaves no thread behind
    db_cluster.kms_key_rotation_stop_event.set()

    sstable_utils_class.assert_called_once_with(db_node=db_node, ks_cf="keyspace1.standard1")
    check.assert_called_once_with(stop_event=db_cluster.kms_key_rotation_stop_event)
