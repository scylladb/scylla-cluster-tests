"""Tests for pre-start checks: KVM/docker/AWS-creds gates and the host-memory arithmetic."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sdcm.utils.minicloud import MinicloudConfig, MinicloudError, MinicloudManager
from unit_tests.unit.minicloud.conftest import _meminfo_path_patch


def _kvm_path_patch(kvm_exists):
    """Patch sdcm.utils.minicloud.manager.Path so /dev/kvm reports the given presence."""
    mock_kvm = MagicMock()
    mock_kvm.exists.return_value = kvm_exists

    def path_side_effect(arg):
        if str(arg) == "/dev/kvm":
            return mock_kvm
        return Path(arg)

    return patch("sdcm.utils.minicloud.manager.Path", side_effect=path_side_effect)


def test_preflight_check_fails_no_kvm(tmp_path):
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))

    with _kvm_path_patch(kvm_exists=False):
        with pytest.raises(MinicloudError, match="/dev/kvm"):
            manager.preflight_check()


def test_preflight_check_fails_docker_not_found(tmp_path):
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))

    with _kvm_path_patch(kvm_exists=True):
        with patch("sdcm.utils.minicloud.manager.shutil.which", return_value=None):
            with pytest.raises(MinicloudError, match="docker is not available"):
                manager.preflight_check()


def test_preflight_check_fails_bad_aws_credentials(tmp_path):
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))

    bad_result = MagicMock()
    bad_result.returncode = 1

    with _kvm_path_patch(kvm_exists=True):
        with patch("sdcm.utils.minicloud.manager.shutil.which", return_value="/usr/bin/docker"):
            with patch("sdcm.utils.minicloud.preflight.subprocess.run", return_value=bad_result):
                with pytest.raises(MinicloudError, match="AWS credentials"):
                    manager.preflight_check()


def test_preflight_check_fails_aws_cli_not_found(tmp_path):
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))

    with _kvm_path_patch(kvm_exists=True):
        with patch("sdcm.utils.minicloud.manager.shutil.which", return_value="/usr/bin/docker"):
            with patch("sdcm.utils.minicloud.preflight.subprocess.run", side_effect=FileNotFoundError("aws not found")):
                with pytest.raises(MinicloudError, match="AWS CLI not found"):
                    manager.preflight_check()


def test_preflight_check_skip_aws_creds(tmp_path):
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))

    with _kvm_path_patch(kvm_exists=True):
        with patch("sdcm.utils.minicloud.manager.shutil.which", return_value="/usr/bin/docker"):
            with patch("sdcm.utils.minicloud.preflight.subprocess.run") as mock_run:
                manager.preflight_check(skip_aws_creds=True)
                mock_run.assert_not_called()


@pytest.mark.parametrize(
    ("value", "expected"),
    [("4GiB", 4.0), ("2.5GiB", 2.5), ("4096MiB", 4.0), ("4G", 4.0), ("1TiB", 1024.0)],
)
def test_parse_memory_gib(value, expected):
    assert MinicloudManager._parse_memory_gib(value) == pytest.approx(expected)


@pytest.mark.parametrize("value", ["lots", "1.2.3GiB", "..GiB", ".5GiB"])
def test_parse_memory_gib_rejects_garbage(value):
    """Malformed numbers must fail as MinicloudError, not as a bare float() ValueError."""
    with pytest.raises(MinicloudError, match="cannot parse"):
        MinicloudManager._parse_memory_gib(value)


@pytest.mark.parametrize(
    ("value", "expected"),
    [(3, 3), ("3 3", 6), ([3, 3], 6), (None, 0), ("", 0), ([], 0)],
)
def test_sum_node_counts(value, expected):
    assert MinicloudManager._sum_node_counts(value) == expected


def test_check_host_memory_fails_when_guests_exceed_available(tmp_path):
    # 6 db + 1 loader + 1 monitor = 8 guests x 4GiB + 2GiB headroom = 34GiB needed, 16GiB available
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    params = {"n_db_nodes": "3 3", "n_loaders": 1, "n_monitor_nodes": 1}
    with _meminfo_path_patch(16 * 1024 * 1024):
        with pytest.raises(MinicloudError, match="8 guest.*34.0GiB needed.*16.0GiB"):
            manager._check_host_memory(params)


def test_check_host_memory_kill_switch(tmp_path):
    """minicloud_skip_memory_check disables the gate — dev machines know their own limits."""
    config = MinicloudConfig(state_dir=str(tmp_path), lightweight=True, skip_memory_check=True)
    manager = MinicloudManager(config=config)
    params = {"n_db_nodes": "3 3", "n_loaders": 1, "n_monitor_nodes": 1}
    with _meminfo_path_patch(16 * 1024 * 1024):
        manager._check_host_memory(params)  # must not raise


def test_check_host_memory_passes_when_it_fits(tmp_path):
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    params = {"n_db_nodes": 1, "n_loaders": 0, "n_monitor_nodes": 0}
    with _meminfo_path_patch(16 * 1024 * 1024):
        manager._check_host_memory(params)


def test_check_host_memory_counts_every_guest_pool(tmp_path):
    """Oracle, zero-token and vector-store nodes are guests too.

    Leaving them out of the arithmetic let a test pass the gate and then OOM-kill the
    container mid-run (exit 137), taking every VM with it.
    """
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    # 1 + 1 + 1 + 1 + 1 + 1 = 6 guests x 4GiB + 2GiB headroom = 26GiB needed, 16GiB available
    params = {
        "n_db_nodes": 1,
        "n_loaders": 1,
        "n_monitor_nodes": 1,
        "n_test_oracle_db_nodes": 1,
        "n_db_zero_token_nodes": 1,
        "n_vector_store_nodes": 1,
    }
    with _meminfo_path_patch(16 * 1024 * 1024):
        with pytest.raises(MinicloudError, match="6 guest.*26.0GiB needed"):
            manager._check_host_memory(params)


def test_check_host_memory_counts_the_grown_cluster_not_the_initial_one(tmp_path):
    """A scale test starts at n_db_nodes and grows to cluster_target_size.

    Sizing the initial cluster only would pass the gate and then let the run die at the exact
    moment it adds the node nobody budgeted for.
    """
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    # starts at 3 db, grows to 6; + 1 loader + 1 monitor = 8 guests x 4GiB + 2GiB = 34GiB needed
    params = {"n_db_nodes": 3, "cluster_target_size": 6, "n_loaders": 1, "n_monitor_nodes": 1}
    with _meminfo_path_patch(24 * 1024 * 1024):  # enough for the initial 5, not for the peak 8
        with pytest.raises(MinicloudError, match="8 guest.*34.0GiB needed"):
            manager._check_host_memory(params)


def test_check_host_memory_ignores_a_target_below_the_initial_size(tmp_path):
    """cluster_target_size never shrinks a cluster, so it must not shrink the budget either."""
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    params = {"n_db_nodes": 6, "cluster_target_size": 3, "n_loaders": 0, "n_monitor_nodes": 0}
    with _meminfo_path_patch(16 * 1024 * 1024):
        with pytest.raises(MinicloudError, match="6 guest.*26.0GiB needed"):
            manager._check_host_memory(params)


def test_check_host_memory_measures_against_container_cap(tmp_path):
    """A docker --memory cap, not host free memory, is what the cgroup OOM killer enforces.

    The host here has plenty free (64GiB) but the cap is 8GiB, so 3 guests x 4GiB cannot fit
    and the run has to fail before starting rather than be killed mid-test.
    """
    config = MinicloudConfig(state_dir=str(tmp_path), lightweight=True, container_memory="8GiB")
    manager = MinicloudManager(config=config)
    params = {"n_db_nodes": 3, "n_loaders": 0, "n_monitor_nodes": 0}
    with _meminfo_path_patch(64 * 1024 * 1024):
        with pytest.raises(MinicloudError, match="3 guest.*12.0GiB needed.*8.0GiB.*container_memory cap"):
            manager._check_host_memory(params)


def test_check_host_memory_container_cap_that_fits_passes(tmp_path):
    """No host headroom is subtracted from the cap — dockerd and SCT live outside the cgroup."""
    config = MinicloudConfig(state_dir=str(tmp_path), lightweight=True, container_memory="12GiB")
    manager = MinicloudManager(config=config)
    params = {"n_db_nodes": 3, "n_loaders": 0, "n_monitor_nodes": 0}
    with _meminfo_path_patch(1 * 1024 * 1024):  # host looks starved; the cap is what counts
        manager._check_host_memory(params)  # must not raise


def test_check_host_memory_skipped_outside_lightweight_mode(tmp_path):
    # non-lightweight sizing follows the requested instance types - no fixed per-guest figure
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=False))
    with patch("sdcm.utils.minicloud.preflight.Path") as mock_path_cls:
        manager._check_host_memory({"n_db_nodes": 100})
        mock_path_cls.assert_not_called()


def test_preflight_check_runs_memory_check_when_params_given(tmp_path):
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    params = {"n_db_nodes": "3 3", "n_loaders": 1, "n_monitor_nodes": 1}

    # /dev/kvm gate lives in manager, /proc/meminfo read lives in preflight — patch both
    with _kvm_path_patch(kvm_exists=True):
        with _meminfo_path_patch(8 * 1024 * 1024):  # 8GiB
            with patch("sdcm.utils.minicloud.manager.shutil.which", return_value="/usr/bin/docker"):
                with pytest.raises(MinicloudError, match="not enough memory"):
                    manager.preflight_check(skip_aws_creds=True, params=params)
