"""Tests for pre-start checks: KVM/docker/AWS-creds gates, host-memory and guest-memory arithmetic."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sdcm.utils.minicloud.config import MinicloudConfig, MinicloudError
from sdcm.utils.minicloud.manager import MinicloudManager
from sdcm.utils.minicloud.preflight import (
    check_scylla_memory_budget,
    container_mem_in_use_gib,
    db_guest_count,
    host_available_memory_gib,
    nemesis_growth_guests,
    scylla_reserve_memory,
)
from unit_tests.unit.minicloud.conftest import _meminfo_path_patch

VM_STAT_OUTPUT = """Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                                    65536.
Pages active:                                 681336.
Pages inactive:                               196608.
Pages speculative:                             31319.
"""


def kvm_path_patch(kvm_exists):
    """Patch sdcm.utils.minicloud.manager.Path so /dev/kvm reports the given presence."""
    mock_kvm = MagicMock()
    mock_kvm.exists.return_value = kvm_exists

    def path_side_effect(arg):
        if str(arg) == "/dev/kvm":
            return mock_kvm
        return Path(arg)

    return patch("sdcm.utils.minicloud.manager.Path", side_effect=path_side_effect)


def no_meminfo_path_patch():
    """Patch sdcm.utils.minicloud.preflight.Path so /proc/meminfo does not exist, as on macOS."""
    mock_meminfo = MagicMock()
    mock_meminfo.exists.return_value = False
    return patch(
        "sdcm.utils.minicloud.preflight.Path",
        side_effect=lambda arg: mock_meminfo if str(arg) == "/proc/meminfo" else Path(arg),
    )


def reserve_params(reserve="3G", guest="8GiB", vcpus=2, **extra):
    """Minicloud params for the Scylla memory budget, with extra keys merged in."""
    return {
        "minicloud_scylla_reserve_memory": reserve,
        "minicloud_lightweight_memory": guest,
        "minicloud_lightweight_vcpus": vcpus,
        **extra,
    }


def test_preflight_check_fails_no_kvm(tmp_path):
    """preflight_check fails with MinicloudError when /dev/kvm is absent."""
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))

    with kvm_path_patch(kvm_exists=False), pytest.raises(MinicloudError, match="/dev/kvm"):
        manager.preflight_check()


def test_preflight_check_fails_docker_not_found(tmp_path):
    """preflight_check fails when the docker binary is not on PATH."""
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))

    with (
        kvm_path_patch(kvm_exists=True),
        patch("sdcm.utils.minicloud.manager.shutil.which", return_value=None),
        pytest.raises(MinicloudError, match="docker is not available"),
    ):
        manager.preflight_check()


def test_preflight_check_fails_bad_aws_credentials(tmp_path):
    """preflight_check fails when the AWS credentials check exits non-zero."""
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))

    bad_result = MagicMock()
    bad_result.returncode = 1

    with (
        kvm_path_patch(kvm_exists=True),
        patch("sdcm.utils.minicloud.manager.shutil.which", return_value="/usr/bin/docker"),
        patch("sdcm.utils.minicloud.preflight.subprocess.run", return_value=bad_result),
        pytest.raises(MinicloudError, match="AWS credentials"),
    ):
        manager.preflight_check()


def test_preflight_check_fails_aws_cli_not_found(tmp_path):
    """preflight_check fails with a clear message when the aws CLI is missing."""
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))

    with (
        kvm_path_patch(kvm_exists=True),
        patch("sdcm.utils.minicloud.manager.shutil.which", return_value="/usr/bin/docker"),
        patch("sdcm.utils.minicloud.preflight.subprocess.run", side_effect=FileNotFoundError("aws not found")),
        pytest.raises(MinicloudError, match="AWS CLI not found"),
    ):
        manager.preflight_check()


def test_preflight_check_skip_aws_creds(tmp_path):
    """skip_aws_creds=True skips the AWS credentials probe."""
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))

    with (
        kvm_path_patch(kvm_exists=True),
        patch("sdcm.utils.minicloud.manager.shutil.which", return_value="/usr/bin/docker"),
        patch("sdcm.utils.minicloud.preflight.subprocess.run") as mock_run,
    ):
        manager.preflight_check(skip_aws_creds=True)
        mock_run.assert_not_called()


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("4GiB", 4.0, id="gib"),
        pytest.param("2.5GiB", 2.5, id="fractional-gib"),
        pytest.param("4096MiB", 4.0, id="mib"),
        pytest.param("4G", 4.0, id="short-g"),
        pytest.param("1TiB", 1024.0, id="tib"),
    ],
)
def test_parse_memory_gib(value, expected):
    """_parse_memory_gib converts GiB, MiB, TiB and G suffixed strings to GiB."""
    assert MinicloudManager._parse_memory_gib(value) == pytest.approx(expected)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("lots", id="not-a-number"),
        pytest.param("1.2.3GiB", id="two-dots"),
        pytest.param("..GiB", id="double-dot"),
        pytest.param(".5GiB", id="no-leading-digit"),
    ],
)
def test_parse_memory_gib_rejects_garbage(value):
    """Malformed numbers must fail as MinicloudError, not as a bare float() ValueError."""
    with pytest.raises(MinicloudError, match="cannot parse"):
        MinicloudManager._parse_memory_gib(value)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(3, 3, id="single-dc-int"),
        pytest.param([3, 3], 6, id="list-of-dcs"),
        pytest.param(None, 0, id="none"),
        pytest.param([], 0, id="empty-list"),
    ],
)
def test_sum_node_counts(value, expected):
    """_sum_node_counts totals an int or a per-DC list, and counts None or an empty list as zero."""
    assert MinicloudManager._sum_node_counts(value) == expected


def test_check_host_memory_fails_when_guests_exceed_available(tmp_path):
    """_check_host_memory raises when the guests need more memory than the host has."""
    # 6 db + 1 loader + 1 monitor = 8 guests x 4GiB + (2GiB + 8 x 1GiB) headroom = 42GiB needed, 16GiB available
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    params = {"n_db_nodes": [3, 3], "n_loaders": 1, "n_monitor_nodes": 1}
    with _meminfo_path_patch(16 * 1024 * 1024), pytest.raises(MinicloudError, match="8 guest.*42.0GiB needed.*16.0GiB"):
        manager._check_host_memory(params)


@pytest.mark.parametrize(
    ("db_memory", "fits"),
    [
        pytest.param("5GiB", False, id="5gib-over-budget"),
        pytest.param("4GiB", True, id="4gib-fits"),
    ],
)
def test_check_host_memory_reserves_overhead_per_guest(tmp_path, db_memory, fits):
    """The 6-node rolling upgrade on minipc5: 42.1GiB budget, 6 db + 2GiB loader + 2GiB monitor.

    With a flat 2GiB headroom the 5GiB run passed the gate (36GiB needed) and the host OOM
    killer then killed a guest; the real overhead was about 1GiB per guest.
    """
    config = MinicloudConfig(
        state_dir=str(tmp_path),
        lightweight=True,
        lightweight_memory=db_memory,
        lightweight_memory_loader="2GiB",
        lightweight_memory_monitor="2GiB",
    )
    manager = MinicloudManager(config=config)
    params = {"n_db_nodes": 6, "n_loaders": 1, "n_monitor_nodes": 1}
    with _meminfo_path_patch(int(42.1 * 1024 * 1024)):
        if fits:  # 6 x 4 + 2 + 2 + 10 = 38GiB
            manager._check_host_memory(params)
        else:  # 6 x 5 + 2 + 2 + 10 = 44GiB
            with pytest.raises(MinicloudError, match=r"8 guest.*44\.0GiB needed"):
                manager._check_host_memory(params)


def test_check_host_memory_kill_switch(tmp_path):
    """minicloud_skip_memory_check disables the gate — dev machines know their own limits."""
    config = MinicloudConfig(state_dir=str(tmp_path), lightweight=True, skip_memory_check=True)
    manager = MinicloudManager(config=config)
    params = {"n_db_nodes": [3, 3], "n_loaders": 1, "n_monitor_nodes": 1}
    with _meminfo_path_patch(16 * 1024 * 1024):
        manager._check_host_memory(params)  # must not raise


def test_check_host_memory_passes_when_it_fits(tmp_path):
    """_check_host_memory passes when one db node fits in the available memory."""
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    params = {"n_db_nodes": 1, "n_loaders": 0, "n_monitor_nodes": 0}
    with _meminfo_path_patch(16 * 1024 * 1024):
        manager._check_host_memory(params)


def test_check_host_memory_sizes_each_role(tmp_path):
    """Loaders and monitors count at their own size, so the gate stops charging them a db node's RAM."""
    config = MinicloudConfig(
        state_dir=str(tmp_path),
        lightweight=True,
        lightweight_memory_loader="2GiB",
        lightweight_memory_monitor="2GiB",
    )
    manager = MinicloudManager(config=config)
    params = {"n_db_nodes": 3, "n_loaders": [1, 1], "n_monitor_nodes": 1}
    with (
        _meminfo_path_patch(19 * 1024 * 1024),
        pytest.raises(
            MinicloudError,
            match=r"6 guest.*3 db x 4\.0GiB \+ 2 loader x 2\.0GiB \+ 1 monitor x 2\.0GiB.*26\.0GiB needed",
        ),
    ):
        manager._check_host_memory(params)


def test_check_host_memory_counts_every_guest_pool(tmp_path):
    """Oracle, zero-token and vector-store nodes are guests too.

    Leaving them out of the arithmetic let a test pass the gate and then OOM-kill the
    container mid-run (exit 137), taking every VM with it. db_type is the mixed one that
    actually provisions the oracle cluster, so all six pools are real guests here.
    """
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    # 1 + 1 + 1 + 1 + 1 + 1 = 6 guests x 4GiB + 8GiB headroom = 32GiB needed, 16GiB available
    params = {
        "db_type": "mixed_scylla",
        "n_db_nodes": 1,
        "n_loaders": 1,
        "n_monitor_nodes": 1,
        "n_test_oracle_db_nodes": 1,
        "n_db_zero_token_nodes": 1,
        "n_vector_store_nodes": 1,
    }
    with _meminfo_path_patch(16 * 1024 * 1024), pytest.raises(MinicloudError, match="6 guest.*32.0GiB needed"):
        manager._check_host_memory(params)


@pytest.mark.parametrize(
    "db_type",
    [
        pytest.param("mixed_scylla", id="scylla"),
        pytest.param("mixed_cassandra", id="cassandra"),
    ],
)
def test_check_host_memory_counts_the_oracle_pool_for_both_mixed_db_types(tmp_path, db_type):
    """Both mixed types provision an oracle pool, by different routes.

    mixed_scylla goes through _create_oracle_cluster(); mixed_cassandra builds a
    CassandraAWSCluster directly with n_nodes=n_test_oracle_db_nodes and node_type="oracle-db",
    so searching tester.py for the helper alone finds only one of the two and the pool of the
    other goes unbudgeted.
    """
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    # 3 db + 1 loader + 1 monitor + 1 oracle = 6 guests x 4GiB + 8GiB = 32GiB, over the 30GiB
    # the same shape without an oracle node (5 x 4GiB + 7GiB = 27GiB) fits into.
    params = {
        "db_type": db_type,
        "n_db_nodes": 3,
        "n_loaders": 1,
        "n_monitor_nodes": 1,
        "n_test_oracle_db_nodes": 1,
    }
    with _meminfo_path_patch(30 * 1024 * 1024), pytest.raises(MinicloudError, match="6 guest"):
        manager._check_host_memory(params)


def test_check_host_memory_ignores_the_oracle_pool_without_a_mixed_db_type(tmp_path):
    """n_test_oracle_db_nodes defaults to 1, but the oracle cluster only exists for mixed db_type.

    tester.py creates it solely when db_type is mixed_scylla/mixed_cassandra, so counting it
    for an ordinary longevity run invents a guest that is never provisioned and rejects a test
    that fits - which is what blocked longevity-100gb-4h on a 123GiB lab machine.
    """
    manager = MinicloudManager(
        config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True, lightweight_memory="10GiB")
    )
    # 6 db + 2 loaders + 1 monitor = 9 guests x 10GiB + 11GiB headroom = 101GiB, under the 105GiB budget.
    # Counting the phantom oracle node makes it 112GiB and the run never starts.
    params = {"n_db_nodes": 6, "n_loaders": 2, "n_monitor_nodes": 1, "n_test_oracle_db_nodes": 1}
    with _meminfo_path_patch(105 * 1024 * 1024):
        manager._check_host_memory(params)  # must not raise


def test_check_host_memory_counts_the_grown_cluster_not_the_initial_one(tmp_path):
    """A scale test starts at n_db_nodes and grows to cluster_target_size.

    Sizing the initial cluster only would pass the gate and then let the run die at the exact
    moment it adds the node nobody budgeted for.
    """
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    # starts at 3 db, grows to 6; + 1 loader + 1 monitor = 8 guests x 4GiB + 10GiB = 42GiB needed
    params = {"n_db_nodes": 3, "cluster_target_size": 6, "n_loaders": 1, "n_monitor_nodes": 1}
    with (
        _meminfo_path_patch(30 * 1024 * 1024),  # enough for the initial 5 (27GiB), not for the peak 8
        pytest.raises(MinicloudError, match="8 guest.*42.0GiB needed"),
    ):
        manager._check_host_memory(params)


def test_check_host_memory_ignores_a_target_below_the_initial_size(tmp_path):
    """cluster_target_size never shrinks a cluster, so it must not shrink the budget either."""
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    params = {"n_db_nodes": 6, "cluster_target_size": 3, "n_loaders": 0, "n_monitor_nodes": 0}
    with _meminfo_path_patch(16 * 1024 * 1024), pytest.raises(MinicloudError, match="6 guest.*32.0GiB needed"):
        manager._check_host_memory(params)


def test_nemesis_growth_skips_a_selection_without_disruptive_nemeses():
    """nemesis_growth_guests counts no added guests for a non-disruptive or no-op nemesis selection."""
    params = {
        "nemesis_class_name": ["SisyphusMonkey"],
        "nemesis_selector": ["not disruptive"],
        "nemesis_add_node_cnt": 3,
    }
    assert nemesis_growth_guests(params) == 0
    assert nemesis_growth_guests({"nemesis_class_name": ["NoOpMonkey"], "nemesis_add_node_cnt": 3}) == 0


def test_check_host_memory_counts_nemesis_growth(tmp_path):
    """_check_host_memory budgets the guests a disruptive nemesis adds on top of the initial cluster."""
    # 6 x 4 + 2 + 2 + (2 + 8) = 38GiB, where the 3 db guests alone fit
    config = MinicloudConfig(
        state_dir=str(tmp_path), lightweight=True, lightweight_memory_loader="2GiB", lightweight_memory_monitor="2GiB"
    )
    params = {
        "n_db_nodes": 3,
        "n_loaders": 1,
        "n_monitor_nodes": 1,
        "nemesis_class_name": ["SisyphusMonkey"],
        "nemesis_selector": ["disruptive"],
        "nemesis_add_node_cnt": 3,
    }
    with _meminfo_path_patch(30 * 1024 * 1024), pytest.raises(MinicloudError, match=r"8 guest.*38\.0GiB needed"):
        MinicloudManager(config=config)._check_host_memory(params)


def test_check_host_memory_measures_against_container_cap(tmp_path):
    """A docker --memory cap, not host free memory, is what the cgroup OOM killer enforces.

    The host here has plenty free (64GiB) but the cap is 8GiB, so 3 guests x 4GiB cannot fit
    and the run has to fail before starting rather than be killed mid-test.
    """
    config = MinicloudConfig(state_dir=str(tmp_path), lightweight=True, container_memory="8GiB")
    manager = MinicloudManager(config=config)
    params = {"n_db_nodes": 3, "n_loaders": 0, "n_monitor_nodes": 0}
    with (
        _meminfo_path_patch(64 * 1024 * 1024),
        pytest.raises(MinicloudError, match="3 guest.*QEMU overhead.*15.0GiB needed.*8.0GiB.*container_memory cap"),
    ):
        manager._check_host_memory(params)


def test_check_host_memory_container_cap_that_fits_passes(tmp_path):
    """Only the per-guest QEMU cost counts against the cap; dockerd and SCT live outside the cgroup.

    3 guests x 4GiB + 3 x 1GiB QEMU overhead = 15GiB, exactly the cap.
    """
    config = MinicloudConfig(state_dir=str(tmp_path), lightweight=True, container_memory="15GiB")
    manager = MinicloudManager(config=config)
    params = {"n_db_nodes": 3, "n_loaders": 0, "n_monitor_nodes": 0}
    with _meminfo_path_patch(1 * 1024 * 1024):  # host looks starved; the cap is what counts
        manager._check_host_memory(params)  # must not raise


def test_check_host_memory_adds_mem_in_use_by_running_guests(tmp_path):
    """In CI the guests are already running when run-test re-runs the gate, so their memory in use is added back."""
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    # 20 db + 2 loaders + 1 monitor = 23 guests x 4GiB + 25GiB headroom = 117GiB needed
    params = {"n_db_nodes": 20, "n_loaders": 2, "n_monitor_nodes": 1}
    with (
        _meminfo_path_patch(int(93.2 * 1024 * 1024)),
        patch("sdcm.utils.minicloud.preflight.container_mem_in_use_gib", return_value=24.0),
    ):
        manager._check_host_memory(params)


def test_check_host_memory_skipped_outside_lightweight_mode(tmp_path):
    """_check_host_memory does not read the host memory outside lightweight mode."""
    # non-lightweight sizing follows the requested instance types - no fixed per-guest figure
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=False))
    with patch("sdcm.utils.minicloud.preflight.Path") as mock_path_cls:
        manager._check_host_memory({"n_db_nodes": 100})
        mock_path_cls.assert_not_called()


def test_container_mem_in_use_gib_reads_docker_stats():
    """container_mem_in_use_gib returns the used part of the docker stats memory column in GiB."""
    stats = MagicMock(returncode=0, stdout=b"24.3GiB / 125.6GiB\n")
    with patch("sdcm.utils.minicloud.preflight.subprocess.run", return_value=stats):
        assert container_mem_in_use_gib("minicloud") == pytest.approx(24.3)


def test_container_mem_in_use_gib_is_zero_without_a_container():
    """start-minicloud runs the gate before the container exists, which must not fail the check."""
    missing = MagicMock(returncode=1, stdout=b"")
    with patch("sdcm.utils.minicloud.preflight.subprocess.run", return_value=missing):
        assert container_mem_in_use_gib("minicloud") == 0.0


def test_preflight_check_runs_memory_check_when_params_given(tmp_path):
    """preflight_check runs the host memory gate when params are given and fails when memory is short."""
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    params = {"n_db_nodes": [3, 3], "n_loaders": 1, "n_monitor_nodes": 1}

    # /dev/kvm gate lives in manager, /proc/meminfo read lives in preflight — patch both
    with (
        kvm_path_patch(kvm_exists=True),
        _meminfo_path_patch(8 * 1024 * 1024),  # 8GiB
        patch("sdcm.utils.minicloud.manager.shutil.which", return_value="/usr/bin/docker"),
        pytest.raises(MinicloudError, match="not enough memory"),
    ):
        manager.preflight_check(skip_aws_creds=True, params=params)


def test_scylla_reserve_memory_off_by_default():
    """scylla_reserve_memory returns None when the reserve option is empty."""
    # it is bought out of Scylla's own budget, so a test has to ask for it
    assert scylla_reserve_memory(reserve_params(reserve="")) is None


def test_scylla_reserve_memory_passes_the_request_through_when_it_fits():
    """scylla_reserve_memory returns the requested reserve when the guest has room for it."""
    assert scylla_reserve_memory(reserve_params(guest="8GiB", vcpus=2)) == "3072M"


def test_scylla_reserve_memory_capped_to_leave_scylla_running():
    """scylla_reserve_memory caps the reserve so a small guest keeps Scylla's floor."""
    # a 4GiB guest cannot spare 3GiB: Scylla still needs its own floor
    assert scylla_reserve_memory(reserve_params(guest="4GiB", vcpus=1)) == "2048M"


def test_scylla_reserve_memory_floor_follows_the_shard_count():
    """scylla_reserve_memory returns None when the guest has no memory to spare above the shard floor."""
    # 3 shards need ~3GiB, so the same 4GiB guest has nothing worth reserving left over -
    # a fixed floor would hand Scylla less memory than it can boot on
    assert scylla_reserve_memory(reserve_params(guest="4GiB", vcpus=3)) is None


def test_scylla_reserve_memory_survives_a_bad_value():
    """scylla_reserve_memory returns None for an unparsable reserve instead of raising."""
    # preflight is where a typo fails the run; a node coming up mid-run must not die on top of it
    assert scylla_reserve_memory(reserve_params(reserve="three gigs")) is None


@pytest.mark.parametrize(
    ("params", "expected_option"),
    [
        pytest.param(reserve_params(reserve="three gigs"), "minicloud_scylla_reserve_memory", id="reserve"),
        pytest.param(reserve_params(guest="plenty"), "minicloud_lightweight_memory", id="guest"),
    ],
)
def test_a_bad_memory_value_names_the_option_that_holds_it(params, expected_option):
    """One parser serves four options, so the message has to say which one failed."""
    with pytest.raises(MinicloudError, match=f"cannot parse {expected_option} value"):
        check_scylla_memory_budget(params)


@pytest.mark.parametrize(
    ("guest", "vcpus", "fits"),
    [
        pytest.param("4GiB", 1, True, id="4gib-1vcpu-fits"),
        pytest.param("3GiB", 1, True, id="3gib-1vcpu-fits"),
        pytest.param("2.5GiB", 1, False, id="2.5gib-1vcpu-too-small"),
        pytest.param("3GiB", 2, False, id="3gib-2vcpu-too-small"),
        pytest.param("3.6GiB", 2, False, id="3.6gib-2vcpu-too-small"),
        pytest.param("4GiB", 2, True, id="4gib-2vcpu-fits"),
    ],
)
def test_memory_per_shard_floor_follows_the_vcpus(guest, vcpus, fits):
    """Production mode refuses to start below 1 GiB per shard, so 3GiB fits 1 vCPU but not 2."""
    params = reserve_params(reserve="", guest=guest, vcpus=vcpus)
    if fits:
        check_scylla_memory_budget(params)
    else:
        with pytest.raises(MinicloudError, match="below 1GiB per shard"):
            check_scylla_memory_budget(params)


def test_memory_per_shard_skipped_outside_lightweight_mode():
    """check_scylla_memory_budget skips the per-shard floor outside lightweight mode."""
    check_scylla_memory_budget(reserve_params(reserve="", guest="1GiB", vcpus=4, minicloud_lightweight=False))


def test_preflight_check_rejects_an_impossible_request(tmp_path):
    """preflight_check fails before provisioning when the lightweight guest request does not fit."""
    # the other half of the contract above: unhonourable is fatal before anything is provisioned,
    # and going through preflight_check also proves the gate is actually wired in
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    params = reserve_params(guest="3GiB", vcpus=1, n_db_nodes=1)

    with (
        kvm_path_patch(kvm_exists=True),
        _meminfo_path_patch(64 * 1024 * 1024),  # plenty, so only the guest gate can fail
        patch("sdcm.utils.minicloud.manager.shutil.which", return_value="/usr/bin/docker"),
        pytest.raises(MinicloudError, match="does not fit"),
    ):
        manager.preflight_check(skip_aws_creds=True, params=params)


def test_host_available_memory_reads_vm_stat_on_macos():
    """host_available_memory_gib sums the free and inactive pages from vm_stat on macOS."""
    # (65536 free + 196608 inactive) pages x 16 KiB = 4 GiB
    with (
        no_meminfo_path_patch(),
        patch("sdcm.utils.minicloud.preflight.sys.platform", "darwin"),
        patch(
            "sdcm.utils.minicloud.preflight.subprocess.run",
            return_value=MagicMock(returncode=0, stdout=VM_STAT_OUTPUT),
        ),
    ):
        assert host_available_memory_gib() == 4.0


def test_check_host_memory_fails_on_macos_when_guests_exceed_available(tmp_path):
    """_check_host_memory on macOS fails when the guests exceed the vm_stat available memory."""
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    params = {"n_db_nodes": 3, "n_loaders": 1, "n_monitor_nodes": 0}
    with (
        no_meminfo_path_patch(),
        patch("sdcm.utils.minicloud.preflight.sys.platform", "darwin"),
        patch(
            "sdcm.utils.minicloud.preflight.subprocess.run",
            return_value=MagicMock(returncode=0, stdout=VM_STAT_OUTPUT),
        ),
        pytest.raises(MinicloudError, match="4 guest.*4.0GiB"),
    ):
        manager._check_host_memory(params)


def test_host_available_memory_is_none_without_a_source():
    """host_available_memory_gib returns None when neither /proc/meminfo nor vm_stat is usable."""
    with no_meminfo_path_patch(), patch("sdcm.utils.minicloud.preflight.sys.platform", "freebsd"):
        assert host_available_memory_gib() is None


def test_db_guest_count_counts_the_peak_db_guests():
    """db_guest_count counts the peak db guests, using cluster_target_size when it is larger."""
    assert db_guest_count({"n_db_nodes": [3, 3]}) == 6
    assert db_guest_count({"n_db_nodes": 3, "cluster_target_size": 5}) == 5


def test_db_guest_count_counts_an_oracle_cluster_only_when_one_is_provisioned():
    """db_guest_count adds the oracle guests only for a mixed db_type."""
    oracle = {"n_db_nodes": 3, "n_test_oracle_db_nodes": 1}
    assert db_guest_count({**oracle, "db_type": "mixed_scylla"}) == 4
    assert db_guest_count({**oracle, "db_type": "scylla"}) == 3
