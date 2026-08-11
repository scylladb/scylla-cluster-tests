"""Tests for the emulated network ranges and the host TUN/route setup.

Both halves exist to keep emulated guest traffic off the real cloud's ranges: the host
running the guests is usually an sct-runner inside a real VPC.
"""

import subprocess
from ipaddress import ip_network
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from google.api_core import exceptions as google_exceptions

from sdcm.keystore import KeyStore
from sdcm.sct_config import AWS_SUPPORTED_REGIONS
from sdcm.utils.aws_region import AwsRegion
from sdcm.utils.minicloud.config import (
    MINICLOUD_GCE_REGION_INDEX_OFFSET,
    MINICLOUD_GCE_REGIONS,
    MINICLOUD_GCE_SUBNET_CIDR_TMPL,
    MINICLOUD_HOST_VPC_ROUTES,
    MINICLOUD_TUN_ADDR,
    MinicloudConfig,
    MinicloudError,
)
from sdcm.utils.minicloud.gcp import prepare_gce_network
from sdcm.utils.minicloud.networking import LEGACY_BLANKET_ROUTE, setup_host_networking

MINICLOUD_ENV_VARS = ("AWS_ENDPOINT_URL", "GCE_ENDPOINT_URL", "SCT_MINICLOUD_ENDPOINT_URL")


def test_aws_region_vpc_cidr_shifted_when_minicloud_active(monkeypatch):
    """With minicloud active, the emulated VPC CIDR moves into the dedicated routed range."""
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:5000")
    with patch("sdcm.utils.aws_region.boto3"):
        region = AwsRegion("eu-west-1")  # index 4 in all_aws_regions(cached=True)
    assert region.vpc_ipv4_cidr == ip_network("10.164.0.0/16")
    assert region.vpc_ipv4_cidr.subnet_of(ip_network(MINICLOUD_HOST_VPC_ROUTES[0]))


def test_aws_region_vpc_cidr_unshifted_without_minicloud(monkeypatch):
    """Without minicloud, the real VPC CIDR derivation is untouched."""
    for var in MINICLOUD_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    with patch("sdcm.utils.aws_region.boto3"):
        region = AwsRegion("eu-west-1")
    assert region.vpc_ipv4_cidr == ip_network("10.4.0.0/16")


def test_every_supported_region_shifted_cidr_is_covered_by_host_routes(monkeypatch):
    """The routed 10.160.0.0/11 must cover the shifted CIDR of every SCT-supported region."""
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:5000")
    routed = ip_network(MINICLOUD_HOST_VPC_ROUTES[0])
    with patch("sdcm.utils.aws_region.boto3"):
        for region_name in AWS_SUPPORTED_REGIONS:
            assert AwsRegion(region_name).vpc_ipv4_cidr.subnet_of(routed)


# --- host networking setup (route overrides for minicloud-setup.sh) ---


class FakeHostNetwork:
    """Simulates the host's ip/docker/sudo command surface for setup_host_networking().

    ``setup_applies`` mirrors whether the image's minicloud-setup.sh honours the
    MINICLOUD_VPC_ROUTES override (scylladb/minicloud#187) or installs its legacy
    hardcoded ranges.
    """

    def __init__(
        self, tun_up=False, routes=(), setup_applies=True, tun_stdout=b"inet 10.127.0.1/24 scope global minicloud0"
    ):
        self.tun_up = tun_up
        self.routes = set(routes)
        self.setup_applies = setup_applies
        self.tun_stdout = tun_stdout
        self.commands = []

    def run(self, cmd, **_):
        self.commands.append(list(cmd))
        if cmd[:3] == ["ip", "addr", "show"]:
            if self.tun_up:
                return subprocess.CompletedProcess(cmd, 0, stdout=self.tun_stdout, stderr=b"")
            return subprocess.CompletedProcess(cmd, 1, stdout=b"", stderr=b'Device "minicloud0" does not exist.')
        if cmd[:3] == ["ip", "route", "show"]:
            route = cmd[3]
            stdout = f"{route} dev minicloud0 scope link".encode() if route in self.routes else b""
            return subprocess.CompletedProcess(cmd, 0, stdout=stdout, stderr=b"")
        if cmd[0] == "docker":
            return subprocess.CompletedProcess(cmd, 0, stdout=b"#!/bin/bash\ntrue\n", stderr=b"")
        if cmd[0] == "sudo":
            self.tun_up = True
            if self.setup_applies:
                self.routes = set(MINICLOUD_HOST_VPC_ROUTES)
                self.tun_stdout = b"inet 10.127.0.1/24 scope global minicloud0"
            else:
                self.routes = {LEGACY_BLANKET_ROUTE, "172.31.0.0/16"}
            return subprocess.CompletedProcess(cmd, 0, stdout=b"", stderr=b"")
        raise AssertionError(f"unexpected command: {cmd}")

    def sudo_commands(self):
        return [cmd for cmd in self.commands if cmd[0] == "sudo"]


def test_setup_host_networking_passes_range_overrides_to_setup_script(tmp_path):
    """The script runs under sudo with the narrowed MINICLOUD_TUN_ADDR/MINICLOUD_VPC_ROUTES."""
    fake = FakeHostNetwork()
    config = MinicloudConfig(docker_image="ghcr.io/scylladb/minicloud:test", state_dir=str(tmp_path))
    with patch("sdcm.utils.minicloud.networking.subprocess.run", side_effect=fake.run):
        setup_host_networking(config)
    (sudo_cmd,) = fake.sudo_commands()
    assert f"MINICLOUD_TUN_ADDR={MINICLOUD_TUN_ADDR}" in sudo_cmd
    assert f"MINICLOUD_VPC_ROUTES={' '.join(MINICLOUD_HOST_VPC_ROUTES)}" in sudo_cmd


def test_setup_host_networking_skips_when_already_configured(tmp_path):
    """A host already carrying the narrowed routes is left alone - no docker, no sudo."""
    fake = FakeHostNetwork(tun_up=True, routes=MINICLOUD_HOST_VPC_ROUTES)
    config = MinicloudConfig(docker_image="ghcr.io/scylladb/minicloud:test", state_dir=str(tmp_path))
    with patch("sdcm.utils.minicloud.networking.subprocess.run", side_effect=fake.run):
        setup_host_networking(config)
    assert not fake.sudo_commands()
    assert not [cmd for cmd in fake.commands if cmd[0] == "docker"]


def test_setup_host_networking_reconfigures_when_legacy_blanket_route_present(tmp_path):
    """A stale 10.0.0.0/8 from an older image forces a re-run instead of an early return."""
    fake = FakeHostNetwork(tun_up=True, routes=(LEGACY_BLANKET_ROUTE, "172.31.0.0/16"))
    config = MinicloudConfig(docker_image="ghcr.io/scylladb/minicloud:test", state_dir=str(tmp_path))
    with patch("sdcm.utils.minicloud.networking.subprocess.run", side_effect=fake.run):
        setup_host_networking(config)
    assert fake.sudo_commands()
    assert LEGACY_BLANKET_ROUTE not in fake.routes


def test_setup_host_networking_old_image_ignoring_overrides_raises(tmp_path):
    """An image whose setup script predates the overrides fails fast with the reason."""
    fake = FakeHostNetwork(setup_applies=False)
    config = MinicloudConfig(docker_image="ghcr.io/scylladb/minicloud:test", state_dir=str(tmp_path))
    with patch("sdcm.utils.minicloud.networking.subprocess.run", side_effect=fake.run):
        with pytest.raises(MinicloudError, match="MINICLOUD_VPC_ROUTES override"):
            setup_host_networking(config)


def test_setup_host_networking_reconfigures_on_wrong_tun_address(tmp_path):
    """A near-miss TUN address (10.127.0.10 vs 10.127.0.1) must trigger setup, not pass the check."""
    fake = FakeHostNetwork(
        tun_up=True,
        routes=MINICLOUD_HOST_VPC_ROUTES,
        tun_stdout=b"inet 10.127.0.10/24 scope global minicloud0",
    )
    config = MinicloudConfig(docker_image="ghcr.io/scylladb/minicloud:test", state_dir=str(tmp_path))
    with patch("sdcm.utils.minicloud.networking.subprocess.run", side_effect=fake.run):
        setup_host_networking(config)
    assert fake.sudo_commands()


# --- emulated GCE network preparation (routed qa-vpc subnets) ---


def _expected_cidr(region):
    """The CIDR prepare_gce_network must give this region's subnet."""
    return MINICLOUD_GCE_SUBNET_CIDR_TMPL.format(
        MINICLOUD_GCE_REGION_INDEX_OFFSET + MINICLOUD_GCE_REGIONS.index(region)
    )


def _prepare_gce_network_with_mocks(existing_network=True, existing_subnets=True, existing_cidr=None):
    """Run prepare_gce_network with mocked compute clients; returns (networks, subnets) mocks.

    ``existing_cidr`` overrides what an already-present subnet reports, to exercise the
    validation that rejects a leftover auto-mode range on a reused emulator.
    """
    networks = MagicMock()
    subnets = MagicMock()
    if not existing_network:
        networks.get.side_effect = google_exceptions.NotFound("no network")
    if not existing_subnets:
        subnets.get.side_effect = google_exceptions.NotFound("no subnet")
    else:
        # an existing subnet reports the CIDR this function would have given it, unless the
        # test asks for a mismatch
        subnets.get.side_effect = lambda project, region, subnetwork: SimpleNamespace(
            ip_cidr_range=existing_cidr or _expected_cidr(region)
        )
    compute = MagicMock()
    compute.NetworksClient.return_value = networks
    compute.SubnetworksClient.return_value = subnets
    # resource constructors keep their kwargs inspectable instead of vanishing into MagicMock
    compute.Subnetwork.side_effect = SimpleNamespace
    compute.Network.side_effect = SimpleNamespace
    config = MinicloudConfig(docker_image="ghcr.io/scylladb/minicloud:test")
    with (
        patch.object(KeyStore, "get_gcp_credentials", return_value={"project_id": "sct-project-1"}),
        patch("sdcm.utils.minicloud.gcp.service_account"),
        patch("sdcm.utils.minicloud.gcp.compute_v1", compute),
        patch("sdcm.utils.gce_utils._gce_client_options", return_value={}),
    ):
        prepare_gce_network(config)
    return networks, subnets


def test_prepare_gce_network_creates_missing_network_and_subnets():
    """A fresh emulator gets qa-vpc (custom mode) plus one routed subnet per region."""
    networks, subnets = _prepare_gce_network_with_mocks(existing_network=False, existing_subnets=False)
    assert networks.insert.call_count == 1
    assert subnets.insert.call_count == len(MINICLOUD_GCE_REGIONS)
    routed = ip_network(MINICLOUD_HOST_VPC_ROUTES[0])
    for call in subnets.insert.call_args_list:
        subnet = call.kwargs["subnetwork_resource"]
        assert ip_network(subnet.ip_cidr_range).subnet_of(routed)


def test_prepare_gce_network_is_idempotent():
    """Existing network and subnets are left alone - safe against a live keep_alive emulator."""
    networks, subnets = _prepare_gce_network_with_mocks()
    networks.insert.assert_not_called()
    subnets.insert.assert_not_called()


def test_gce_subnet_range_is_disjoint_from_aws_shifted_range(monkeypatch):
    """GCE guest subnets must never collide with the shifted AWS emulated VPCs."""
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:5000")
    gce_cidrs = [
        ip_network(MINICLOUD_GCE_SUBNET_CIDR_TMPL.format(MINICLOUD_GCE_REGION_INDEX_OFFSET + i))
        for i in range(len(MINICLOUD_GCE_REGIONS))
    ]
    with patch("sdcm.utils.aws_region.boto3"):
        aws_cidrs = [AwsRegion(region).vpc_ipv4_cidr for region in AWS_SUPPORTED_REGIONS]
    routed = ip_network(MINICLOUD_HOST_VPC_ROUTES[0])
    for gce_cidr in gce_cidrs:
        assert gce_cidr.subnet_of(routed)
        assert all(not gce_cidr.overlaps(aws_cidr) for aws_cidr in aws_cidrs)


def test_prepare_gce_network_rejects_a_leftover_auto_mode_subnet():
    """A reused emulator carrying an auto-mode /20 must fail, not be silently accepted."""
    with pytest.raises(MinicloudError, match="outside the host-routed range"):
        _prepare_gce_network_with_mocks(existing_cidr="10.128.0.0/20")
