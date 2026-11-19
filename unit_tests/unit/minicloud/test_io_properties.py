"""Tests for the per-guest share of the host io_properties on minicloud."""

from pathlib import Path
from unittest.mock import patch

import yaml

from sdcm.utils.minicloud.io_properties import override_content, scale_io_properties

HOST = """\
disks:
  - mountpoint: /var/lib/scylla
    read_iops: 386501
    read_bandwidth: 4862312960
    write_iops: 28405
    write_bandwidth: 168000000
    physical_block_size: 4096
"""
PARAMS = {"n_db_nodes": 3, "minicloud_io_properties_margin": 0.2}
SCYLLA_D = Path("configurations/minicloud/scylla.d")


def test_scale_gives_each_guest_its_share():
    disk = yaml.safe_load(scale_io_properties(HOST, guests=3, margin=0.2))["disks"][0]
    assert disk == {
        "mountpoint": "/var/lib/scylla",
        "read_iops": 103066,
        "read_bandwidth": 1296616789,
        "write_iops": 7574,
        "write_bandwidth": 44800000,
        "physical_block_size": 4096,
    }


def test_override_content_scales_only_io_properties_on_minicloud():
    with patch("sdcm.utils.minicloud.io_properties.is_minicloud_active", return_value=True):
        scaled = override_content(SCYLLA_D / "io_properties.yaml", HOST, PARAMS)
        io_conf = override_content(SCYLLA_D / "io.conf", "SEASTAR_IO=x\n", PARAMS)
    assert yaml.safe_load(scaled)["disks"][0]["write_bandwidth"] == 44800000
    assert io_conf == "SEASTAR_IO=x\n"


def test_override_content_leaves_other_runs_alone():
    with patch("sdcm.utils.minicloud.io_properties.is_minicloud_active", return_value=False):
        assert override_content(SCYLLA_D / "io_properties.yaml", HOST, PARAMS) == HOST
