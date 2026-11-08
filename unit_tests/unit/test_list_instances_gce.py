"""list_instances_gce must return stopped and preempted VMs, or the cleanup never deletes them."""

from unittest.mock import MagicMock, patch

import pytest
from google.cloud.compute_v1.types import AccessConfig, Instance, NetworkInterface

from sdcm.utils.common import list_instances_gce
from sdcm.utils.gce_utils import gce_public_addresses


@pytest.mark.parametrize(
    "running, expected",
    [(False, ["running-vm", "preempted-vm"]), (True, ["running-vm"])],
)
def test_list_instances_gce_filters_by_status_only_when_running(running, expected):
    instances = [MagicMock(status="RUNNING"), MagicMock(status="TERMINATED")]
    instances[0].name, instances[1].name = "running-vm", "preempted-vm"
    client = MagicMock()
    client.aggregated_list.return_value = [("zones/us-east1-b", MagicMock(instances=instances))]

    with patch("sdcm.utils.common.get_gce_compute_instances_client", return_value=(client, {"project_id": "p"})):
        found = list_instances_gce(running=running, verbose=False)

    assert [i.name for i in found] == expected


def test_stopped_vm_has_no_public_address():
    """A stopped VM keeps an access config without a NAT IP; '' must not count as an address."""
    running = Instance(network_interfaces=[NetworkInterface(access_configs=[AccessConfig(nat_i_p="1.2.3.4")])])
    stopped = Instance(network_interfaces=[NetworkInterface(access_configs=[AccessConfig(name="External NAT")])])

    assert gce_public_addresses(running) == ["1.2.3.4"]
    assert gce_public_addresses(stopped) == []
