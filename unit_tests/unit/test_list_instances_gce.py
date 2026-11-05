"""list_instances_gce must return stopped and preempted VMs, or the cleanup never deletes them."""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.utils.common import list_instances_gce


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
