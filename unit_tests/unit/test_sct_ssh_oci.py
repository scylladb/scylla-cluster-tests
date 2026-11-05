# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

from unittest.mock import MagicMock, patch

from oci.core.models import Instance, Vnic, VnicAttachment

import sct_ssh
from sdcm.utils import oci_utils

IPS = {"node": ("1.1.1.1", "10.0.0.5"), "runner": ("2.2.2.2", "10.0.0.2")}


def oci_instance(name: str, **sct_tags) -> Instance:
    return Instance(id=name, display_name=name, defined_tags={"sct": {"TestId": "t1", **sct_tags}})


NODE = oci_instance("node", ssh_user="scyllaadm", ssh_key="scylla_test_id_ed25519", NodeType="scylla-db")
RUNNER = oci_instance("runner", UserName="ubuntu", bastion="true")


def fake_ips(instance):
    return IPS[instance.id]


@patch.object(sct_ssh, "get_oci_instance_ips", side_effect=fake_ips)
@patch.object(sct_ssh, "list_instances_oci", return_value=[RUNNER])
def test_oci_proxy_command_goes_through_the_runner(list_instances, _):
    proxy_command, target_ip, target_username, target_key = sct_ssh.get_proxy_command(NODE, force_use_public_ip=False)

    assert (target_ip, target_username, target_key) == ("10.0.0.5", "scyllaadm", "~/.ssh/scylla_test_id_ed25519")
    assert "-W %h:%p ubuntu@2.2.2.2" in proxy_command
    list_instances.assert_called_once_with({"bastion": "true", "TestId": "t1"}, running=True, verbose=False)


@patch.object(sct_ssh, "get_oci_instance_ips", side_effect=fake_ips)
def test_oci_force_public_ip_connects_directly(_):
    assert sct_ssh.get_proxy_command(RUNNER, force_use_public_ip=True) == (
        "",
        "2.2.2.2",
        "ubuntu",
        "~/.ssh/scylla_test_id_ed25519",
    )


def test_get_oci_instance_ips_uses_primary_vnic_and_full_region_name():
    attachments = [
        VnicAttachment(vnic_id="secondary", lifecycle_state="ATTACHED"),
        VnicAttachment(vnic_id="primary", lifecycle_state="ATTACHED"),
    ]
    vnics = {
        "secondary": Vnic(is_primary=False, public_ip=None, private_ip="10.0.1.5"),
        "primary": Vnic(is_primary=True, public_ip="1.1.1.1", private_ip="10.0.0.5"),
    }
    service = MagicMock()
    service.get_network_client.return_value.get_vnic.side_effect = lambda vnic_id: MagicMock(data=vnics[vnic_id])
    instance = Instance(id="node", region="iad", compartment_id="c1")

    with (
        patch.object(oci_utils, "OciService", return_value=service),
        patch.object(oci_utils.oci.pagination, "list_call_get_all_results", return_value=MagicMock(data=attachments)),
    ):
        assert oci_utils.get_oci_instance_ips(instance) == ("1.1.1.1", "10.0.0.5")

    service.get_compute_client.assert_called_once_with(region="us-ashburn-1")
