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
"""The guest firewall is taken down from cloud-init, by one script, through both boot paths.

The OCI images are what made this necessary - they accept nothing but SSH and restore that
ruleset on every boot (SCT-479). The script is backend- and distro-agnostic and both cloud-init
paths run it: the `SctUserDataObject` list used by gce/azure/oci/k8s-gke, and
`ConfigurationScriptBuilder`, which AWS builds its user data from and which every backend runs
as its startup script. Which backends actually do it is one tuple, and only OCI is in it today.
"""

from unittest.mock import Mock

import pytest

from sdcm.provision.common.configuration_script import ConfigurationScriptBuilder
from sdcm.provision.common.utils import disable_firewall, guest_firewall_needs_disabling
from sdcm.sct_provision.user_data_objects.firewall import DisableFirewallUserDataObject


def _user_data_object(backend: str = "oci") -> DisableFirewallUserDataObject:
    return DisableFirewallUserDataObject(
        test_config=Mock(), params={"cluster_backend": backend}, instance_name="node-1", node_type="scylla-db"
    )


@pytest.mark.parametrize(
    ("backend", "disabled"),
    [("oci", True), ("aws", False), ("gce", False), ("azure", False), ("k8s-gke", False)],
)
def test_only_oci_boots_with_its_firewall_taken_down(backend, disabled):
    """OCI is the only backend whose images ship a ruleset that has to go.

    The rest have nothing to disable, and doing it anyway would change their nodes' behaviour
    with no failure behind it. Both cloud-init paths ask the same question, so widening it is
    one tuple in `sdcm/provision/common/utils.py`.
    """
    assert guest_firewall_needs_disabling({"cluster_backend": backend}) is disabled
    assert _user_data_object(backend).is_applicable is disabled


def test_the_boot_script_itself_has_nothing_backend_specific_in_it():
    """Which is what makes widening that tuple the only change another backend would need."""
    script = _user_data_object().script_to_run

    for backend in ("oci", "aws", "gce", "azure"):
        assert backend not in script


def test_the_boot_script_removes_what_restores_the_ruleset():
    """Flushing the live tables is not what makes it stick - dropping the saved rules is."""
    script = _user_data_object().script_to_run

    assert "rm -f /etc/iptables/rules.v4 /etc/iptables/rules.v6" in script
    assert "systemctl disable --now $service" in script
    for service in ("ufw", "netfilter-persistent", "nftables"):
        assert service in script
    # the IPv6 rules are as persistent as the IPv4 ones, and carry the same REJECT
    assert "for iptables in iptables ip6tables" in script
    assert "$iptables -F" in script
    assert "$iptables -P $chain ACCEPT" in script


def test_the_boot_script_skips_the_tooling_a_distro_does_not_have():
    """Which is what lets one script serve every distro, with no branch of its own.

    `systemctl disable --now` of a unit which is not there fails harmlessly, and the tables are
    only flushed with a tool the node actually has - so centos, rhel and ubuntu all run it as is.
    """
    script = _user_data_object().script_to_run

    assert "command -v $iptables >/dev/null || continue" in script
    assert script.count("|| true") >= 4, "every command must be allowed to fail, the script runs under 'set -e'"


def test_the_other_boot_path_runs_the_very_same_script():
    """`ConfigurationScriptBuilder` is the AWS user data and the startup script of every backend.

    It is what makes the base ready for a backend which is not OCI: no second implementation to
    keep in step, only the tuple deciding who gets it.
    """
    script = ConfigurationScriptBuilder(logs_transport="libssh2", disable_guest_firewall=True).to_string()

    assert disable_firewall() in script
    # first thing on the node: whatever comes after needs the node to be reachable
    assert script.index("netfilter-persistent") < script.index("backoff")


def test_a_boot_script_leaves_the_tables_alone_unless_asked():
    """The default is off, so a caller which re-runs the script on a live node cannot surprise.

    `configure_remote_logging()` rebuilds it mid-test to re-point the logs, and flushing the
    tables there would take the rules of a running network nemesis with it.
    """
    script = ConfigurationScriptBuilder(logs_transport="libssh2").to_string()

    assert "netfilter-persistent" not in script
