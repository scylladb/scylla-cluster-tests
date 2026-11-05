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
"""The RHSB-2026-016 nftables mitigation for CVE-2026-98432 is installed on every SCT node (SCT-1425)."""

import subprocess

import pytest

from sdcm.provision.common.configuration_script import ConfigurationScriptBuilder
from sdcm.provision.common.utils import apply_cve_2026_98432_mitigation, minify_shell_script

RULE = "ip protocol tcp ip hdrlength > 5 counter drop"


def _boot_script(**kwargs) -> str:
    return ConfigurationScriptBuilder(syslog_host_port=("127.0.0.1", 5000), **kwargs).to_string()


def test_the_mitigation_script_is_valid_bash_after_minify():
    subprocess.run(["bash", "-n"], input=minify_shell_script(apply_cve_2026_98432_mitigation()), text=True, check=True)


def test_the_rule_is_exactly_the_red_hat_one_and_reloads_idempotently():
    """Deleting the table before adding it keeps a second run from duplicating the rule."""
    script = apply_cve_2026_98432_mitigation()

    assert RULE in script
    assert "type filter hook input priority -310; policy accept;" in script
    assert script.index("delete table inet cve_2026_98432") < script.index("table inet cve_2026_98432 {")
    assert "systemctl enable --now sct-cve-2026-98432.service" in script


@pytest.mark.parametrize("disable_guest_firewall", [True, False])
def test_every_boot_script_installs_it_before_the_rerun_exit(disable_guest_firewall):
    """A node configured before the runner reruns the script and exits early: the rule must come first.

    On OCI it must also come after `disable_firewall()`, which disables `nftables.service`.
    """
    script = _boot_script(disable_guest_firewall=disable_guest_firewall)

    assert RULE in script
    assert script.index(RULE) < script.index("exit 0")
    if disable_guest_firewall:
        assert script.index("systemctl disable --now $service") < script.index(RULE)
