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

"""The guest-side script configuring addresses and policy routing for secondary Azure NICs.

It runs on the node, so it is exercised here directly: the embedded Python is fed an IMDS payload
and a stub 'ip' binary, and the commands it issues are asserted.
"""

import json
import os
import subprocess
import sys
import textwrap

import pytest

from sdcm.utils.azure_utils import SECONDARY_NICS_SCRIPT

PRIMARY_MAC = "000D3A111111"
SECONDARY_MAC = "000D3A222222"


def embedded_python() -> str:
    """The Python program the bash wrapper pipes the IMDS payload into."""
    return SECONDARY_NICS_SCRIPT.split("python3 -c '")[1].rsplit('\' "$EXPECTED_NICS"', 1)[0]


def imds_interface(mac: str, private_ip: str, subnet: str, prefix: str = "24") -> dict:
    return {
        "ipv4": {
            "ipAddress": [{"privateIpAddress": private_ip, "publicIpAddress": ""}],
            "subnet": [{"address": subnet, "prefix": prefix}],
        },
        "ipv6": {"ipAddress": []},
        "macAddress": mac,
    }


@pytest.fixture(name="run_script")
def fixture_run_script(tmp_path):
    """Run the embedded program against a stub 'ip' binary, returning the commands it issued."""
    log = tmp_path / "ip.log"
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    stub = stub_dir / "ip"
    stub.write_text(
        textwrap.dedent(f"""\
        #!/usr/bin/env python3
        import sys
        args = sys.argv[1:]
        with open({str(log)!r}, "a", encoding="utf-8") as log_file:
            log_file.write(" ".join(args) + "\\n")
        if args[:2] == ["-o", "link"]:
            print("1: lo: <LOOPBACK> link/loopback 00:00:00:00:00:00")
            print("2: eth0: <BROADCAST> link/ether 00:0d:3a:11:11:11 brd ff:ff:ff:ff:ff:ff")
            print("3: eth1: <BROADCAST> link/ether 00:0d:3a:22:22:22 brd ff:ff:ff:ff:ff:ff")
        # "ip rule del" must eventually fail, or the drain loop would never stop
        if args[:2] == ["rule", "del"] or args[:3] == ["-6", "rule", "del"]:
            sys.exit(2)
        """)
    )
    stub.chmod(0o755)

    def _run(interfaces: list[dict], expected_nics: int):
        env = dict(os.environ, PATH=f"{stub_dir}:{os.environ['PATH']}")
        result = subprocess.run(
            [sys.executable, "-c", embedded_python(), str(expected_nics)],
            input=json.dumps({"interface": interfaces}),
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        commands = log.read_text().splitlines() if log.exists() else []
        return result, [command for command in commands if command != "-o link"]

    return _run


def test_primary_nic_is_left_untouched(run_script):
    """The primary NIC already owns the default route SCT reaches the node through."""
    result, commands = run_script([imds_interface(PRIMARY_MAC, "10.0.0.4", "10.0.0.0")], expected_nics=1)

    assert result.returncode == 0, result.stderr
    assert commands == []


def test_secondary_nic_gets_address_rule_and_routes(run_script):
    result, commands = run_script(
        [
            imds_interface(PRIMARY_MAC, "10.0.0.4", "10.0.0.0"),
            imds_interface(SECONDARY_MAC, "10.0.1.4", "10.0.1.0"),
        ],
        expected_nics=2,
    )

    assert result.returncode == 0, result.stderr
    assert "link set dev eth1 up" in commands
    assert "addr replace 10.0.1.4/24 dev eth1" in commands
    assert "rule add from 10.0.1.4 lookup 101 priority 101" in commands
    assert "route replace 10.0.1.0/24 dev eth1 table 101" in commands
    # Azure reserves the first usable address of a subnet as its gateway
    assert "route replace default via 10.0.1.1 dev eth1 table 101" in commands


def test_each_secondary_nic_gets_its_own_routing_table(run_script):
    result, commands = run_script(
        [
            imds_interface(PRIMARY_MAC, "10.0.0.4", "10.0.0.0"),
            imds_interface(SECONDARY_MAC, "10.0.1.4", "10.0.1.0"),
            imds_interface("000D3A333333", "10.0.2.4", "10.0.2.0"),
        ],
        expected_nics=3,
    )

    # the third NIC has no OS device in the stub, so it is reported rather than silently skipped
    assert result.returncode == 1
    assert "no OS device with MAC 000D3A333333" in result.stderr
    assert "rule add from 10.0.1.4 lookup 101 priority 101" in commands


def test_incomplete_metadata_fails_loudly(run_script):
    """A half-configured NIC breaks much later as a confusing connectivity error, so fail here."""
    result, _ = run_script(
        [imds_interface(PRIMARY_MAC, "10.0.0.4", "10.0.0.0"), {"macAddress": SECONDARY_MAC, "ipv4": {}}],
        expected_nics=2,
    )

    assert result.returncode == 1
    assert "metadata is incomplete" in result.stderr


def test_rerunning_does_not_stack_duplicate_rules(run_script):
    """The script re-runs on every boot and after an interface restart, so it must be idempotent."""
    _, commands = run_script(
        [
            imds_interface(PRIMARY_MAC, "10.0.0.4", "10.0.0.0"),
            imds_interface(SECONDARY_MAC, "10.0.1.4", "10.0.1.0"),
        ],
        expected_nics=2,
    )

    assert commands.count("rule add from 10.0.1.4 lookup 101 priority 101") == 1
    assert "rule del from 10.0.1.4 lookup 101" in commands


def imds_dual_stack_interface(mac: str, private_ip: str, subnet: str, ipv6_address: str, ipv6_subnet: str) -> dict:
    interface = imds_interface(mac, private_ip, subnet)
    interface["ipv6"] = {
        "ipAddress": [{"privateIpAddress": ipv6_address}],
        "subnet": [{"address": ipv6_subnet, "prefix": "64"}],
    }
    return interface


def test_dual_stack_secondary_nic_gets_ipv6_address_rule_and_routes(run_script):
    result, commands = run_script(
        [
            imds_dual_stack_interface(PRIMARY_MAC, "10.0.0.4", "10.0.0.0", "fd00:db8:5c7::4", "fd00:db8:5c7::"),
            imds_dual_stack_interface(SECONDARY_MAC, "10.0.1.4", "10.0.1.0", "fd00:db8:5c7:1::4", "fd00:db8:5c7:1::"),
        ],
        expected_nics=2,
    )

    assert result.returncode == 0, result.stderr
    assert "-6 addr replace fd00:db8:5c7:1::4/128 dev eth1" in commands
    assert "-6 rule add from fd00:db8:5c7:1::4 lookup 101 priority 101" in commands
    assert "-6 route replace fd00:db8:5c7:1::/64 dev eth1 table 101" in commands
    # Azure publishes no IPv6 gateway, so it is derived as the first address of the subnet prefix
    assert "-6 route replace default via fd00:db8:5c7:1::1 dev eth1 table 101" in commands


def test_ipv4_only_nic_issues_no_ipv6_command(run_script):
    result, commands = run_script(
        [
            imds_interface(PRIMARY_MAC, "10.0.0.4", "10.0.0.0"),
            imds_interface(SECONDARY_MAC, "10.0.1.4", "10.0.1.0"),
        ],
        expected_nics=2,
    )

    assert result.returncode == 0, result.stderr
    assert not [command for command in commands if command.startswith("-6")]


def test_ipv4_routing_is_unaffected_by_the_ipv6_configuration(run_script):
    result, commands = run_script(
        [
            imds_dual_stack_interface(PRIMARY_MAC, "10.0.0.4", "10.0.0.0", "fd00:db8:5c7::4", "fd00:db8:5c7::"),
            imds_dual_stack_interface(SECONDARY_MAC, "10.0.1.4", "10.0.1.0", "fd00:db8:5c7:1::4", "fd00:db8:5c7:1::"),
        ],
        expected_nics=2,
    )

    assert result.returncode == 0, result.stderr
    assert "rule add from 10.0.1.4 lookup 101 priority 101" in commands
    assert "route replace default via 10.0.1.1 dev eth1 table 101" in commands
