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
import subprocess
from types import SimpleNamespace
from unittest.mock import Mock

from invoke.runners import Result

from sdcm.logcollector import LogCollector
from sdcm.provision.azure.provisioner import AzureProvisioner
from sdcm.provision.oci.provisioner import OciProvisioner
from sdcm.utils.oob_runner import ProvisionerCommandRunner, oob_runner_for

AZURE_OUTPUT_LIMIT = 4096


class LocalAgentInstance:
    """A VmInstance whose run_command() runs locally, keeping only the last 4 KB of stdout like Azure."""

    name = "loader-node-1"

    def __init__(self):
        self.calls = []

    def run_command(self, command: str) -> Result:
        self.calls.append(command)
        done = subprocess.run(["bash", "-c", command], capture_output=True, text=True, check=False)
        return Result(stdout=done.stdout[-AZURE_OUTPUT_LIMIT:], stderr=done.stderr, exited=done.returncode)


def _journal(lines: int) -> str:
    return "".join(
        f"Nov 17 10:00:{n % 60:02} loader sshd[42]: Connection closed by user ubuntu {n} [preauth]\n"
        for n in range(lines)
    )


def test_a_log_larger_than_the_agent_output_comes_back_whole(tmp_path):
    source, local = tmp_path / "journal.txt", tmp_path / "out" / "journal.log"
    source.write_text(_journal(20_000))  # ~1.6 MB, far over 4 KB per call
    local.parent.mkdir()
    instance = LocalAgentInstance()
    runner = ProvisionerCommandRunner(instance, chunk_size=3000, max_chunks=100)

    assert runner.run_command_and_save_output(f"cat {source}", str(local)).ok

    assert local.read_text() == source.read_text()
    assert len(instance.calls) > 3  # staged, read in slices, removed
    assert instance.calls[-1].startswith("rm -f /tmp/sct-oob-")


def test_a_log_over_the_call_budget_keeps_what_arrived(tmp_path):
    source, local = tmp_path / "journal.txt", tmp_path / "journal.log"
    source.write_bytes(
        subprocess.run(["head", "-c", "200000", "/dev/urandom"], capture_output=True, check=True).stdout.hex().encode()
    )
    runner = ProvisionerCommandRunner(LocalAgentInstance(), chunk_size=3000, max_chunks=2)

    assert runner.run_command_and_save_output(f"cat {source}", str(local)).ok

    collected = local.read_text()
    assert collected and source.read_text().startswith(collected)


def test_only_provisioners_with_a_command_channel_get_a_runner():
    def instance_of(provisioner):
        return SimpleNamespace(name="node", _provisioner=provisioner)

    assert oob_runner_for(instance_of(Mock(spec=AzureProvisioner, oob_chunk_size=3000, oob_max_chunks=24)))
    assert oob_runner_for(instance_of(Mock(spec=OciProvisioner, oob_chunk_size=500_000, oob_max_chunks=20)))
    assert oob_runner_for(instance_of(Mock(oob_chunk_size=None))) is None  # GCE: run_command() needs SSH
    assert oob_runner_for(SimpleNamespace(name="gce-instance")) is None  # a GCE compute Instance
    assert oob_runner_for(None) is None
    assert AzureProvisioner.oob_chunk_size < AZURE_OUTPUT_LIMIT


def test_collect_log_remotely_goes_through_the_runner_of_a_node_without_ssh(tmp_path):
    source, local = tmp_path / "cloud-init.log", tmp_path / "node" / "cloud-init.log"
    source.write_text(_journal(500))
    runner = ProvisionerCommandRunner(LocalAgentInstance(), chunk_size=3000, max_chunks=24)
    node = SimpleNamespace(name="loader-node-1", remoter=Mock(is_up=Mock(return_value=False)), oob_runner=runner)

    assert LogCollector.collect_log_remotely(node, f"cat {source}", str(local)) == (str(local), False)
    assert local.read_text() == source.read_text()


def test_collect_log_remotely_survives_an_sdk_error(tmp_path):
    runner = Mock(run_command_and_save_output=Mock(side_effect=OSError("agent unreachable")), instance_id="node")
    node = SimpleNamespace(name="loader-node-1", remoter=Mock(is_up=Mock(return_value=False)), oob_runner=runner)

    assert LogCollector.collect_log_remotely(node, "journalctl", str(tmp_path / "system.log")) == (None, False)
