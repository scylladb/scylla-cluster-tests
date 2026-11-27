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
"""SCT-1471: the sshd and cloud-init journal goes to the serial console, which SCT saves on every backend."""

import subprocess
from unittest.mock import Mock

import pytest

from sdcm.provision.aws.configuration_script import AWSConfigurationScriptBuilder
from sdcm.provision.common.configuration_script import ConfigurationScriptBuilder
from sdcm.provision.common.utils import forward_boot_journal_to_console, minify_shell_script
from sdcm.sct_provision.user_data_objects.boot_console import (
    BootJournalToConsoleUserDataObject,
    OciRunCommandSudoUserDataObject,
)

UNIT = "sct-boot-console.service"


def _user_data_object(klass, backend):
    return klass(test_config=Mock(), params={"cluster_backend": backend}, instance_name="node-1", node_type="loader")


@pytest.mark.parametrize("backend", ["gce", "azure", "oci", "k8s-gke"])
def test_every_user_data_backend_streams_the_boot_journal(backend):
    assert _user_data_object(BootJournalToConsoleUserDataObject, backend).is_applicable


@pytest.mark.parametrize(("backend", "applicable"), [("oci", True), ("azure", False), ("gce", False)])
def test_only_oci_lets_the_run_command_user_sudo(backend, applicable):
    assert _user_data_object(OciRunCommandSudoUserDataObject, backend).is_applicable is applicable


def test_aws_boot_script_streams_it_and_the_live_rerun_does_not():
    """configure_remote_logging() re-runs the base script on a live node: no unit restarts there."""
    params = {"cluster_backend": "aws"}
    aws = AWSConfigurationScriptBuilder(logs_transport="", params=params, test_config=Mock()).to_string()
    live = ConfigurationScriptBuilder(logs_transport="", params=params, test_config=Mock()).to_string()

    assert UNIT in aws
    assert UNIT not in live


def test_the_unit_follows_only_sshd_and_cloud_init():
    """Journalctl ANDs different fields: `+` is what ORs cloud-final in instead of filtering everything out."""
    script = forward_boot_journal_to_console()

    assert "SYSLOG_IDENTIFIER=sshd-session" in script
    assert "SYSLOG_IDENTIFIER=cloud-init + _SYSTEMD_UNIT=cloud-final.service" in script
    assert ">/dev/console" in script


def test_the_minified_script_is_valid_bash():
    """The unit file is a heredoc: minifying must keep it byte for byte."""
    script = minify_shell_script(forward_boot_journal_to_console())

    subprocess.run(["bash", "-n"], input=script, text=True, check=True)
    assert "[Service]\nExecStart=" in script
