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

import re
from types import SimpleNamespace
from unittest import mock

import pytest
import yaml

from sdcm.cluster import BaseNode
from sdcm.cluster_baremetal import PhysicalMachineNode
from sdcm.provision.common.configuration_script import ConfigurationScriptBuilder

CHECKPOINT = "/var/lib/vector/journald/checkpoint.txt"


def vector_journald_source(script: str) -> dict:
    return yaml.safe_load(re.search(r"<<'EOF'\n(.*?)\nEOF", script, re.S).group(1))["sources"]["journald"]


@pytest.mark.parametrize("journal_from_now", [False, True])
def test_vector_reads_the_journal_from_now_only_when_asked(journal_from_now):
    """Asked to, vector drops its checkpoint while stopped and starts at the present; by default nothing changes."""
    script = ConfigurationScriptBuilder(
        syslog_host_port=("10.0.0.1", 5003), logs_transport="vector", vector_journal_from_now=journal_from_now
    ).to_string()

    assert vector_journald_source(script).get("since_now", False) is journal_from_now
    if journal_from_now:
        stop, drop, start = (
            script.index("systemctl stop vector"),
            script.index(f"rm -f {CHECKPOINT}"),
            script.index("systemctl restart vector"),
        )
        assert stop < drop < start
    else:
        assert CHECKPOINT not in script


@pytest.mark.parametrize(
    "node_class,journal_since,expected",
    [
        pytest.param(BaseNode, None, False, id="node-created-for-the-run"),
        pytest.param(PhysicalMachineNode, None, True, id="physical-host-at-run-start"),
        pytest.param(PhysicalMachineNode, "@1791459190", False, id="physical-host-later-in-the-run"),
    ],
)
def test_only_a_host_that_outlives_the_run_reads_vector_journal_from_now(node_class, journal_since, expected):
    """A physical host drops vector's checkpoint once, at the run start; later in the run it keeps this run's."""
    node = node_class.__new__(node_class)
    node.name = "node-1"
    node.journal_since = journal_since
    node.parent_cluster = SimpleNamespace(params={"logs_transport": "vector"})
    node.test_config = mock.Mock(get_logging_service_host_port=mock.Mock(return_value=("127.0.0.1", 5003)))
    node.remoter = mock.Mock()

    with mock.patch("sdcm.cluster.ConfigurationScriptBuilder") as builder:
        node.configure_remote_logging()

    assert builder.call_args.kwargs["vector_journal_from_now"] is expected
    node.remoter.sudo.assert_called_once()
