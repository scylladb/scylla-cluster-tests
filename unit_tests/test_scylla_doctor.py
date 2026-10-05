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
# Copyright (c) 2020 ScyllaDB

from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import pytest

from utils.scylla_doctor import ScyllaDoctor


@dataclass
class FakeParentCluster:
    cluster_backend: str = "aws"


class FakeNode:
    """Minimal fake node for ScyllaDoctor unit tests."""

    def __init__(self):
        self.name = "test-node"
        self.is_nonroot_install = False
        self.public_dns_name = "test-node.local"
        self.parent_cluster = FakeParentCluster()
        self.remoter = MagicMock()


@pytest.fixture()
def doctor():
    """Create a ScyllaDoctor instance with fake dependencies."""
    return ScyllaDoctor(node=FakeNode(), test_config=MagicMock(), offline_install=True)


# --- run_scylla_doctor_and_collect_results: log archive location tests ---


def _ls_side_effects(cwd_archive: str = "", tmp_archive: str = ""):
    """Build remoter.run results for the vitals lookup followed by the log archive lookup."""
    vitals = MagicMock()
    vitals.stdout = "test-node.local.vitals.json\n"
    archives = MagicMock()
    archives.stdout = "\n".join(path for path in (cwd_archive, tmp_archive) if path)
    return [vitals, archives]


@pytest.mark.parametrize(
    "cwd_archive,tmp_archive,expected",
    [
        pytest.param(
            "scylla_logs_20260914132345.tar.gz",
            "",
            "scylla_logs_20260914132345.tar.gz",
            id="login_dir_up_to_1_13",
        ),
        pytest.param(
            "",
            "/tmp/scylla_logs_20260914132345.tar.gz",
            "/tmp/scylla_logs_20260914132345.tar.gz",
            id="temp_dir_since_1_14",
        ),
    ],
)
def test_collect_results_finds_log_archive_in_either_location(doctor, cwd_archive, tmp_archive, expected):
    """scylla-doctor 1.14 moved the log archive to the temp dir; both locations must be accepted."""
    doctor.node.parent_cluster.get_db_auth = MagicMock(return_value=None)
    doctor.node.remoter.run.side_effect = _ls_side_effects(cwd_archive, tmp_archive)

    with patch.object(ScyllaDoctor, "_ensure_lspci"), patch.object(ScyllaDoctor, "run"):
        doctor.run_scylla_doctor_and_collect_results()

    assert doctor.scylla_logs_file == expected


def test_collect_results_fails_when_log_archive_is_missing_everywhere(doctor):
    """A genuinely missing log archive must still fail the test."""
    doctor.node.parent_cluster.get_db_auth = MagicMock(return_value=None)
    doctor.node.remoter.run.side_effect = _ls_side_effects()

    with (
        patch.object(ScyllaDoctor, "_ensure_lspci"),
        patch.object(ScyllaDoctor, "run"),
        pytest.raises(AssertionError, match="Scylla log archive has not been created"),
    ):
        doctor.run_scylla_doctor_and_collect_results()


def test_collect_results_skips_log_archive_lookup_on_docker(doctor):
    """Scylla Docker does not collect cluster logs - field-engineering#2288."""
    doctor.node.parent_cluster.cluster_backend = "docker"
    doctor.node.parent_cluster.get_db_auth = MagicMock(return_value=None)
    doctor.node.remoter.run.side_effect = _ls_side_effects()

    with patch.object(ScyllaDoctor, "_ensure_lspci"), patch.object(ScyllaDoctor, "run"):
        doctor.run_scylla_doctor_and_collect_results()

    assert doctor.scylla_logs_file == ""
    assert doctor.node.remoter.run.call_count == 1


@pytest.mark.parametrize("lspci_present", [True, False], ids=["lspci_present", "lspci_missing"])
def test_ensure_lspci_installs_pciutils_only_when_missing(doctor, lspci_present):
    """scylla-doctor 1.14's LSPCICollector fails without lspci, so pciutils is installed when missing."""
    doctor.node.remoter.sudo.return_value = MagicMock(ok=lspci_present)
    doctor.node.install_package = MagicMock()

    doctor._ensure_lspci()

    if lspci_present:
        doctor.node.install_package.assert_not_called()
    else:
        doctor.node.install_package.assert_called_once_with("pciutils")
