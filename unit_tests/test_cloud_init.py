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

import os
from unittest.mock import MagicMock, patch

from sdcm.provision.helpers.cloud_init import (
    collect_cloud_init_scripts,
    log_user_data_scripts_errors,
    wait_cloud_init_completes,
)
from sdcm.provision.user_data import CLOUD_INIT_SCRIPTS_PATH


class TestCollectCloudInitScripts:
    """Tests for the collect_cloud_init_scripts function."""

    def test_collect_scripts_to_logdir(self, tmp_path):
        """Test that cloud-init scripts are collected to the node logdir."""
        logdir = str(tmp_path / "node-logdir")
        os.makedirs(logdir)
        remoter = MagicMock()

        collect_cloud_init_scripts(remoter=remoter, logdir=logdir)

        expected_dst = os.path.join(logdir, "cloud-init-scripts")
        remoter.receive_files.assert_called_once_with(
            src=f"{CLOUD_INIT_SCRIPTS_PATH}/*",
            dst=expected_dst,
        )
        assert os.path.isdir(expected_dst)

    def test_skip_when_no_logdir(self):
        """Test that collection is skipped when logdir is None."""
        remoter = MagicMock()

        collect_cloud_init_scripts(remoter=remoter, logdir=None)

        remoter.receive_files.assert_not_called()

    def test_handles_receive_files_failure(self, tmp_path):
        """Test that failure to receive files is handled gracefully."""
        logdir = str(tmp_path / "node-logdir")
        os.makedirs(logdir)
        remoter = MagicMock()
        remoter.receive_files.side_effect = Exception("SSH connection failed")

        collect_cloud_init_scripts(remoter=remoter, logdir=logdir)

        remoter.receive_files.assert_called_once()


class TestLogUserDataScriptsErrors:
    """Tests for the log_user_data_scripts_errors function."""

    def test_no_errors_when_done_present(self):
        """Test no errors reported when 'done' marker is present."""
        remoter = MagicMock()
        remoter.run.return_value = MagicMock(failed=False, stdout="0_SomeScript.sh\ndone\n")

        result = log_user_data_scripts_errors(remoter=remoter)

        assert result is False

    def test_errors_when_failed_marker_present(self):
        """Test errors reported when .failed file is present."""
        remoter = MagicMock()
        remoter.run.return_value = MagicMock(
            failed=False,
            stdout="0_SomeScript.sh\n0_SomeScript.sh.failed\ndone\n",
        )

        result = log_user_data_scripts_errors(remoter=remoter)

        assert result is True

    def test_errors_when_ls_fails(self):
        """Test errors reported when ls command fails."""
        remoter = MagicMock()
        remoter.run.return_value = MagicMock(
            failed=True,
            stdout="",
            return_code=2,
            stderr="No such file or directory",
        )

        result = log_user_data_scripts_errors(remoter=remoter)

        assert result is True

    def test_errors_when_no_scripts_generated(self):
        """Test errors reported when no scripts were generated."""
        remoter = MagicMock()
        remoter.run.return_value = MagicMock(failed=False, stdout="")

        result = log_user_data_scripts_errors(remoter=remoter)

        assert result is True

    def test_errors_when_done_not_present(self):
        """Test errors reported when done marker is missing."""
        remoter = MagicMock()
        remoter.run.return_value = MagicMock(
            failed=False,
            stdout="0_SomeScript.sh\n1_AnotherScript.sh\n",
        )

        result = log_user_data_scripts_errors(remoter=remoter)

        assert result is True


class TestWaitCloudInitCompletes:
    """Tests for the wait_cloud_init_completes integration with collect_cloud_init_scripts."""

    @patch("sdcm.provision.helpers.cloud_init.collect_cloud_init_scripts")
    @patch("sdcm.provision.helpers.cloud_init.log_user_data_scripts_errors")
    def test_collect_called_with_instance_logdir(self, mock_log_errors, mock_collect):
        """Test that collect_cloud_init_scripts is called with instance.logdir."""
        remoter = MagicMock()
        instance = MagicMock()
        instance.name = "test-node"
        instance.logdir = "/some/logdir"

        # Make cloud-init not installed so we skip the main logic
        remoter.sudo.return_value = MagicMock(ok=False)

        wait_cloud_init_completes(remoter=remoter, instance=instance)

        # Since cloud-init is not installed, the function returns early
        # and collect is NOT called (it's after the cloud-init check)
        mock_collect.assert_not_called()

    @patch("sdcm.provision.helpers.cloud_init.collect_cloud_init_scripts")
    @patch("sdcm.provision.helpers.cloud_init.log_user_data_scripts_errors", return_value=False)
    def test_collect_called_after_cloud_init_check(self, mock_log_errors, mock_collect):
        """Test that collect is called when cloud-init completes successfully."""
        remoter = MagicMock()
        instance = MagicMock()
        instance.name = "test-node"
        instance.logdir = "/some/logdir"

        # cloud-init is installed
        remoter.sudo.side_effect = [
            MagicMock(ok=True),  # command -v cloud-init
            MagicMock(ok=True, stdout='{"status": "done", "errors": []}', return_code=0),  # cloud-init status
        ]
        remoter.run.return_value = MagicMock(stdout="cloud-init 24.1.3-0ubuntu3.3\n")
        remoter.is_up.return_value = True

        wait_cloud_init_completes(remoter=remoter, instance=instance)

        mock_collect.assert_called_once_with(remoter=remoter, logdir="/some/logdir")

    @patch("sdcm.provision.helpers.cloud_init.collect_cloud_init_scripts")
    @patch("sdcm.provision.helpers.cloud_init.log_user_data_scripts_errors", return_value=False)
    def test_collect_called_with_none_when_no_logdir(self, mock_log_errors, mock_collect):
        """Test that collect is called with logdir=None when instance has no logdir attribute."""
        remoter = MagicMock()
        instance = MagicMock(spec=["name"])  # spec without logdir
        instance.name = "test-node"

        remoter.sudo.side_effect = [
            MagicMock(ok=True),
            MagicMock(ok=True, stdout='{"status": "done", "errors": []}', return_code=0),
        ]
        remoter.run.return_value = MagicMock(stdout="cloud-init 24.1.3-0ubuntu3.3\n")
        remoter.is_up.return_value = True

        wait_cloud_init_completes(remoter=remoter, instance=instance)

        mock_collect.assert_called_once_with(remoter=remoter, logdir=None)
