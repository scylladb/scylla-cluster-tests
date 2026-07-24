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

"""Unit test to verify provision_resources sends error events to Argus."""

import os
from unittest.mock import MagicMock, patch
from click.testing import CliRunner


def test_provision_resources_sends_error_event_to_argus():
    """Test that provision_resources sends error event to Argus on failure."""

    # Mock the necessary components
    with (
        patch("sct.init_and_verify_sct_config") as mock_config,
        patch("sct.get_test_config") as mock_test_config,
        patch("sct.LocalHost"),
        patch("sct.SCTProvisionLayout") as mock_layout,
        patch("sct.add_file_logger"),
    ):
        # Setup mocks
        mock_params = MagicMock()
        mock_params.get.side_effect = lambda key, default=None: {
            "user_prefix": "test",
            "test_id": "test-id-12345",
            "logs_transport": "none",
            "agent": {"enabled": False},
        }.get(key, default)
        mock_config.return_value = mock_params

        test_config_instance = MagicMock()
        test_config_instance.test_id.return_value = "test-id-12345"
        mock_argus_client = MagicMock()
        test_config_instance.argus_client.return_value = mock_argus_client
        mock_test_config.return_value = test_config_instance

        # Make provision fail with an exception
        provision_error = Exception("InsufficientInstanceCapacity: Not enough instances available")
        mock_layout.return_value.provision.side_effect = provision_error

        # Set environment variables
        os.environ["SCT_CLUSTER_BACKEND"] = "aws"

        # Use Click's CliRunner to invoke the command
        from sct import provision_resources  # noqa: PLC0415

        runner = CliRunner()
        result = runner.invoke(provision_resources, ["--backend", "aws", "--test-name", "test"])

        # Verify command exited with error code 1
        assert result.exit_code == 1

        # Verify argus_client was initialized
        test_config_instance.init_argus_client.assert_called_once()

        # Verify submit_event was called
        assert mock_argus_client.submit_event.called

        # Get the event payload that was submitted
        call_args = mock_argus_client.submit_event.call_args
        event_payload = call_args[0][0] if call_args[0] else call_args[1].get("event_payload")

        # Verify the event payload structure
        assert event_payload is not None
        assert event_payload["run_id"] == "test-id-12345"
        assert event_payload["severity"] == "CRITICAL"
        assert event_payload["event_type"] == "TestFrameworkEvent"
        assert "Failed to provision aws resources" in event_payload["message"]

        # Verify TEST_ERROR status was set
        mock_argus_client.set_sct_run_status.assert_called_once()

        # Verify events summary was submitted so the error appears in Argus events tab
        mock_argus_client.submit_events.assert_called_once()
        events_summary = mock_argus_client.submit_events.call_args[0][0]
        assert len(events_summary) == 1
        assert events_summary[0].severity == "CRITICAL"
        assert events_summary[0].total_events == 1
        assert "Failed to provision aws resources" in events_summary[0].messages[0]

        # Verify the run was finalized
        mock_argus_client.finalize_sct_run.assert_called_once()


def test_provision_resources_reports_config_init_failure_to_argus():
    """Test that provision_resources reports to Argus when init_and_verify_sct_config fails.

    This covers the scenario where the unified package download fails with 404,
    raising a FileNotFoundError before provisioning even begins.
    When params is None, get_argus_client is used directly to bypass the
    enable_argus check that would silently swallow the error with MagicMock.
    """

    with (
        patch("sct.init_and_verify_sct_config") as mock_config,
        patch("sct.get_test_config") as mock_test_config,
        patch("sct.get_argus_client") as mock_get_argus_client,
        patch("sct.add_file_logger"),
        patch.dict(os.environ, {"SCT_TEST_ID": "test-id-67890"}),
    ):
        # init_and_verify_sct_config raises FileNotFoundError (unified package 404)
        config_error = FileNotFoundError(
            "Unified package not found or failed to download: "
            "https://downloads.scylladb.com/unstable/scylla/master/relocatable/latest/"
            "scylla-unified-2026.2.0~dev-0.20260330.x86_64.tar.gz. "
            "The URL may not exist or is not accessible."
        )
        mock_config.side_effect = config_error

        test_config_instance = MagicMock()
        test_config_instance.test_id.return_value = "test-id-67890"
        mock_test_config.return_value = test_config_instance

        # Mock get_argus_client to return a real mock client
        mock_argus_client = MagicMock()
        mock_get_argus_client.return_value = mock_argus_client

        from sct import provision_resources  # noqa: PLC0415

        runner = CliRunner()
        result = runner.invoke(provision_resources, ["--backend", "aws", "--test-name", "test"])

        # Verify command exited with error code 1
        assert result.exit_code == 1

        # Verify get_argus_client was called directly with test_id (not via test_config.init_argus_client)
        mock_get_argus_client.assert_called_once_with(run_id="test-id-67890")

        # Verify test_config.init_argus_client was NOT called (params is None)
        test_config_instance.init_argus_client.assert_not_called()

        # Verify test_id was set from environment variable
        test_config_instance.set_test_id_only.assert_called_with("test-id-67890")

        # Verify submit_event was called on the directly-created client
        assert mock_argus_client.submit_event.called

        # Get the event payload that was submitted
        call_args = mock_argus_client.submit_event.call_args
        event_payload = call_args[0][0] if call_args[0] else call_args[1].get("event_payload")

        # Verify the event payload contains FileNotFoundError details
        assert event_payload is not None
        assert event_payload["run_id"] == "test-id-67890"
        assert event_payload["severity"] == "CRITICAL"
        assert event_payload["event_type"] == "TestFrameworkEvent"
        assert "FileNotFoundError" in event_payload["message"]
        assert "Failed to provision aws resources" in event_payload["message"]

        # Verify TEST_ERROR status was set
        mock_argus_client.set_sct_run_status.assert_called_once()

        # Verify events summary was submitted so the error appears in Argus events tab
        mock_argus_client.submit_events.assert_called_once()
        events_summary = mock_argus_client.submit_events.call_args[0][0]
        assert len(events_summary) == 1
        assert events_summary[0].severity == "CRITICAL"
        assert events_summary[0].total_events == 1
        assert "FileNotFoundError" in events_summary[0].messages[0]

        # Verify the run was finalized
        mock_argus_client.finalize_sct_run.assert_called_once()


def test_provision_resources_no_argus_report_without_test_id():
    """Test that Argus reporting is skipped when no test_id is available."""

    with (
        patch("sct.init_and_verify_sct_config") as mock_config,
        patch("sct.get_test_config") as mock_test_config,
        patch("sct.add_file_logger"),
        patch.dict(os.environ, {}, clear=False),
    ):
        # Ensure SCT_TEST_ID is not set
        os.environ.pop("SCT_TEST_ID", None)

        # init_and_verify_sct_config raises an error
        mock_config.side_effect = FileNotFoundError("Package not found")

        test_config_instance = MagicMock()
        mock_argus_client = MagicMock()
        test_config_instance.argus_client.return_value = mock_argus_client
        mock_test_config.return_value = test_config_instance

        from sct import provision_resources  # noqa: PLC0415

        runner = CliRunner()
        result = runner.invoke(provision_resources, ["--backend", "aws", "--test-name", "test"])

        # Verify command exited with error code 1
        assert result.exit_code == 1

        # Verify argus_client was NOT initialized (no test_id available)
        test_config_instance.init_argus_client.assert_not_called()

        # Verify submit_event was NOT called
        assert not mock_argus_client.submit_event.called
