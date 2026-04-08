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
        test_config_instance.init_argus_client.assert_called_once_with(mock_params)

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
