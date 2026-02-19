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

from concurrent.futures import TimeoutError
from unittest.mock import MagicMock, patch
import pytest

from sdcm.utils.gce_utils import wait_for_extended_operation


class TestWaitForExtendedOperation:
    """Test suite for wait_for_extended_operation function."""

    def test_wait_for_extended_operation_success(self):
        """Test that successful operation returns result."""
        # Arrange
        mock_operation = MagicMock()
        mock_operation.result.return_value = "success_result"
        mock_operation.error_code = None
        mock_operation.warnings = []

        # Act
        result = wait_for_extended_operation(mock_operation, "test operation")

        # Assert
        assert result == "success_result"
        mock_operation.result.assert_called_once_with(timeout=300)

    def test_wait_for_extended_operation_with_custom_timeout(self):
        """Test that custom timeout is passed correctly."""
        # Arrange
        mock_operation = MagicMock()
        mock_operation.result.return_value = "success"
        mock_operation.error_code = None
        mock_operation.warnings = []

        # Act
        wait_for_extended_operation(mock_operation, "test operation", timeout=600)

        # Assert
        mock_operation.result.assert_called_once_with(timeout=600)

    def test_wait_for_extended_operation_timeout_with_basic_info(self):
        """Test that timeout includes operation details in error message."""
        # Arrange
        mock_operation = MagicMock()
        mock_operation.result.side_effect = TimeoutError("Original timeout")
        mock_operation.name = "operation-12345"

        # Act & Assert
        with pytest.raises(TimeoutError) as exc_info:
            wait_for_extended_operation(mock_operation, "instance creation", timeout=300)

        # Verify error message contains operation details
        error_message = str(exc_info.value)
        assert "instance creation timed out" in error_message
        assert "Operation timed out after 300 seconds" in error_message
        assert "Operation ID: operation-12345" in error_message

    def test_wait_for_extended_operation_timeout_with_error_info(self):
        """Test that timeout includes error code and message when available."""
        # Arrange
        mock_operation = MagicMock()
        mock_operation.result.side_effect = TimeoutError("Original timeout")
        mock_operation.name = "operation-67890"
        mock_operation.error_code = "QUOTA_EXCEEDED"
        mock_operation.error_message = "Quota 'CPUS' exceeded. Limit: 24.0"

        # Act & Assert
        with pytest.raises(TimeoutError) as exc_info:
            wait_for_extended_operation(mock_operation, "instance creation", timeout=300)

        # Verify error message contains error details
        error_message = str(exc_info.value)
        assert "Error Code: QUOTA_EXCEEDED" in error_message
        assert "Error Message: Quota 'CPUS' exceeded. Limit: 24.0" in error_message

    def test_wait_for_extended_operation_timeout_with_status(self):
        """Test that timeout includes status when available."""
        # Arrange
        mock_operation = MagicMock()
        mock_operation.result.side_effect = TimeoutError("Original timeout")
        mock_operation.name = "operation-status-test"
        mock_operation.status = "PENDING"

        # Act & Assert
        with pytest.raises(TimeoutError) as exc_info:
            wait_for_extended_operation(mock_operation, "instance creation", timeout=300)

        # Verify error message contains status
        error_message = str(exc_info.value)
        assert "Status: PENDING" in error_message

    def test_wait_for_extended_operation_timeout_with_target_link(self):
        """Test that timeout includes target link when available."""
        # Arrange
        mock_operation = MagicMock()
        mock_operation.result.side_effect = TimeoutError("Original timeout")
        mock_operation.name = "operation-target-test"
        mock_operation.target_link = (
            "https://www.googleapis.com/compute/v1/projects/test-project/zones/us-east1-b/instances/test-instance"
        )

        # Act & Assert
        with pytest.raises(TimeoutError) as exc_info:
            wait_for_extended_operation(mock_operation, "instance creation", timeout=300)

        # Verify error message contains target link
        error_message = str(exc_info.value)
        assert (
            "Target: https://www.googleapis.com/compute/v1/projects/test-project/zones/us-east1-b/instances/test-instance"
            in error_message
        )

    def test_wait_for_extended_operation_timeout_with_operation_type(self):
        """Test that timeout includes operation type when available."""
        # Arrange
        mock_operation = MagicMock()
        mock_operation.result.side_effect = TimeoutError("Original timeout")
        mock_operation.name = "operation-type-test"
        mock_operation.operation_type = "insert"

        # Act & Assert
        with pytest.raises(TimeoutError) as exc_info:
            wait_for_extended_operation(mock_operation, "instance creation", timeout=300)

        # Verify error message contains operation type
        error_message = str(exc_info.value)
        assert "Operation Type: insert" in error_message

    @patch("sdcm.utils.gce_utils.LOGGER")
    def test_wait_for_extended_operation_timeout_logs_details(self, mock_logger):
        """Test that timeout logs detailed information."""
        # Arrange
        mock_operation = MagicMock()
        mock_operation.result.side_effect = TimeoutError("Original timeout")
        mock_operation.name = "operation-log-test"
        mock_operation.error_code = "RESOURCE_NOT_READY"
        mock_operation.error_message = "Resource is not ready"

        # Act
        with pytest.raises(TimeoutError):
            wait_for_extended_operation(mock_operation, "instance creation", timeout=300)

        # Assert - verify logging was called
        assert mock_logger.error.called
        log_message = mock_logger.error.call_args[0][1]
        assert "Operation timed out after 300 seconds" in log_message
        assert "Operation ID: operation-log-test" in log_message
        assert "Error Code: RESOURCE_NOT_READY" in log_message

    def test_wait_for_extended_operation_error_code_handling(self):
        """Test that error_code is properly handled when operation completes with error."""
        # Arrange
        mock_operation = MagicMock()
        mock_operation.result.return_value = None
        mock_operation.error_code = "RESOURCE_NOT_FOUND"
        mock_operation.error_message = "The resource was not found"
        mock_operation.name = "operation-error"
        mock_operation.exception.return_value = RuntimeError("Resource not found")

        # Act & Assert
        with pytest.raises(RuntimeError) as exc_info:
            wait_for_extended_operation(mock_operation, "instance creation")

        assert "Resource not found" in str(exc_info.value)

    def test_wait_for_extended_operation_with_warnings(self):
        """Test that warnings are logged properly."""
        # Arrange
        mock_operation = MagicMock()
        mock_operation.result.return_value = "success"
        mock_operation.error_code = None
        mock_warning1 = MagicMock()
        mock_warning1.code = "WARNING_CODE_1"
        mock_warning1.message = "This is a warning"
        mock_operation.warnings = [mock_warning1]

        # Act
        result = wait_for_extended_operation(mock_operation, "test operation")

        # Assert
        assert result == "success"
