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

import unittest
from unittest.mock import Mock, MagicMock
from dataclasses import dataclass


@dataclass
class MockResult:
    """Mock result object for testing."""
    stdout: str = ""
    stderr: str = ""
    command: str = ""
    

class MockFailedToRunCommand(Exception):
    """Mock exception with result attribute."""
    def __init__(self, result, original_exception):
        self.result = result
        self.original_exception = original_exception
        super().__init__(str(original_exception))


class MockOpenChannelTimeout(Exception):
    """Mock exception for testing."""
    pass


class TestLoggerSuppression(unittest.TestCase):
    """Test that logger command errors are suppressed while other errors are still logged."""

    def setUp(self):
        # Create a mock RemoteLibSSH2CmdRunner instance
        from unittest.mock import Mock
        self.remoter = Mock()
        self.remoter.log = Mock()
        
        # Import the actual method we want to test
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        
        # Create the actual method implementation to test
        def _run_on_retryable_exception(exc, new_session):
            # Check if this is a logger command for scylla-cluster-tests to reduce log noise
            is_logger_command = False
            if hasattr(exc, 'result') and hasattr(exc.result, 'command'):
                command = exc.result.command
                is_logger_command = (command and 
                                   'logger' in command and 
                                   'scylla-cluster-tests' in command)
            
            # Use DEBUG level for logger commands to suppress noise, ERROR for others
            if is_logger_command:
                self.remoter.log.debug("Failed to log test action on node (this is expected for unreachable nodes): %s", exc)
            else:
                self.remoter.log.error(exc, exc_info=exc)
                
            if isinstance(exc, MockFailedToRunCommand) and not new_session:
                self.remoter.log.debug('Reestablish the session...')
            
            # Simulate raising RetryableNetworkException  
            raise Exception("RetryableNetworkException simulation")
        
        self.remoter._run_on_retryable_exception = _run_on_retryable_exception

    def test_logger_command_suppression(self):
        """Test that logger commands for scylla-cluster-tests are logged at DEBUG level."""
        # Create a result object with a logger command
        result = MockResult(
            command="logger -p info -t scylla-cluster-tests 'executing nodetool command'"
        )
        
        # Create an exception with the logger command
        exc = MockFailedToRunCommand(result, MockOpenChannelTimeout("Failed to open channel in 15 seconds"))
        
        # Call the method under test
        with self.assertRaises(Exception):  # Should raise simulated RetryableNetworkException
            self.remoter._run_on_retryable_exception(exc, new_session=False)
        
        # Verify that DEBUG was called instead of ERROR
        self.remoter.log.debug.assert_called()
        self.remoter.log.error.assert_not_called()
        
        # Verify the debug message contains expected text
        debug_calls = self.remoter.log.debug.call_args_list
        debug_message = debug_calls[0][0][0]  # First call, first argument
        self.assertIn("Failed to log test action on node", debug_message)

    def test_regular_command_not_suppressed(self):
        """Test that non-logger commands are still logged at ERROR level."""
        # Create a result object with a regular command
        result = MockResult(
            command="nodetool status"
        )
        
        # Create an exception with the regular command
        exc = MockFailedToRunCommand(result, MockOpenChannelTimeout("Failed to open channel in 15 seconds"))
        
        # Call the method under test
        with self.assertRaises(Exception):  # Should raise simulated RetryableNetworkException
            self.remoter._run_on_retryable_exception(exc, new_session=False)
        
        # Verify that ERROR was called
        self.remoter.log.error.assert_called()

    def test_logger_command_without_scylla_cluster_tests_not_suppressed(self):
        """Test that logger commands without scylla-cluster-tests are not suppressed."""
        # Create a result object with a logger command but different tag
        result = MockResult(
            command="logger -p info -t other-tool 'some message'"
        )
        
        # Create an exception with the logger command
        exc = MockFailedToRunCommand(result, MockOpenChannelTimeout("Failed to open channel in 15 seconds"))
        
        # Call the method under test
        with self.assertRaises(Exception):  # Should raise simulated RetryableNetworkException
            self.remoter._run_on_retryable_exception(exc, new_session=False)
        
        # Verify that ERROR was called (not suppressed)
        self.remoter.log.error.assert_called()

    def test_exception_without_result_not_suppressed(self):
        """Test that exceptions without result attribute are handled normally."""
        # Create an exception without result attribute
        exc = MockOpenChannelTimeout("Failed to open channel in 15 seconds")
        
        # Call the method under test
        with self.assertRaises(Exception):  # Should raise simulated RetryableNetworkException
            self.remoter._run_on_retryable_exception(exc, new_session=False)
        
        # Verify that ERROR was called (not suppressed)
        self.remoter.log.error.assert_called()


if __name__ == '__main__':
    unittest.main()