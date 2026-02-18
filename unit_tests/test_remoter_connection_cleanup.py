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

"""
Test to verify that remoter connection cleanup prevents id() reuse issues.

This test verifies the fix for a race condition where Python's id() reuse
could cause a new remoter to inherit a stale SSH connection from a previously
garbage-collected remoter. This was causing issues where SSH commands for
node-12 would be executed on node-10 during parallel node creation.

The root cause was:
1. connection_thread_map is a class-level threading.local() that stores
   SSH connections keyed by str(id(self))
2. When a remoter is garbage collected, Python may reuse its id() for new objects
3. Without proper cleanup, the new remoter would find and reuse a stale
   connection from the previous remoter, connected to a different host
"""

from unittest.mock import MagicMock

from sdcm.remote.remote_base import RemoteCmdRunnerBase


class TestRemoterConnectionCleanup:
    """Test suite for remoter connection cleanup functionality."""

    def test_connection_removed_from_thread_map_on_stop(self):
        """Test that connection is removed from thread map when remoter is stopped.

        This prevents id() reuse issues where a new remoter could inherit
        a stale connection from a previously garbage-collected remoter.
        """

        # Create a mock remoter class for testing
        class MockRemoter(RemoteCmdRunnerBase):
            exception_unexpected = Exception
            exception_failure = Exception
            exception_retryable = (Exception,)

            def _create_connection(self):
                mock_conn = MagicMock()
                return mock_conn

            def is_up(self, timeout: float = 30):
                return True

            def _run_on_retryable_exception(self, exc, new_session, suppress_errors=False):
                return False

        # Create first remoter and access its connection to populate thread map
        remoter1 = MockRemoter(hostname="10.0.0.1", user="test", key_file="/tmp/test_key")
        remoter1_id = str(id(remoter1))

        # Access connection to populate thread map
        _ = remoter1.connection

        # Verify connection is in thread map
        assert hasattr(RemoteCmdRunnerBase.connection_thread_map, remoter1_id)

        # Stop the remoter
        remoter1.stop()

        # Verify connection is removed from thread map
        assert not hasattr(RemoteCmdRunnerBase.connection_thread_map, remoter1_id)

    def test_close_connection_does_not_recreate_connection(self):
        """Test that _close_connection doesn't recreate connection if it doesn't exist.

        The old implementation called self.connection which would recreate the
        connection, defeating the purpose of cleanup.
        """

        class MockRemoter(RemoteCmdRunnerBase):
            exception_unexpected = Exception
            exception_failure = Exception
            exception_retryable = (Exception,)
            connection_created = False

            def _create_connection(self):
                self.connection_created = True
                return MagicMock()

            def is_up(self, timeout: float = 30):
                return True

            def _run_on_retryable_exception(self, exc, new_session, suppress_errors=False):
                return False

        remoter = MockRemoter(hostname="10.0.0.2", user="test", key_file="/tmp/test_key")

        # Call _close_connection without ever accessing connection
        remoter._close_connection()

        # Verify that connection was NOT created
        assert not remoter.connection_created

    def test_del_cleans_up_connection(self):
        """Test that __del__ properly cleans up connection from thread map.

        This is critical because __del__ is called when the object is garbage
        collected, which is when id() could be reused.
        """

        class MockRemoter(RemoteCmdRunnerBase):
            exception_unexpected = Exception
            exception_failure = Exception
            exception_retryable = (Exception,)

            def _create_connection(self):
                return MagicMock()

            def is_up(self, timeout: float = 30):
                return True

            def _run_on_retryable_exception(self, exc, new_session, suppress_errors=False):
                return False

        remoter = MockRemoter(hostname="10.0.0.3", user="test", key_file="/tmp/test_key")
        remoter_id = str(id(remoter))

        # Access connection to populate thread map
        _ = remoter.connection
        assert hasattr(RemoteCmdRunnerBase.connection_thread_map, remoter_id)

        # Delete remoter (calls __del__ which calls stop())
        del remoter

        # Verify connection is removed from thread map
        assert not hasattr(RemoteCmdRunnerBase.connection_thread_map, remoter_id)

    def test_new_remoter_does_not_reuse_old_connection_after_cleanup(self):
        """Test that a new remoter creates a fresh connection after old one is cleaned up.

        This simulates the actual bug scenario where node-12's remoter would
        reuse node-10's connection due to id() reuse.
        """

        class MockRemoter(RemoteCmdRunnerBase):
            exception_unexpected = Exception
            exception_failure = Exception
            exception_retryable = (Exception,)

            def __init__(self, hostname, **kwargs):
                super().__init__(hostname=hostname, **kwargs)
                self.my_hostname = hostname

            def _create_connection(self):
                mock_conn = MagicMock()
                mock_conn.hostname = self.hostname
                return mock_conn

            def is_up(self, timeout: float = 30):
                return True

            def _run_on_retryable_exception(self, exc, new_session, suppress_errors=False):
                return False

        # Create and use first remoter (simulating node-10)
        remoter1 = MockRemoter(hostname="10.0.0.10", user="test", key_file="/tmp/test_key")
        conn1 = remoter1.connection
        assert conn1.hostname == "10.0.0.10"

        # Stop and delete first remoter (simulating garbage collection)
        remoter1.stop()
        del remoter1

        # Create second remoter (simulating node-12)
        # In the bug scenario, this could get the same id() as remoter1
        remoter2 = MockRemoter(hostname="10.0.0.12", user="test", key_file="/tmp/test_key")
        conn2 = remoter2.connection

        # Verify the connection is for the correct host
        assert conn2.hostname == "10.0.0.12", (
            f"Expected connection to 10.0.0.12, but got {conn2.hostname}. "
            "This indicates the new remoter reused a stale connection."
        )

        # Cleanup
        remoter2.stop()
