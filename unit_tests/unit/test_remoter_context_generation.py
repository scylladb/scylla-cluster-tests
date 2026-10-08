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

import threading

from sdcm.remote.remote_cmd_runner import RemoteCmdRunner


def connection_in_new_thread(remoter):
    connections = []
    thread = threading.Thread(target=lambda: connections.append(remoter.connection))
    thread.start()
    thread.join()
    return connections[0]


def test_reconnect_in_one_thread_keeps_other_threads_connections_stale():
    """A context change must reconnect every thread's connection, not just the first one to reconnect."""
    # NOTE: a closed local port: its SSH ping thread is refused at once, so `stop()` doesn't wait for a connect timeout
    remoter = RemoteCmdRunner(hostname="127.0.0.1", port=1, user="test", key_file="/tmp/test_key")
    try:
        main_connection = remoter.connection
        setup_connection = connection_in_new_thread(remoter)

        # NOTE: what `run(..., change_context=True)` followed by a reconnect in the setup thread does
        remoter._context_generation += 1
        remoter._bind_generation_to_connection(setup_connection)

        assert remoter._is_connection_generation_ok(setup_connection)
        assert not remoter._is_connection_generation_ok(main_connection)
    finally:
        remoter.stop()
