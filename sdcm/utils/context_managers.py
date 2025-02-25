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
# Copyright (c) 2023 ScyllaDB

import os
import time
from contextlib import contextmanager


@contextmanager
def environment(**kwargs):
    original_environment = dict(os.environ)
    os.environ.update(kwargs)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(original_environment)


@contextmanager
def nodetool_context(node, start_command, end_command):
    """
    To be used when a nodetool command can affect the state of the node,
    like disablebinary/disablegossip, and it is needed to keep the node in said
    state temporarily.

    :param node: the db node where the nodetool command are to be executed on
    :param start_command: the command to execute before yielding the context
    :param end_command: the command to execute as the closing step
    :return:
    """
    try:
        result = node.run_nodetool(start_command)
        yield result
    finally:
        node.run_nodetool(end_command)


class DbNodeLogger:
    """A context manager for logging the start, end and duration of the given operation in system log of database nodes.

    Example usage:
        with DbNodeLogger([node1, node2], "create index", target_node=self.target_node,
                          additional_info=f"on {ks}.{cf}.{column}"):
            index_name = create_index(session, ks, cf, column)
    """

    def __init__(self, nodes, operation_name, target_node=None, additional_info=None):
        """Initialize the logger with the operation details.

        :param nodes (list): list of nodes to log messages to
        :param operation_name (str): name of the operation being performed
        :param target_node (BaseNode, optional): the node the operation is being performed on
        :param additional_info (str, optional): additional information about the operation
        """
        self.nodes = nodes
        self.operation_name = operation_name
        self.target_node = target_node
        self.additional_info = additional_info
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        message = f"executing {self.operation_name}"
        if self.target_node:
            message += f" on {self.target_node.name} [{self.target_node.private_ip_address}]"
        if self.additional_info:
            message += f" - {self.additional_info}"

        for node in self.nodes:
            node.log_message(message)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        status = "failed" if exc_type else "completed"
        message = f"{self.operation_name} {status} after {duration:.2f}s"
        if self.target_node:
            message += f" on {self.target_node.name} [{self.target_node.private_ip_address}]"
        if exc_val:
            message += f" - Error: {exc_val}"

        for node in self.nodes:
            node.log_message(message)
        return False
