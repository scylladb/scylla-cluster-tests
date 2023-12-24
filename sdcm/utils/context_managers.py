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


@contextmanager
def run_nemesis(node: 'BaseNode', nemesis_name: str):
    node.running_nemesis = nemesis_name
    try:
        yield node
    finally:
        node.running_nemesis = None
