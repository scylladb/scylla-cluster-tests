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

"""The DB node instance type that Argus results are recorded under."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from sdcm.cluster import BaseNode

PARAMS = {"instance_type_db": "i7i.large", "zero_token_instance_type_db": "c6i.large"}


def db_node_instance_type(launched_type, is_zero_token_node=False, backend="aws"):
    node = SimpleNamespace(
        parent_cluster=SimpleNamespace(cluster_backend=backend, params=PARAMS),
        _instance_type=launched_type,
        _is_zero_token_node=is_zero_token_node,
        log=MagicMock(),
    )
    return BaseNode.db_node_instance_type.fget(node)


def test_aws_reports_the_type_ec2_actually_launched():
    """EC2 Fleet may launch one of `aws_instance_type_db_alternatives` instead of instance_type_db."""
    assert db_node_instance_type("i3en.large") == "i3en.large"


@pytest.mark.parametrize(
    "is_zero_token_node, expected",
    [pytest.param(False, "i7i.large", id="db_node"), pytest.param(True, "c6i.large", id="zero_token_node")],
)
def test_aws_falls_back_to_the_configured_type_when_unknown(is_zero_token_node, expected):
    assert db_node_instance_type("N/A", is_zero_token_node=is_zero_token_node) == expected
