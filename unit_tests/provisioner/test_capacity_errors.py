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

import pytest
from botocore.exceptions import ClientError

from sdcm.provision.aws.capacity_errors import is_capacity_error


def _client_error(code: str, message: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": message}}, "RunInstances")


@pytest.mark.parametrize(
    "exc,expected",
    [
        # direct capacity error code
        (_client_error("InsufficientInstanceCapacity", "No capacity in eu-west-1a."), True),
        # wrapped error: real code buried in the message, extracted via "An error occurred (X)" pattern
        (
            _client_error("ClientError", "An error occurred (InsufficientInstanceCapacity) when calling RunInstances"),
            True,
        ),
        # distinct AWS error code sharing a prefix with a capacity code must not match
        (_client_error("UnsupportedOperation", "Operation not supported in this region."), False),
    ],
)
def test_is_capacity_error(exc, expected):
    assert is_capacity_error(exc) is expected


def test_is_capacity_error_false_for_non_client_exception():
    assert is_capacity_error(ValueError("just a value error")) is False
