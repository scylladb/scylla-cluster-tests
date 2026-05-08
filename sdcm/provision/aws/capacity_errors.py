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

import re

from botocore.exceptions import ClientError

CAPACITY_ERROR_CODES: list[str] = [
    "InsufficientInstanceCapacity",
    "Unsupported",
    "InsufficientCapacity",
]

_AWS_ERROR_CODE_RE = re.compile(r"An error occurred \(([A-Za-z]+)\)")


def is_capacity_error(exception: BaseException) -> bool:
    """Return True if `exception` is an AWS capacity-shortage `ClientError`."""
    if not isinstance(exception, ClientError):
        return False
    error_code = exception.response.get("Error", {}).get("Code", "")
    codes = {error_code} | set(_AWS_ERROR_CODE_RE.findall(str(exception)))
    return bool(codes & set(CAPACITY_ERROR_CODES))
