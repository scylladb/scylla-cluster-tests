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

import enum
import json


# monkey patch JSONEncoder make enums jsonable
_SAVED_DEFAULT = json.JSONEncoder().default  # Save default method.


def _new_default(self, obj):  # pylint: disable=unused-argument
    if isinstance(obj, enum.Enum):
        return obj.name  # Could also be obj.value
    else:
        return _SAVED_DEFAULT


json.JSONEncoder.default = _new_default  # Set new default method.


__all__ = ()
