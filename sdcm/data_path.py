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
# Copyright (c) 2016 ScyllaDB

import os
import sys

_BASE_DIR = os.path.join(sys.modules[__name__].__file__, "..", "..")
_BASE_DIR = os.path.abspath(_BASE_DIR)


def get_data_path(*args):
    return os.path.join(_BASE_DIR, "data_dir", *args)
