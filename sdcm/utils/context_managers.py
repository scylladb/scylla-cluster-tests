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
