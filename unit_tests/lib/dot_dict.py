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


class DotDict(dict):
    """A dict that also supports attribute access, standing in for SCTConfiguration in tests.

    Provides both item access (``d["key"] = ...``) and attribute access (``d.key``), which the
    provisioning code relies on (e.g. ``params.get(...)`` alongside ``params.gce_datacenters``).
    """

    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
