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


class KeyBasedLock:
    """Class designed for creating locks based on hashable keys."""

    def __init__(self):
        self.key_lock_mapping = {}
        self.handler_lock = threading.Lock()

    def get_lock(self, hashable_key):
        with self.handler_lock:
            if hashable_key not in self.key_lock_mapping:
                self.key_lock_mapping[hashable_key] = threading.Lock()
            return self.key_lock_mapping[hashable_key]
