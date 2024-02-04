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
# Copyright (c) 2022 ScyllaDB
from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from sdcm.sct_events.base import SctEvent

if TYPE_CHECKING:
    from sdcm.tester import ClusterTester


# pylint: disable=too-few-public-methods
class EventHandler(abc.ABC):

    def __init__(self):
        """Initializes event handler"""

    def handle(self, event: SctEvent, tester_obj: ClusterTester):
        """Handler main function"""
        raise NotImplementedError()
