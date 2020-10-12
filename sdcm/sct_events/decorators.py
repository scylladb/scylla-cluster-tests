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

import os
from functools import wraps
from traceback import format_exc

from sdcm.sct_events.system import ThreadFailedEvent


def raise_event_on_failure(func):
    """
    Decorate a function that is running inside a thread,
    when exception is raised in this function,
    will raise an Error severity event
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = None
        _test_pid = os.getpid()
        try:
            result = func(*args, **kwargs)
        except Exception as ex:  # pylint: disable=broad-except
            ThreadFailedEvent(message=str(ex), traceback=format_exc())

        return result
    return wrapper


__all__ = ("raise_event_on_failure", )
