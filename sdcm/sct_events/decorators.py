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

from functools import wraps
from traceback import format_exc

from sdcm.sct_events.system import ThreadFailedEvent


def raise_event_on_failure(func):
    """Convert any exception to a ThreadFailedEvent."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            ThreadFailedEvent(message=str(exc), traceback=format_exc()).publish()
        return None

    return wrapper


__all__ = ("raise_event_on_failure", )
