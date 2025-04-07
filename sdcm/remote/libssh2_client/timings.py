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

from typing import Iterable, Optional
from dataclasses import dataclass, field


NullableTiming = Optional[float]
Timing = float
Delays = Iterable[float]


@dataclass
class Timings:
    """A store for timeouts and delays
    """
    keepalive_timeout: Timing = 30
    keepalive_sending_timeout: Timing = 1
    socket_timeout: NullableTiming = 10
    connect_timeout: NullableTiming = 60
    connect_delays: Delays = field(default_factory=lambda: [0, 0.1, 0.5, 1, 15])
    interactive_read_data_chunk_timeout: NullableTiming = 0.5
    read_data_chunk_timeout: NullableTiming = 1
    read_command_output_timeout: NullableTiming = None
    check_if_alive_timeout: NullableTiming = 5
    # Timeout on all basic ssh session operation like authentications, open_session, etc...
    ssh_session_timeout: NullableTiming = 10
    ssh_handshake_timeout: NullableTiming = 3
    auth_timeout: NullableTiming = 10
    open_channel_timeout: NullableTiming = 15
    channel_close_timeout: NullableTiming = 0.5
    close_channel_timeout: NullableTiming = 1
    # Delays for open_channel, if open_channel failed it sleeps for amount of seconds it got from the list
    open_channel_delays: Delays = field(default_factory=lambda: [0, 0.1, 0.5, 1, 15])
