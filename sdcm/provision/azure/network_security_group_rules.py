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

from sdcm.provision.security import OpenPorts

template = {
    "name": "",
    "protocol": "TCP",
    "source_port_range": "*",
    "destination_port_range": "",
    "source_address_prefix": "*",
    "destination_address_prefix": "*",
    "access": "Allow",
    "priority": 300,
    "direction": "Inbound",
}

open_ports_rules = []
for idx, port in enumerate(OpenPorts):
    t = template.copy()
    t["name"] = port.name
    t["destination_port_range"] = str(port.value)
    t["priority"] = t["priority"] + idx
    open_ports_rules.append(t)
