#!/usr/bin/env python

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


def set_stress_command_to_tenant(params, tenant_number: int):
    tenant_stress_cmds = {}
    for stress_cmd_param in params.stress_cmd_params:
        current_stress_cmd = params.get(stress_cmd_param)

        if not isinstance(current_stress_cmd, list):
            continue

        if all((isinstance(current_stress_cmd_element, list)
                for current_stress_cmd_element in current_stress_cmd)):
            tenant_stress_cmds[stress_cmd_param] = current_stress_cmd[tenant_number]
        else:
            tenant_stress_cmds[stress_cmd_param] = current_stress_cmd

    return tenant_stress_cmds
