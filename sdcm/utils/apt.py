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
# Copyright (c) 2025 ScyllaDB

from typing import Optional


APT_DEFAULTS = {
    "Acquire::http::Timeout": "60",
    "Acquire::Retries": "3",
    "DPkg::Lock::Timeout": "120",
    "Dpkg::Options::": '"--force-confold"',
}

APT_CONFDEF = '-o Dpkg::Options::="--force-confdef"'


def apt_cmd(subcommand: str = "", options: Optional[dict[str, str]] = None) -> str:
    """Build an apt-get command string with safe defaults for non-interactive use.

    Args:
        subcommand: The apt-get subcommand with arguments (e.g. "update", "install -y nginx").
            If empty, returns the apt-get prefix with options (useful as a command prefix).
        options: Override or extend default apt options. Keys are apt option names, values are their values.

    Returns:
        Complete apt-get command string ready for remoter.sudo().
    """
    merged = dict(APT_DEFAULTS)
    if options:
        merged.update(options)

    parts = ["apt-get"]
    for key, value in merged.items():
        parts.append(f"-o {key}={value}")
    parts.append(APT_CONFDEF)
    if subcommand:
        parts.append(subcommand)

    return " ".join(parts)
