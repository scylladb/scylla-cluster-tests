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


# Cloud provider agents run their own apt commands at boot and may hold the apt locks for
# minutes (e.g. the oracle-cloud-agent snap runs `apt update` on OCI instances), so wait
# as long as the cloud-init apt configuration does (see sdcm/provision/user_data.py).
APT_LOCK_TIMEOUT = 300

# All the lock files apt/dpkg may hold. `DPkg::Lock::Timeout` covers only the dpkg ones,
# the others have to be waited for explicitly, see apt_lock_wait().
APT_LOCK_FILES = (
    "/var/lib/dpkg/lock",
    "/var/lib/dpkg/lock-frontend",
    "/var/lib/apt/lists/lock",
    "/var/cache/apt/archives/lock",
)

APT_DEFAULTS = {
    "Acquire::http::Timeout": "60",
    "Acquire::Retries": "3",
    "DPkg::Lock::Timeout": str(APT_LOCK_TIMEOUT),
    "Dpkg::Options::": '"--force-confold"',
}

APT_CONFDEF = '-o Dpkg::Options::="--force-confdef"'


def apt_lock_wait(lock_timeout: int = APT_LOCK_TIMEOUT) -> str:
    """Build a shell loop which waits for all the apt/dpkg locks to be released.

    `DPkg::Lock::Timeout` makes apt wait for the dpkg locks only: `apt-get clean` and
    `apt-get update` still fail immediately when the archives/lists locks are taken
    (checked on Ubuntu 24.04), so poll all of them with fuser the same way
    sdcm/utils/rpm.py:rpm_lock_wait() does for the rpm lock.

    Quote free (uses double quotes only), so it can be embedded both into ``bash -c '...'``
    commands and into the generated cloud-init scripts.

    Args:
        lock_timeout: Max seconds to wait for the apt locks (default 300, polls every 5s).
    """
    iterations = max(lock_timeout // 5, 1)
    return (
        f"for i in $(seq 1 {iterations}); do "
        f"fuser {' '.join(APT_LOCK_FILES)} >/dev/null 2>&1 || break; "
        f'echo "apt lock held, waiting... ($i/{iterations})"; sleep 5; done'
    )


def apt_cmd(
    subcommand: str = "",
    options: Optional[dict[str, str]] = None,
    dpkg_options: bool = True,
    lock_wait: bool = False,
) -> str:
    """Build an apt-get command string with safe defaults for non-interactive use.

    Args:
        subcommand: The apt-get subcommand with arguments (e.g. "update", "install -y nginx").
            If empty, returns the apt-get prefix with options (useful as a command prefix).
        options: Override or extend default apt options. Keys are apt option names, values are their values.
        dpkg_options: Include the dpkg config file options (--force-confold/--force-confdef).
            Set to False for subcommands which never invoke dpkg (e.g. "clean").
        lock_wait: Prefix the command with apt_lock_wait(), i.e. wait for the apt locks which
            `DPkg::Lock::Timeout` does not cover. The result gets wrapped into ``bash -c '...'``,
            so do not use it where the result is embedded into another single quoted command.

    Returns:
        Complete apt-get command string ready for remoter.sudo().
    """
    merged = dict(APT_DEFAULTS)
    if not dpkg_options:
        merged.pop("Dpkg::Options::", None)
    if options:
        merged.update(options)

    parts = ["apt-get"]
    for key, value in merged.items():
        parts.append(f"-o {key}={value}")
    if dpkg_options:
        parts.append(APT_CONFDEF)
    if subcommand:
        parts.append(subcommand)

    cmd = " ".join(parts)
    if lock_wait:
        # Escape single quotes in the command portion to prevent shell injection.
        # The wait loop is static (no external input), only the apt options need escaping.
        cmd = cmd.replace("'", r"'\''")
        cmd = f"bash -c '{apt_lock_wait()} && {cmd}'"

    return cmd
