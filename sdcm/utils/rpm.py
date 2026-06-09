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


def rpm_cmd(pkg_manager: str, subcommand: str, lock_timeout: int = 120) -> str:
    """Build a yum/dnf/microdnf command string with rpm lock tolerance baked in.

    Analogous to sdcm/utils/apt.py:apt_cmd() which uses DPkg::Lock::Timeout.
    Since yum/dnf have no native lock-wait flag, we prefix the command with a
    shell loop that polls fuser on the rpm lock file.

    On OCI, oracle-cloud-agent holds the lock at boot while installing monitoring
    plugins.  Other transient holders: yum-cron, dnf-automatic, PackageKit.

    The returned string is wrapped in ``bash -c '...'`` so that it works correctly
    with both ``remoter.sudo()`` (which prepends ``sudo``) and
    ``remoter.run("sudo ...")``.

    Args:
        pkg_manager: The RPM package manager binary name ("yum", "dnf", or "microdnf").
        subcommand: The subcommand with arguments (e.g. "install -y nginx").
        lock_timeout: Max seconds to wait for rpm lock (default 120, polls every 2s).

    Returns:
        Shell command string ready for remoter.run() / remoter.sudo().
    """
    iterations = lock_timeout // 2
    wait = (
        f"for i in $(seq 1 {iterations}); do "
        "fuser /var/lib/rpm/.rpm.lock &>/dev/null || break; "
        f'echo "rpm lock held, waiting... ($i/{iterations})"; sleep 2; done'
    )

    # Escape single quotes in the command portion to prevent shell injection.
    # The wait loop is static (no external input), only pkg_manager+subcommand need escaping.
    cmd = f"{pkg_manager} {subcommand}".replace("'", r"'\''")
    return f"bash -c '{wait} && {cmd}'"
