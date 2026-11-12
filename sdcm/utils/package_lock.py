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


def lock_holders(lock_files: str) -> str:
    """Shell snippet which prints the processes holding any of ``lock_files`` (pid, ppid, age, command).

    Never fails, so it is safe under ``bash -e``.
    """
    return (
        f'pids=$(fuser {lock_files} 2>/dev/null | xargs | tr " " ,); '
        f'[ -n "$pids" ] && ps -o pid,ppid,etimes,args -p "$pids" || true'
    )


def lock_wait(
    name: str, lock_files: str, lock_timeout: int, poll: int, kill_stale: bool = False, repair: str = ""
) -> str:
    """Build a shell snippet which waits for a package manager lock to be released.

    Logs who holds the lock when it first finds it taken and when the wait runs out, so a
    failure shows whether the holder is a cloud agent, a system timer or an orphaned SCT command.

    Quote free (uses double quotes only) and safe under ``bash -e``, so it can be embedded both
    into ``bash -c '...'`` commands and into the generated cloud-init scripts.

    Args:
        name: Lock name for the log lines (e.g. "apt").
        lock_files: Space separated lock files to poll with fuser.
        lock_timeout: Max seconds to wait for the lock.
        poll: Seconds between the polls.
        kill_stale: When the wait runs out, kill the lock holders (TERM, then KILL) and run ``repair``.
            Only for callers which know the holder can be their own orphaned command, e.g. a
            retry after an SSH timeout left the previous attempt running on the node.
        repair: Command run after killing the holders (e.g. ``dpkg --configure -a``).
    """
    iterations = max(lock_timeout // poll, 1)
    holders = lock_holders(lock_files)
    snippet = (
        f"for i in $(seq 1 {iterations}); do "
        f"fuser {lock_files} >/dev/null 2>&1 || break; "
        f'if [ "$i" = 1 ]; then echo "{name} lock held by:"; {holders}; fi; '
        f'echo "{name} lock held, waiting... ($i/{iterations})"; sleep {poll}; done; '
        f"if fuser {lock_files} >/dev/null 2>&1; then "
        f'echo "{name} lock still held after {lock_timeout}s by:"; {holders}; '
    )
    if kill_stale:
        snippet += (
            f'echo "killing the stale {name} lock holders"; '
            f"fuser -k -TERM {lock_files} >/dev/null 2>&1 || true; sleep 5; "
            f"fuser -k -KILL {lock_files} >/dev/null 2>&1 || true; "
        )
        if repair:
            snippet += f"{repair} || true; "
    return snippet + "fi"
