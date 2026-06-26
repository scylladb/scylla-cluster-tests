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
import stat
import logging
import tempfile
import contextlib
from pathlib import Path
from io import StringIO

import yaml

from sdcm import wait
from sdcm.remote import shell_script_cmd


LOGGER = logging.getLogger(__name__)


def read_to_stringio(fobj):
    return StringIO(fobj.read())


@contextlib.contextmanager
def remote_file(
    remoter,
    remote_path: str | Path,
    serializer=StringIO.getvalue,
    deserializer=read_to_stringio,
    sudo=False,
    preserve_ownership=True,
    preserve_permissions=True,
    log_change=True,
):
    filename = os.path.basename(remote_path)
    local_tempfile = os.path.join(tempfile.mkdtemp(prefix="sct"), filename)
    if preserve_ownership and sudo:
        ownership = remoter.sudo(cmd=f'stat -c "%U:%G" {remote_path}').stdout.strip()
    if preserve_permissions and sudo:
        permissions = remoter.sudo(cmd=f'stat -c "%a" {remote_path}').stdout.strip()

    wait.wait_for(
        remoter.receive_files,
        step=10,
        text=f"Waiting for copying `{remote_path}' from {remoter.hostname}",
        timeout=300,
        throw_exc=True,
        src=str(remote_path),
        dst=local_tempfile,
        sudo=sudo,
    )
    with open(local_tempfile, encoding="utf-8") as fobj:
        parsed_data = deserializer(fobj)
        original_content = serializer(parsed_data)
    yield parsed_data

    content = serializer(parsed_data)

    if original_content == content:
        LOGGER.debug("Content of '%s' wasn't changed", remote_path)
    else:
        with open(local_tempfile, "w", encoding="utf-8") as fobj:
            fobj.write(content)

        if log_change:
            LOGGER.debug("New content of `%s':\n%s", remote_path, content)

        remote_tempfile = remoter.run("mktemp").stdout.strip()
        if not remote_tempfile:
            raise RuntimeError(
                f"'mktemp' on {remoter.hostname} returned an empty path; "
                f"cannot update '{remote_path}' without a valid remote temporary file"
            )
        remote_tempfile_move_cmd = shell_script_cmd(f"""\
            cat '{remote_tempfile}' > '{remote_path}'
            rm '{remote_tempfile}'
            """)
        wait.wait_for(
            remoter.send_files,
            step=10,
            text=f"Waiting for updating of `{remote_path}' on {remoter.hostname}",
            timeout=300,
            throw_exc=True,
            src=local_tempfile,
            dst=remote_tempfile,
            sudo=sudo,
        )
        if sudo:
            remoter.sudo(remote_tempfile_move_cmd)
        else:
            remoter.run(remote_tempfile_move_cmd)

        if preserve_ownership and sudo:
            remoter.sudo(f"chown {ownership} {remote_path}")
        if preserve_permissions and sudo:
            remoter.sudo(f"chmod {permissions} {remote_path}")

    os.unlink(local_tempfile)


def install_secret_file(remoter, content: str, remote_path: str) -> None:
    """Install `content` as a root-owned 0600 file at `remote_path`.

    The content goes through a local 0600 temp file and a file transfer, so it never
    appears on a command line, where it would be logged.
    """
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
        try:
            os.chmod(tmp.name, stat.S_IRUSR | stat.S_IWUSR)
            tmp.write(content)
            tmp.flush()
            tmp_remote = f"/tmp/{os.path.basename(tmp.name)}"
            remoter.send_files(src=tmp.name, dst=tmp_remote)
        finally:
            os.unlink(tmp.name)
    remoter.sudo(
        shell_script_cmd(f"""\
        mkdir -p {os.path.dirname(remote_path)}
        install -m 0600 -o root -g root {tmp_remote} {remote_path}
        rm -f {tmp_remote}
    """)
    )


def yaml_file_to_dict(fobj):
    return yaml.safe_load(fobj) or {}


def dict_to_yaml_file(data):
    return yaml.safe_dump(data) if data else ""
