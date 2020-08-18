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
import logging
import tempfile
import contextlib
from io import StringIO

from sdcm import wait


LOGGER = logging.getLogger(__name__)


def read_to_stringio(fobj):
    return StringIO(fobj.read())


@contextlib.contextmanager
def remote_file(remoter, remote_path, serializer=StringIO.getvalue, deserializer=read_to_stringio, sudo=False):
    filename = os.path.basename(remote_path)
    local_tempfile = os.path.join(tempfile.mkdtemp(prefix='sct'), filename)
    wait.wait_for(remoter.receive_files,
                  step=10,
                  text=f"Waiting for copying `{remote_path}' from {remoter.hostname}",
                  timeout=300,
                  throw_exc=True,
                  src=remote_path,
                  dst=local_tempfile)
    with open(local_tempfile, "r") as fobj:
        parsed_data = deserializer(fobj)

    yield parsed_data

    content = serializer(parsed_data)
    with open(local_tempfile, "w") as fobj:
        fobj.write(content)

    LOGGER.debug("New content of `%s':\n%s", remote_path, content)

    remote_tempfile = remoter.run("mktemp").stdout.strip()
    wait.wait_for(remoter.send_files,
                  step=10,
                  text=f"Waiting for updating of `{remote_path}' on {remoter.hostname}",
                  timeout=300,
                  throw_exc=True,
                  src=local_tempfile,
                  dst=remote_tempfile)
    (remoter.sudo if sudo else remoter.run)(f"mv '{remote_tempfile}' '{remote_path}'")
