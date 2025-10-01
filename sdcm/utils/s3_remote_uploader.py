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
import logging
import socket
from os.path import expanduser
from typing import List

import boto3
from botocore.response import StreamingBody
from ssh2.session import Session

from sdcm.keystore import KeyStore

LOGGER = logging.getLogger(__name__)


class SshOutAsFile(StreamingBody):
    """Wraps channel stream around file-like object for use by s3.upload_fileobj"""

    def __init__(self, channel):
        self._chan = channel

    def read(self, amt=1024):
        received = 0
        buff = b''
        while received < amt:
            size, data = self._chan.read(amt - received)
            received += size
            buff += data
            if size == 0:
                break
        return buff

    def readable(self):
        return True


def upload_remote_files_directly_to_s3(ssh_info: dict[str, str], files: List[str],
                                       s3_bucket: str, s3_key: str, max_size_gb: int = 80, public_read_acl: bool = False):
    """Streams given remote files/directories straight to S3 as tar.gz file. Returns download link."""

    def get_dir_size_kb(session, files):
        channel = session.open_session()
        channel.execute(f"du -csBG {' '.join(files)}")
        channel.wait_eof()
        channel.close()
        channel.wait_closed()
        size, data = channel.read()
        out = b""
        while size > 0:
            out += data
            size, data = channel.read()
        if not out:
            raise FileNotFoundError(f"Could not get the size of {files}. Possibly it does not exist.")
        total = out.split(b"\n")[-2]
        return int(total.split(b'G')[0].decode())

    LOGGER.info("Uploading %s directly to S3 bucket %s with key %s", files, s3_bucket, s3_key)
    extra_args = {}
    if public_read_acl is True:
        extra_args.update({"ACL": "public-read"})
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ssh_info.get("hostname"), 22))
    session = Session()
    session.handshake(sock)
    session.userauth_publickey_fromfile(username=ssh_info.get("user"), privatekey=expanduser(ssh_info.get("key_file")))
    size = get_dir_size_kb(session, files)
    LOGGER.info("Size to upload (before compression): %s", size)
    if size > min(max_size_gb, 80):  # ~80 GB is the maximum size of a single file in S3 with current transport settings
        LOGGER.warning("Skipping upload '%s' directory to S3 due its size: %s GB", files, size)
        return ""
    chan = session.open_session()
    chan.execute(f'tar -czf - {" ".join(files)}')
    chan_response = SshOutAsFile(chan)
    s3 = boto3.client("s3")
    s3.upload_fileobj(chan_response, s3_bucket, s3_key, ExtraArgs=extra_args)
    for user, canonical_id in KeyStore.get_acl_grantees().items():
        LOGGER.info("Setting ACL for user %s", user)
        s3.put_object_acl(Bucket=s3_bucket, Key=s3_key, GrantRead=f"id={canonical_id}")
    link = f"https://{s3_bucket}.s3.amazonaws.com/{s3_key}"
    LOGGER.info("Uploaded successfully to %s", link)
    return link
