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
# Copyright (c) 2021 ScyllaDB

from textwrap import dedent

from sdcm.remote import shell_script_cmd
from sdcm.utils.common import get_data_dir_path

CLIENT_KEYFILE = get_data_dir_path("ssl_conf", "client/test.key")
CLIENT_CERTFILE = get_data_dir_path("ssl_conf", "client/test.crt")
CLIENT_TRUSTSTORE = get_data_dir_path("ssl_conf", "client/catest.pem")


def install_client_certificate(remoter):
    if remoter.run("ls /etc/scylla/ssl_conf", ignore_status=True).ok:
        return
    remoter.send_files(src=get_data_dir_path("ssl_conf"), dst="/tmp/")  # pylint: disable=not-callable
    setup_script = dedent("""
        mkdir -p ~/.cassandra/
        cp /tmp/ssl_conf/client/cqlshrc ~/.cassandra/
        sudo mkdir -p /etc/scylla/
        sudo rm -rf /etc/scylla/ssl_conf/
        sudo mv -f /tmp/ssl_conf/ /etc/scylla/
    """)
    remoter.run('bash -cxe "%s"' % setup_script)


def install_encryption_at_rest_files(remoter):
    if remoter.sudo("ls /etc/encrypt_conf/system_key_dir", ignore_status=True).ok:
        return
    remoter.send_files(src=get_data_dir_path("encrypt_conf"), dst="/tmp/")
    remoter.sudo(
        shell_script_cmd(
            dedent("""
        rm -rf /etc/encrypt_conf
        mv -f /tmp/encrypt_conf /etc
        mkdir -p /etc/scylla/encrypt_conf /etc/encrypt_conf/system_key_dir
        chown -R scylla:scylla /etc/scylla /etc/encrypt_conf
    """)
        )
    )
    remoter.sudo("md5sum /etc/encrypt_conf/*.pem", ignore_status=True)
