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

# pylint: disable=too-few-public-methods

import os
import shutil
import logging

from sdcm.remote import LocalCmdRunner


class DummyOutput:
    def __init__(self, stdout):
        self.stdout = stdout


class DummyRemote:
    @staticmethod
    def run(*args, **kwargs):
        logging.info(args, kwargs)
        return DummyOutput(args[0])

    @staticmethod
    def is_up():
        return True

    @staticmethod
    def receive_files(src, dst):
        shutil.copy(src, dst)
        return True


class LocalNode:
    def __init__(self):
        self.remoter = LocalCmdRunner()
        self.ip_address = "127.0.0.1"
        self.logdir = os.path.dirname(__file__)


class LocalLoaderSetDummy:
    def __init__(self):
        self.name = "LocalLoaderSetDummy"
        self.nodes = [LocalNode(), ]

    @staticmethod
    def get_db_auth():
        return None
