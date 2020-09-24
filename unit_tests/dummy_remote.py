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
from typing import Optional

from sdcm.remote import LocalCmdRunner


class DummyOutput:
    def __init__(self, stdout):
        self.stdout = stdout
        self.exit_status = 0
        self.stderr = ""


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


class DummyRemoteWithException(DummyRemote):

    def __init__(self) -> None:
        # Hold exception object that should be raised
        self.test_exception: Optional[Exception] = None
        # How many time return exception from run
        self.exceptions_amount: int = 0
        # How many times the "run" function has been called.
        self.run_number: int = 0

    def run(self, *args, **kwargs):
        logging.info(args, kwargs)
        self.raise_exception()

        if 'sudo coredumpctl --no-pager' in args[0]:
            dummy_output = "No coredumps found"
        else:
            dummy_output = args[0]
        return DummyOutput(dummy_output)

    def raise_exception(self):
        if self.test_exception:
            if self.exceptions_amount:
                self.exceptions_amount -= 1
                self.run_number += 1
                raise self.test_exception  # pylint: disable=raising-bad-type


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
