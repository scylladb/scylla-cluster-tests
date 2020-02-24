import os
import logging
import shutil
import types

from sdcm.remote import LocalCmdRunner
from sdcm.cluster_docker import send_receive_files


class DummeyOutput():  # pylint: disable=too-few-public-methods
    def __init__(self, stdout):
        self.stdout = stdout


class DummyRemote():  # pylint: disable=too-few-public-methods
    def run(self, *args, **kwargs):  # pylint: disable=no-self-use
        logging.info(args, kwargs)
        return DummeyOutput(args[0])

    @staticmethod
    def is_up():
        return True

    @staticmethod
    def receive_files(src, dst):
        shutil.copy(src, dst)
        return True


class LocalNode:  # pylint: disable=no-init,too-few-public-methods
    def __init__(self):
        self.remoter = LocalCmdRunner()
        self.remoter.receive_files = types.MethodType(send_receive_files, self.remoter)
        self.remoter.send_files = types.MethodType(send_receive_files, self.remoter)
        self.ip_address = "127.0.0.1"
        self.logdir = os.path.dirname(__file__)


class LocalLoaderSetDummy:  # pylint: disable=no-init,too-few-public-methods
    def __init__(self):
        self.name = 'LocalLoaderSetDummy'
        self.nodes = [LocalNode()]

    @staticmethod
    def get_db_auth():
        return None
