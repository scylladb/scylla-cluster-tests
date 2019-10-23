import logging
import shutil


class DummeyOutput(object):  # pylint: disable=too-few-public-methods
    def __init__(self, stdout):
        self.stdout = stdout


class DummyRemote(object):  # pylint: disable=too-few-public-methods
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
