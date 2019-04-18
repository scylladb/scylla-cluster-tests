import random
from unittest import TestCase
import logging

from sdcm.utils.common import version

# pylint: disable=function-redefined


class VersionedClass(object):  # pylint: disable=too-few-public-methods,function-redefined
    def __init__(self, current_version):
        self.version = current_version
        self.log = logging.getLogger(__name__)

    @version("1.2")
    def setup(self, data=None):
        self.log.debug(self.version)
        return "1.2", data

    @version("2")
    def setup(self, data=None):
        self.log.debug(self.version)
        return "2", data

    @version("3")
    def setup(self, data=None):
        self.log.debug(self.version)
        return "3", data


class MutliVersionMethods(VersionedClass):  # pylint: disable=too-few-public-methods

    @version("1.2")
    def mvc(self):
        self.log.debug(self.version)
        return "1.2"

    @version("2")
    def mvc(self):
        self.log.debug(self.version)
        return "2"

    @version("3")
    def mvc(self):
        self.log.debug(self.version)
        return "3"


class TestUtilsVersionDecorator(TestCase):
    @staticmethod
    def test_runs_correct_version():
        versions = ["1.2", "2", "3"]
        random.shuffle(versions)
        for ver in versions:
            print ver
            vclass = VersionedClass(ver)
            got_ver, _ = vclass.setup()
            assert ver == got_ver

    def test_assert_no_version_attribute(self):  # pylint: disable=invalid-name
        vclass = VersionedClass("1")
        del vclass.version
        self.assertRaises(AttributeError, vclass.setup)

    def test_version_not_found(self):
        from sdcm.utils.common import MethodVersionNotFound
        vclass = VersionedClass("1")
        self.assertRaises(MethodVersionNotFound, vclass.setup)

    @staticmethod
    def test_different_versioned_methods():  # pylint: disable=invalid-name
        versions = ["1.2", "2", "3"]
        random.shuffle(versions)
        for ver in versions:
            print ver
            vclass = MutliVersionMethods(ver)
            got_ver = vclass.mvc()
            assert ver == got_ver
            assert vclass.mvc.__func__.__name__ == "mvc"
            got_ver, data = vclass.setup("data")
            assert ver == got_ver
            assert data == "data"
            assert vclass.setup.__func__.__name__ == "setup"

    @staticmethod
    def test_data_returned_correctly():
        ver = "3"
        vclass = VersionedClass(ver)
        _, data = vclass.setup(ver)
        assert data == ver
