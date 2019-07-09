import random
from unittest import TestCase
from sdcm.utils import version


class VersionedClass(object):
    def __init__(self, current_version):
        self.version = current_version

    @version("1.2")
    def setup(self, data=None):
        return "1.2", data

    @version("2")
    def setup(self, data=None):
        return "2", data

    @version("3")
    def setup(self, data=None):
        return "3", data


class MutliVersionMethods(VersionedClass):

    @version("1.2")
    def mvc(self):
        return "1.2"

    @version("2")
    def mvc(self):
        return "2"

    @version("3")
    def mvc(self):
        return "3"


class TestUtilsVersionDecorator(TestCase):
    def test_runs_correct_version(self):
        versions = ["1.2", "2", "3"]
        random.shuffle(versions)
        for ver in versions:
            print ver
            vc = VersionedClass(ver)
            got_ver, _ = vc.setup()
            assert ver == got_ver

    def test_assert_no_version_attribute(self):
        vc = VersionedClass("1")
        del vc.version
        self.assertRaises(AttributeError, vc.setup)

    def test_version_not_found(self):
        from sdcm.utils import MethodVersionNotFound
        vc = VersionedClass("1")
        self.assertRaises(MethodVersionNotFound, vc.setup)

    def test_different_versioned_methods(self):
        versions = ["1.2", "2", "3"]
        random.shuffle(versions)
        for ver in versions:
            print ver
            vc = MutliVersionMethods(ver)
            got_ver = vc.mvc()
            assert ver == got_ver
            assert vc.mvc.__func__.__name__ == "mvc"
            got_ver, data = vc.setup("data")
            assert ver == got_ver
            assert data == "data"
            assert vc.setup.__func__.__name__ == "setup"

    def test_data_returned_correctly(self):
        ver = "3"
        vc = VersionedClass(ver)
        _, data = vc.setup(ver)
        assert data == ver
