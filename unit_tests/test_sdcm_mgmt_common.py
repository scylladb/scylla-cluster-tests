from sdcm.mgmt.common import get_manager_scylla_backend, get_manager_repo_from_defaults
from sdcm.utils.distro import Distro


class TestManagerVersions:

    def test_get_manager_scylla_backend_returns_repo_address(self):  # pylint: disable=no-self-use
        url = get_manager_scylla_backend("2021", Distro.UBUNTU20)

        assert url == 'http://downloads.scylladb.com/deb/ubuntu/scylla-2021.1.list'

    def test_get_manager_repo_from_defaults_returns_repo_address(self):  # pylint: disable=no-self-use
        url = get_manager_repo_from_defaults("3.0", Distro.UBUNTU20)

        assert url == 'http://downloads.scylladb.com.s3.amazonaws.com/deb/ubuntu/scylladb-manager-3.0-focal.list'
