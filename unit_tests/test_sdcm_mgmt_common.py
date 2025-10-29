from sdcm.mgmt.common import get_manager_scylla_backend, get_manager_repo_from_defaults
from sdcm.utils.distro import Distro


class TestManagerVersions:

    def test_get_manager_scylla_backend_returns_repo_address(self):  # pylint: disable=no-self-use
        url = get_manager_scylla_backend("2025", Distro.UBUNTU22)

        assert url == 'https://downloads.scylladb.com/deb/debian/scylla-2025.3.list'

<<<<<<< HEAD
    def test_get_manager_repo_from_defaults_returns_repo_address(self):  # pylint: disable=no-self-use
        url = get_manager_repo_from_defaults("3.5", Distro.UBUNTU22)
||||||| parent of d79469377 (feature(manager): introduce Manager 3.7 support)
    def test_get_manager_repo_from_defaults_returns_repo_address(self):
        url = get_manager_repo_from_defaults("3.6", Distro.UBUNTU22)
=======
    def test_get_manager_repo_from_defaults_returns_repo_address(self):
        url = get_manager_repo_from_defaults("3.7", Distro.UBUNTU22)
>>>>>>> d79469377 (feature(manager): introduce Manager 3.7 support)

<<<<<<< HEAD
        assert url == 'https://downloads.scylladb.com/deb/debian/scylladb-manager-3.5.list'
||||||| parent of d79469377 (feature(manager): introduce Manager 3.7 support)
        assert url == 'https://downloads.scylladb.com/deb/debian/scylladb-manager-3.6.list'
=======
        assert url == 'https://downloads.scylladb.com/deb/debian/scylladb-manager-3.7.list'
>>>>>>> d79469377 (feature(manager): introduce Manager 3.7 support)
