from sdcm.mgmt.common import get_manager_scylla_backend, get_manager_repo_from_defaults
from sdcm.utils.distro import Distro


class TestManagerVersions:

    def test_get_manager_scylla_backend_returns_repo_address(self):  # pylint: disable=no-self-use
        url = get_manager_scylla_backend("2025", Distro.UBUNTU22)

<<<<<<< HEAD
        assert url == 'https://downloads.scylladb.com/deb/debian/scylla-2025.1.list'
||||||| parent of 05254bacd (fix(manager): up SM DB version to the latest 2025.3)
        assert url == 'https://downloads.scylladb.com/deb/debian/scylla-2025.2.list'
=======
        assert url == 'https://downloads.scylladb.com/deb/debian/scylla-2025.3.list'
>>>>>>> 05254bacd (fix(manager): up SM DB version to the latest 2025.3)

    def test_get_manager_repo_from_defaults_returns_repo_address(self):  # pylint: disable=no-self-use
        url = get_manager_repo_from_defaults("3.5", Distro.UBUNTU22)

        assert url == 'https://downloads.scylladb.com/deb/debian/scylladb-manager-3.5.list'
