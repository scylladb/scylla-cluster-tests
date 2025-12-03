from sdcm.mgmt.common import get_manager_scylla_backend, get_manager_repo_from_defaults
from sdcm.utils.distro import Distro


class TestManagerVersions:
<<<<<<< HEAD

    def test_get_manager_scylla_backend_returns_repo_address(self):  # pylint: disable=no-self-use
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)

    def test_get_manager_scylla_backend_returns_repo_address(self):
=======
    def test_get_manager_scylla_backend_returns_repo_address(self):
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)
        url = get_manager_scylla_backend("2025", Distro.UBUNTU22)

        assert url == "https://downloads.scylladb.com/deb/debian/scylla-2025.3.list"

    def test_get_manager_repo_from_defaults_returns_repo_address(self):  # pylint: disable=no-self-use
        url = get_manager_repo_from_defaults("3.5", Distro.UBUNTU22)

<<<<<<< HEAD
        assert url == 'https://downloads.scylladb.com/deb/debian/scylladb-manager-3.5.list'
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        assert url == 'https://downloads.scylladb.com/deb/debian/scylladb-manager-3.7.list'
=======
        assert url == "https://downloads.scylladb.com/deb/debian/scylladb-manager-3.7.list"
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)
