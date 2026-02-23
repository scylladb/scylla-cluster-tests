import pytest

from sdcm.mgmt.common import get_manager_scylla_backend, get_manager_repo
from sdcm.utils.distro import Distro


class TestManagerVersions:
    @pytest.mark.parametrize(
        "version, distro, expected_url",
        [
            (
                "2025.4",
                Distro.UBUNTU24,
                "https://s3.amazonaws.com/downloads.scylladb.com/deb/debian/scylla-2025.4.list",
            ),
            (
                "2025.4",
                Distro.CENTOS9,
                "https://s3.amazonaws.com/downloads.scylladb.com/rpm/centos/scylla-2025.4.repo",
            ),
            (
                "2025.4.1",
                Distro.UBUNTU24,
                "https://s3.amazonaws.com/downloads.scylladb.com/deb/debian/scylla-2025.4.list",
            ),
        ],
    )
    def test_get_manager_scylla_backend(self, version, distro, expected_url):
        assert get_manager_scylla_backend(version, distro) == expected_url

    @pytest.mark.parametrize(
        "version, distro, expected_url",
        [
            (
                "3.8",
                Distro.UBUNTU24,
                "https://downloads.scylladb.com/deb/debian/scylladb-manager-3.8.list",
            ),
            (
                "3.8",
                Distro.CENTOS9,
                "https://downloads.scylladb.com/rpm/centos/scylladb-manager-3.8.repo",
            ),
            (
                "3.8.1",
                Distro.UBUNTU24,
                "https://downloads.scylladb.com/deb/debian/scylladb-manager-3.8.list",
            ),
            (
                "master_latest",
                Distro.UBUNTU24,
                "https://downloads.scylladb.com/manager/deb/unstable/unified-deb/master/latest/scylla-manager.list",
            ),
            (
                "master_latest",
                Distro.CENTOS9,
                "https://downloads.scylladb.com/manager/rpm/unstable/centos/master/latest/scylla-manager.repo",
            ),
        ],
    )
    def test_get_manager_repo(self, version, distro, expected_url):
        assert get_manager_repo(version, distro) == expected_url
