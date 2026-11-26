import pytest

from sdcm.mgmt.common import get_manager_repo, get_manager_scylla_backend
from sdcm.utils.distro import Distro


@pytest.mark.parametrize(
    "version, distro, expected_url",
    [
        pytest.param(
            "2025.4",
            Distro.UBUNTU24,
            "https://s3.amazonaws.com/downloads.scylladb.com/deb/debian/scylla-2025.4.list",
            id="ubuntu-release",
        ),
        pytest.param(
            "2025.4",
            Distro.CENTOS9,
            "https://s3.amazonaws.com/downloads.scylladb.com/rpm/centos/scylla-2025.4.repo",
            id="centos-release",
        ),
        pytest.param(
            "2025.4.1",
            Distro.UBUNTU24,
            "https://s3.amazonaws.com/downloads.scylladb.com/deb/debian/scylla-2025.4.list",
            id="ubuntu-patch-release",
        ),
    ],
)
def test_get_manager_scylla_backend(version, distro, expected_url):
    """get_manager_scylla_backend returns the Scylla repo file for the version and distro, dropping the patch part."""
    assert get_manager_scylla_backend(version, distro) == expected_url


@pytest.mark.parametrize(
    "version, distro, expected_url",
    [
        pytest.param(
            "3.12",
            Distro.UBUNTU24,
            "https://downloads.scylladb.com/deb/debian/scylladb-manager-3.12.list",
            id="ubuntu-release",
        ),
        pytest.param(
            "3.12",
            Distro.CENTOS9,
            "https://downloads.scylladb.com/rpm/centos/scylladb-manager-3.12.repo",
            id="centos-release",
        ),
        pytest.param(
            "3.8.1",
            Distro.UBUNTU24,
            "https://downloads.scylladb.com/deb/debian/scylladb-manager-3.8.list",
            id="ubuntu-patch-release",
        ),
        pytest.param(
            "master_latest",
            Distro.UBUNTU24,
            "https://downloads.scylladb.com/manager/deb/unstable/unified-deb/master/latest/scylla-manager.list",
            id="ubuntu-master-latest",
        ),
        pytest.param(
            "master_latest",
            Distro.CENTOS9,
            "https://downloads.scylladb.com/manager/rpm/unstable/centos/master/latest/scylla-manager.repo",
            id="centos-master-latest",
        ),
    ],
)
def test_get_manager_repo(version, distro, expected_url):
    """get_manager_repo returns the Scylla Manager repo file for the version and distro, with master_latest as unstable."""
    assert get_manager_repo(version, distro) == expected_url
