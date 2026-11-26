from __future__ import absolute_import

import os
import unittest
from unittest.mock import MagicMock, patch

import pytest

import sdcm
from sdcm.utils.version_utils import (
    ARGUS_VERSION_RE,
    SCYLLA_VERSION_GROUPED_RE,
    VERSION_NOT_FOUND_ERROR,
    ComparableScyllaOperatorVersion,
    ComparableScyllaVersion,
    FullVersionTag,
    MethodVersionNotFound,
    RepositoryDetails,
    ScyllaFileType,
    assume_version,
    get_all_versions,
    get_branch_version,
    get_branch_version_for_multiple_repositories,
    get_gemini_version,
    get_git_tag_from_helm_chart_version,
    get_scylla_docker_repo_from_version,
    get_scylla_urls_from_repository,
    get_specific_tag_of_docker_image,
    is_enterprise,
    latest_unified_package,
    parse_scylla_version_tag,
    scylla_versions,
)

BASE_S3_DOWNLOAD_URL = "https://s3.amazonaws.com/downloads.scylladb.com"
DEB_URL = (
    f"{BASE_S3_DOWNLOAD_URL}/enterprise/deb/unstable/stretch/9f724fedb93b4734fcfaec1156806921ff46e956-2bdfa9f7e"
    f"f592edaf15e028faf3b7f695f39ebc1-525a0255f73d454f8f97f32b8bdd71c8dec35d3d/68/scylladb-2019.1/"
    f"scylla.list"
)
RPM_URL = (
    f"{BASE_S3_DOWNLOAD_URL}/enterprise/rpm/unstable/centos/9f724fedb93b4734fcfaec1156806921ff46e956-2bdfa9f7e"
    f"f592edaf15e028faf3b7f695f39ebc1-525a0255f73d454f8f97f32b8bdd71c8dec35d3d-a6b2b2355c666b1893f702a587287da"
    f"978aeec22/71/scylla.repo"
)

BROKEN_URL = "https://www.google.com"


def check_multiple_urls(urls):
    """get_branch_version_for_multiple_repositories returns the expected version for each (url, version) pair."""
    expected_versions, repo_urls = [], []
    for url, expected_version in urls:
        repo_urls.append(url)
        expected_versions.append(expected_version)
    for branch_version, expected_version in zip(
        get_branch_version_for_multiple_repositories(urls=repo_urls), expected_versions
    ):
        assert branch_version == expected_version


class ClassWithVersiondMethods:
    """Object with versioned methods, shaped like a cluster (nemesis_like_class) or a plain object."""

    def __init__(self, scylla_version, nemesis_like_class):
        params = {"scylla_version": scylla_version}
        if scylla_version.startswith("enterprise:"):
            node_scylla_version = "2023.1.dev"
        elif scylla_version.startswith("master:") or scylla_version == "":
            node_scylla_version = "4.7.dev"
        elif ":" in scylla_version:
            node_scylla_version = scylla_version.split(":")[0]
            if node_scylla_version.count(".") < 1:
                node_scylla_version += ".0"
            node_scylla_version += ".dev"
        else:
            node_scylla_version = scylla_version
        nodes = [type("Node", (object,), {"scylla_version": node_scylla_version})]
        if nemesis_like_class:
            self.cluster = type(
                "Cluster",
                (object,),
                {
                    "params": params,
                    "nodes": nodes,
                },
            )
        else:
            self.params = params
            self.nodes = nodes

    @scylla_versions((None, "4.3"))
    def oss_method(self):
        """Selected for OSS 4.3 and lower."""
        return "any 4.3.x and lower"

    @scylla_versions(("4.4.rc1", "4.4.rc1"), ("4.4.rc4", "4.5"))
    def oss_method(self):  # noqa: F811
        """Selected for OSS 4.4.rc1 through 4.5, except 4.4.rc2 and 4.4.rc3."""
        return "all 4.4 and 4.5 except 4.4.rc2 and 4.4.rc3"

    @scylla_versions(("4.6.rc1", None))
    def oss_method(self):  # noqa: F811
        """Selected for OSS 4.6.rc1 and higher."""
        return "4.6.rc1 and higher"

    @scylla_versions((None, "2019.1"))
    def es_method(self):
        """Selected for enterprise 2019.1 and lower."""
        return "any 2019.1.x and lower"

    @scylla_versions(("2020.1.rc1", "2020.1.rc1"), ("2020.1.rc4", "2021.1"))
    def es_method(self):  # noqa: F811
        """Selected for enterprise 2020.1.rc1 through 2021.1, except 2020.1.rc2 and 2020.1.rc3."""
        return "all 2020.1 and 2021.1 except 2020.1.rc2 and 2020.1.rc3"

    @scylla_versions(("2022.1.rc1", None))
    def es_method(self):  # noqa: F811
        """Selected for enterprise 2022.1.rc1 and higher."""
        return "2022.1.rc1 and higher"

    @scylla_versions((None, "4.3"), (None, "2019.1"))
    def mixed_method(self):
        """Selected for OSS 4.3 and lower, or enterprise 2019.1 and lower."""
        return "any 4.3.x and lower, any 2019.1.x and lower"

    @scylla_versions(("4.4.rc1", "4.4.rc1"), ("4.4.rc4", "4.5"), ("2020.1.rc1", "2020.1.rc1"), ("2020.1.rc4", "2021.1"))
    def mixed_method(self):  # noqa: F811
        """Selected for OSS 4.4 through 4.5 and enterprise 2020.1 through 2021.1."""
        return "all 4.4, 4.5, 2020.1 and 2021.1 except 4.4.rc2, 4.4.rc3, 2020.1.rc2 and 2020.1.rc3"

    @scylla_versions(("4.6.rc1", None), ("2022.1.rc1", None))
    def mixed_method(self):  # noqa: F811
        """Selected for OSS 4.6.rc1 and higher, or enterprise 2022.1.rc1 and higher."""
        return "4.6.rc1 and higher, 2022.1.rc1 and higher"

    @scylla_versions(("4.6.rc1", None))
    def new_oss_method(self):
        """Selected for OSS 4.6.rc1 and higher."""
        return "4.6.rc1 and higher"

    @scylla_versions(("2022.1.rc1", None))
    def new_es_method(self):
        """Selected for enterprise 2022.1.rc1 and higher."""
        return "4.6.rc1 and higher"

    @scylla_versions(("4.6.rc1", None), ("2022.1.rc1", None))
    def new_mixed_method(self):
        """Selected for OSS 4.6.rc1 and higher, or enterprise 2022.1.rc1 and higher."""
        return "4.6.rc1 and higher"


def assert_version_comparison(version_string_left, version_string_right, is_left_greater, is_equal, comparable_class):
    """Comparison operators of comparable_class agree with is_left_greater and is_equal."""
    comparable_version_left = comparable_class(version_string_left)
    comparable_version_right = comparable_class(version_string_right)

    compare_expected_result_err_msg = (
        "One of 'is_left_greater' and 'is_equal' must be 'True' and another one must be 'False'"
    )
    assert is_left_greater or is_equal, compare_expected_result_err_msg
    assert not (is_left_greater and is_equal)
    if is_left_greater:
        assert comparable_version_left > comparable_version_right
        assert comparable_version_left >= comparable_version_right
        assert comparable_version_left > version_string_right
        assert comparable_version_left >= version_string_right
        assert comparable_version_right < comparable_version_left
        assert comparable_version_right <= comparable_version_left
        assert comparable_version_right < version_string_left
        assert comparable_version_right <= version_string_left
    else:
        assert comparable_version_left == comparable_version_right
        assert comparable_version_left == version_string_right
        assert comparable_version_left >= comparable_version_right
        assert comparable_version_left >= version_string_right
        assert comparable_version_right <= comparable_version_left
        assert comparable_version_right <= version_string_left


def test_01_get_branch_version_from_list():
    """get_branch_version parses the version from a deb repo list URL."""
    assert get_branch_version(DEB_URL) == "2019.1.1"


def test_02_get_branch_version_from_repo():
    """get_branch_version_for_multiple_repositories parses the version from an rpm repo URL."""
    check_multiple_urls(urls=[(RPM_URL, "2019.1.1")])


def test_03_get_branch_version():
    """get_branch_version_for_multiple_repositories returns the version for rpm and deb URLs together."""
    check_multiple_urls(urls=[(RPM_URL, "2019.1.1"), (DEB_URL, "2019.1.1")])


def test_04_get_branch_version_failed():
    """get_branch_version raises ValueError for a URL that has no version."""
    msg_error = VERSION_NOT_FOUND_ERROR
    with pytest.raises(ValueError, match=msg_error):
        get_branch_version(BROKEN_URL)
    with pytest.raises(ValueError, match=msg_error):
        get_branch_version(BROKEN_URL)
    with pytest.raises(ValueError, match=msg_error):
        get_branch_version(BROKEN_URL)


def test_05_is_enterprise():
    """is_enterprise is True only for enterprise version strings."""
    assert is_enterprise(None) is False
    assert is_enterprise("2019.1.1") is True
    assert is_enterprise("2018") is True
    assert is_enterprise("3.1") is False
    assert is_enterprise("2.2") is False
    assert is_enterprise("666.development") is False


def test_06_get_scylla_urls_from_repository_rpm_one_arch():
    """get_scylla_urls_from_repository builds repomd.xml URLs for one rpm arch."""
    with unittest.mock.patch.object(sdcm.utils.version_utils, "get_url_content", return_value="", clear=True):
        urls = get_scylla_urls_from_repository(
            RepositoryDetails(
                type=ScyllaFileType.YUM,
                urls=[
                    "baseurl=http://downloads.scylladb.com/unstable/scylla/master/rpm/centos/2021-06-29T00:59:00Z/scylla/$basearch/",
                    "[scylla-generic]",
                    "enabled=1",
                    "name=Scylla for centos $releasever",
                    "[scylla]",
                    "name=Scylla for Centos $releasever - $basearch",
                    "baseurl=http://downloads.scylladb.com/unstable/scylla/master/rpm/centos/2021-06-29T00:59:00Z/scylla/noarch/",
                    "gpgcheck=0",
                ],
            )
        )
    assert urls == {
        "http://downloads.scylladb.com/unstable/scylla/master/rpm/centos/2021-06-29T00:59:00Z/scylla/noarch"
        "/repodata/repomd.xml",
        "http://downloads.scylladb.com/unstable/scylla/master/rpm/centos/2021-06-29T00:59:00Z/scylla/x86_64"
        "/repodata/repomd.xml",
    }


def test_06_get_scylla_urls_from_repository_deb_one_arch():
    """get_scylla_urls_from_repository builds a Packages URL for one deb arch."""
    with unittest.mock.patch.object(sdcm.utils.version_utils, "get_url_content", return_value="", clear=True):
        urls = get_scylla_urls_from_repository(
            RepositoryDetails(
                type=ScyllaFileType.DEBIAN,
                urls=[
                    "deb [arch=amd64] http://downloads.scylladb.com/unstable/scylla/master/deb/unified/"
                    "2021-06-23T10:53:35Z/scylladb-master stable main"
                ],
            )
        )
    assert urls == {
        "http://downloads.scylladb.com/unstable/scylla/master/deb/unified/2021-06-23T10:53:35Z/scylladb-master/"
        "dists/stable/main/binary-amd64/Packages",
    }


def test_06_get_scylla_urls_from_repository_deb_two_archs():
    """get_scylla_urls_from_repository builds one Packages URL per deb arch."""
    with unittest.mock.patch.object(sdcm.utils.version_utils, "get_url_content", return_value="", clear=True):
        urls = get_scylla_urls_from_repository(
            RepositoryDetails(
                type=ScyllaFileType.DEBIAN,
                urls=[
                    "deb [arch=amd64,arm64] http://downloads.scylladb.com/unstable/scylla/master/deb/unified/"
                    "2021-06-23T10:53:35Z/scylladb-master stable main"
                ],
            )
        )
    assert urls == {
        "http://downloads.scylladb.com/unstable/scylla/master/deb/unified/2021-06-23T10:53:35Z/scylladb-master/"
        "dists/stable/main/binary-amd64/Packages",
        "http://downloads.scylladb.com/unstable/scylla/master/deb/unified/2021-06-23T10:53:35Z/scylladb-master/"
        "dists/stable/main/binary-arm64/Packages",
    }


def test_06_get_scylla_urls_from_repository_no_urls():
    """get_scylla_urls_from_repository returns an empty set for an empty deb entry."""
    with unittest.mock.patch.object(sdcm.utils.version_utils, "get_url_content", return_value="", clear=True):
        urls = get_scylla_urls_from_repository(
            RepositoryDetails(
                type=ScyllaFileType.DEBIAN,
                urls=[""],
            )
        )
    assert urls == set()


def test_07_get_all_versions():
    """get_all_versions lists the versions published in an rpm repo file."""
    assert "4.5.3" in get_all_versions("https://s3.amazonaws.com/downloads.scylladb.com/rpm/centos/scylla-4.5.repo")


def test_09_assume_versions():
    """assume_version derives the nightly version from the branch env var or the scylla_repo URL."""
    with unittest.mock.patch.object(os.environ, "get", return_value="branch-2022.1", clear=True):
        params = {}
        version = assume_version(params)
        assert version == "nightly-2022.1", "Version should be 2022.1"

        scylla_version = "2022.1"
        version = assume_version(params, scylla_version)
        assert version == "nightly-2022.1", "Version should be 2022.1"

        repo_url = (
            "http://downloads.scylladb.com/unstable/scylla-enterprise/enterprise-2022.1/deb/unified/"
            "2022-05-10T22:12:50Z/scylladb-2022.1/scylla.list"
        )
        params.update({"scylla_repo": repo_url})
        version = assume_version(params)
        assert version == "nightly-2022.1", "Version should be 2022.1"


@pytest.mark.parametrize(
    "chart_version,git_tag",
    [
        pytest.param("v1.1.0-rc.2-0-gc86ad89", "v1.1.0-rc.2", id="rc2-zero-commits-ahead"),
        pytest.param("v1.1.0-rc.1-1-g6d35b37", "v1.1.0-rc.1", id="rc1-one-commit-ahead"),
        pytest.param("v1.1.0-rc.1", "v1.1.0-rc.1", id="rc1-exact-tag"),
        pytest.param("v1.1.0-alpha.0-3-g6594091-nightly", "v1.1.0-alpha.0", id="alpha-nightly-suffix"),
        pytest.param("v1.1.0-alpha.0-3-g6594091", "v1.1.0-alpha.0", id="alpha-commits-ahead"),
        pytest.param("v1.0.0", "v1.0.0", id="release-exact-tag"),
        pytest.param("v1.0.0-39-g5bc1839", "v1.0.0", id="release-commits-ahead"),
        pytest.param("v1.0.0-rc0-53-g489398a-nightly", "v1.0.0-rc0", id="rc0-nightly-suffix"),
        pytest.param("v1.0.0-rc0-53-g489398a", "v1.0.0-rc0", id="rc0-commits-ahead"),
        pytest.param("v1.0.0-rc0-51-ga52c206-latest", "v1.0.0-rc0", id="rc0-latest-suffix"),
    ],
)
def test_06_get_git_tag_from_helm_chart_version(chart_version, git_tag):
    """get_git_tag_from_helm_chart_version strips the commits-ahead and build suffixes."""
    assert get_git_tag_from_helm_chart_version(chart_version) == git_tag


@pytest.mark.parametrize(
    "chart_version",
    [
        pytest.param("", id="empty"),
        pytest.param("fake", id="fake"),
        pytest.param("V1.0.0", id="uppercase-v"),
        pytest.param("1.0.0", id="no-v-prefix"),
        pytest.param("1.1.0-rc.1", id="rc-no-v-prefix"),
        pytest.param("1.1.0-rc.1-1-g6d35b37", id="rc-commits-no-v-prefix"),
    ],
)
def test_07_get_git_tag_from_helm_chart_version__wrong_input(chart_version):
    """get_git_tag_from_helm_chart_version raises ValueError for a malformed chart version."""
    try:
        git_tag = get_git_tag_from_helm_chart_version(chart_version)
    except ValueError:
        return
    assert False, f"'ValueError' was expected, but absent. Returned value: {git_tag}"


@pytest.mark.parametrize(
    "scylla_version,method",
    [
        pytest.param(scylla_version, method, id=f"{scylla_version or 'empty'}-{method}")
        for scylla_version in (
            "",
            "4.2.rc1",
            "4.2",
            "4.2.0",
            "4.2.1",
            "4.3.rc1",
            "4.3",
            "4.3.0",
            "4.3.1",
            "4.4.rc1",
            "4.4.rc4",
            "4.4",
            "4.4.0",
            "4.4.4",
            "4.5.rc1",
            "4.5",
            "4.5.0",
            "4.5.1",
            "4.6.rc1",
            "4.6",
            "4.6.0",
            "4.6.1",
            "4.7.rc1",
            "4.7",
            "4.7.0",
            "4.7.1",
            "5.0.rc1",
            "5.0",
            "5.0.0",
            "5.0.1",
            "4.7:latest",
            "master:latest",
        )
        for method in ("oss_method", "mixed_method")
    ]
    + [
        pytest.param(scylla_version, method, id=f"{scylla_version}-{method}")
        for scylla_version in (
            "2019.1",
            "2019.1.0",
            "2019.1.1",
            "2020.1",
            "2020.1.0",
            "2020.1.1",
            "2021.1",
            "2021.1.0",
            "2021.1.1",
            "2022.1",
            "2022.1.0",
            "2022.1.1",
            "2023:latest",
        )
        for method in ("es_method", "mixed_method")
    ]
    + [
        pytest.param(scylla_version, method, id=f"{scylla_version}-{method}")
        for scylla_version in (
            "4.6.rc1",
            "4.6",
            "4.6.0",
            "4.6.1",
            "4.7:latest",
            "master:latest",
        )
        for method in ("new_oss_method", "new_mixed_method")
    ]
    + [
        pytest.param(scylla_version, method, id=f"{scylla_version}-{method}")
        for scylla_version in ("2022.1.rc1", "2022.1", "2022.1.0", "2022.1.1", "2023:latest")
        for method in ("new_es_method", "new_mixed_method")
    ],
)
def test_scylla_versions_decorator_positive(scylla_version, method):
    """A versioned method is found for every version it supports, on a cluster and on a plain object."""
    for nemesis_like_class in (True, False):
        cls_instance = ClassWithVersiondMethods(scylla_version=scylla_version, nemesis_like_class=nemesis_like_class)
        assert getattr(cls_instance, method)()


@pytest.mark.parametrize(
    "scylla_version,method",
    (
        pytest.param("4.4.rc2", "oss_method", id="4-4-rc2-oss"),
        pytest.param("4.4.rc2", "mixed_method", id="4-4-rc2-mixed"),
        pytest.param("4.4.rc3", "oss_method", id="4-4-rc3-oss"),
        pytest.param("4.4.rc3", "mixed_method", id="4-4-rc3-mixed"),
        pytest.param("2020.1.rc2", "es_method", id="2020-1-rc2-es"),
        pytest.param("2020.1.rc2", "mixed_method", id="2020-1-rc2-mixed"),
        pytest.param("2020.1.rc3", "es_method", id="2020-1-rc3-es"),
        pytest.param("2020.1.rc3", "mixed_method", id="2020-1-rc3-mixed"),
        pytest.param("4.4", "es_method", id="4-4-es"),
        pytest.param("2020.1", "oss_method", id="2020-1-oss"),
        pytest.param("4.5", "new_oss_method", id="4-5-new-oss"),
        pytest.param("4.5", "new_mixed_method", id="4-5-new-mixed"),
        pytest.param("4.6.rc1", "new_es_method", id="4-6-rc1-new-es"),
        pytest.param("2021.1", "new_es_method", id="2021-1-new-es"),
        pytest.param("2021.1", "new_mixed_method", id="2021-1-new-mixed"),
        pytest.param("2022.1.rc1", "new_oss_method", id="2022-1-rc1-new-oss"),
    ),
)
def test_scylla_versions_decorator_negative(scylla_version, method):
    """A versioned method raises MethodVersionNotFound for a version it does not support."""
    for nemesis_like_class in (True, False):
        try:
            cls_instance = ClassWithVersiondMethods(
                scylla_version=scylla_version, nemesis_like_class=nemesis_like_class
            )
            getattr(cls_instance, method)()
        except MethodVersionNotFound as exc:
            assert (
                f"Method '{method}' with version '{scylla_version}' is not supported in '{cls_instance.__class__.__name__}'!"
                in str(exc)
            )
        else:
            assert False, f"Versioned method must have been not found for the '{scylla_version}' scylla version"


def test_scylla_versions_decorator_negative_latest_scylla_no_nodes():
    """A master:latest cluster without nodes raises MethodVersionNotFound with version 'n/a'."""
    scylla_version = "master:latest"
    for nemesis_like_class in (True, False):
        try:
            cls_instance = ClassWithVersiondMethods(
                scylla_version=scylla_version, nemesis_like_class=nemesis_like_class
            )
            try:
                cls_instance.cluster.nodes = []
            except AttributeError:
                cls_instance.nodes = []
            cls_instance.oss_method()
        except MethodVersionNotFound as exc:
            assert (
                f"Method 'oss_method' with version 'n/a' is not supported in '{cls_instance.__class__.__name__}'!"
                in str(exc)
            )
        else:
            assert False, f"Versioned method must have been not found for the '{scylla_version}' scylla version"


def test_scylla_versions_decorator_negative_latest_scylla_no_attr():
    """A master:latest node without scylla_version raises MethodVersionNotFound with version 'n/a'."""
    scylla_version = "master:latest"
    for nemesis_like_class in (True, False):
        try:
            cls_instance = ClassWithVersiondMethods(
                scylla_version=scylla_version, nemesis_like_class=nemesis_like_class
            )
            try:
                delattr(cls_instance.cluster.nodes[0], "scylla_version")
            except AttributeError:
                delattr(cls_instance.nodes[0], "scylla_version")
            cls_instance.oss_method()
        except MethodVersionNotFound as exc:
            assert (
                f"Method 'oss_method' with version 'n/a' is not supported in '{cls_instance.__class__.__name__}'!"
                in str(exc)
            )
        else:
            assert False, f"Versioned method must have been not found for the '{scylla_version}' scylla version"


@pytest.mark.parametrize(
    "full_version,version,date,commit_id",
    (
        pytest.param("5.1.dev-0.20220713.15ed0a441e18", "5.1.dev", "20220713", "15ed0a441e18", id="dev-with-hash"),
        pytest.param("5.0.1-20220719.b177dacd3", "5.0.1", "20220719", "b177dacd3", id="release-no-build-id"),
        pytest.param(
            "5.0.1-20220719.b177dacd3 with build-id 217f31634f8c8722cadcfe57ade8da58af05d415",
            "5.0.1",
            "20220719",
            "b177dacd3",
            id="release-with-build-id",
        ),
        pytest.param("2022.1~rc5-20220515.6a1e89fbb", "2022.1~rc5", "20220515", "6a1e89fbb", id="tilde-rc"),
        pytest.param("2022.2.dev-20220715.6fd8d82112e1", "2022.2.dev", "20220715", "6fd8d82112e1", id="enterprise-dev"),
        pytest.param("4.6.rc2-20220102.e8a1cfb6f", "4.6.rc2", "20220102", "e8a1cfb6f", id="rc-release"),
    ),
)
def test_scylla_version_grouped_regexp(full_version, version, date, commit_id):
    """SCYLLA_VERSION_GROUPED_RE splits the version, date and commit id."""
    parsed_version = SCYLLA_VERSION_GROUPED_RE.match(full_version)
    assert parsed_version.group("version") == version
    assert parsed_version.group("date") == date
    assert parsed_version.group("commit_id") == commit_id


@pytest.mark.parametrize(
    "full_version,short,date,commit_id",
    (
        pytest.param("5.1.dev-0.20220713.15ed0a441e18", "5.1.dev", "20220713", "15ed0a441e18", id="dev-with-hash"),
        pytest.param("5.0.1-20220719.b177dacd3", "5.0.1", "20220719", "b177dacd3", id="release-no-build-id"),
        pytest.param(
            "5.0.1-20220719.b177dacd3 with build-id 217f31634f8c8722cadcfe57ade8da58af05d415",
            "5.0.1",
            "20220719",
            "b177dacd3",
            id="release-with-build-id",
        ),
        pytest.param("2022.1~rc5-20220515.6a1e89fbb", "2022.1~rc5", "20220515", "6a1e89fbb", id="tilde-rc"),
        pytest.param("2022.2.dev-20220715.6fd8d82112e1", "2022.2.dev", "20220715", "6fd8d82112e1", id="enterprise-dev"),
        pytest.param("4.6.rc2-20220102.e8a1cfb6f", "4.6.rc2", "20220102", "e8a1cfb6f", id="rc-release"),
    ),
)
def test_scylla_version_for_argus_regexp(full_version, short, date, commit_id):
    """ARGUS_VERSION_RE splits the short version, date and commit."""
    parsed_version = ARGUS_VERSION_RE.match(full_version)
    assert parsed_version.group("short") == short
    assert parsed_version.group("date") == date
    assert parsed_version.group("commit") == commit_id


@pytest.mark.parametrize(
    "version_string, expected",
    (
        pytest.param("5.1", (5, 1, 0, "", ""), id="two-part"),
        pytest.param("5.1.0", (5, 1, 0, "", ""), id="zero-patch"),
        pytest.param("5.1.1", (5, 1, 1, "", ""), id="patch"),
        pytest.param("5.1.0-rc1", (5, 1, 0, "rc1", ""), id="rc-dash"),
        pytest.param("5.1.0~rc1", (5, 1, 0, "rc1", ""), id="rc-tilde"),
        pytest.param("2022.1-rc8", (2022, 1, 0, "rc8", ""), id="enterprise-rc-dash"),
        pytest.param("2022.1~rc8", (2022, 1, 0, "rc8", ""), id="enterprise-rc-tilde"),
        pytest.param("5.1.rc1", (5, 1, 0, "rc1", ""), id="rc-no-patch"),
        pytest.param("2022.1.3-0.20220922.539a55e35", (2022, 1, 3, "dev-0.20220922", "539a55e35"), id="dev-build"),
        pytest.param(
            "2022.1.3-0.20220922.539a55e35 with build-id d1fb2faafd95058a04aad30b675ff7d2b930278d",
            (2022, 1, 3, "dev-0.20220922", "539a55e35"),
            id="dev-build-with-build-id",
        ),
        pytest.param(
            "2022.1.3-dev-0.20220922.539a55e35", (2022, 1, 3, "dev-0.20220922", "539a55e35"), id="dev-build-dash"
        ),
        pytest.param(
            "5.2.0~rc1-0.20230207.8ff4717fd010", (5, 2, 0, "rc1-0.20230207", "8ff4717fd010"), id="rc-build-tilde"
        ),
        pytest.param(
            "5.2.0-dev-0.20230109.08b3a9c786d9", (5, 2, 0, "dev-0.20230109", "08b3a9c786d9"), id="dev-build-5-2"
        ),
        pytest.param(
            "5.2.0-dev-0.20230109.08b3a9c786d9-x86_64",
            (5, 2, 0, "dev-0.20230109", "08b3a9c786d9"),
            id="dev-build-x86-suffix",
        ),
        pytest.param(
            "5.2.0-dev-0.20230109.08b3a9c786d9-aarch64",
            (5, 2, 0, "dev-0.20230109", "08b3a9c786d9"),
            id="dev-build-aarch64-suffix",
        ),
        pytest.param(
            "2024.2.0.dev.0.20231219.c7cdb16538f2.1",
            (2024, 2, 0, "dev-0.20231219", "c7cdb16538f2.1"),
            id="dotted-dev-build",
        ),
        pytest.param(
            "2024.1.0.rc2.0.20231218.a063c2c16185.1",
            (2024, 1, 0, "rc2-0.20231218", "a063c2c16185.1"),
            id="dotted-rc-build",
        ),
        pytest.param(
            "3.5.0~dev_0.20250105+ef3b96816_SNAPSHOT",
            (3, 5, 0, "dev.0.20250105", "ef3b96816.SNAPSHOT"),
            id="snapshot-plus-build",
        ),
    ),
)
def test_comparable_scylla_version_init_positive(version_string, expected):
    """ComparableScyllaVersion parses major, minor, patch, pre-release and build parts."""
    comparable_scylla_version = ComparableScyllaVersion(version_string)
    assert comparable_scylla_version.v_major == expected[0]
    assert comparable_scylla_version.v_minor == expected[1]
    assert comparable_scylla_version.v_patch == expected[2]
    assert comparable_scylla_version.v_pre_release == expected[3]
    assert comparable_scylla_version.v_build == expected[4]


@pytest.mark.parametrize(
    "version_string",
    (
        pytest.param(None, id="none"),
        pytest.param("", id="empty"),
        pytest.param("5", id="major-only"),
        pytest.param("2023", id="year-only"),
        pytest.param("2023.dev", id="year-dev"),
    ),
)
def test_comparable_scylla_versions_init_negative(version_string):
    """ComparableScyllaVersion raises ValueError for an input it cannot parse."""
    try:
        ComparableScyllaVersion(version_string)
    except ValueError:
        pass
    else:
        assert False, f"'ComparableScyllaVersion' must raise a ValueError for the '{version_string}' provided input"


@pytest.mark.parametrize(
    "version_string_left, version_string_right, is_left_greater, is_equal, comparable_class",
    (
        pytest.param("5.2.2", "5.2.2", False, True, ComparableScyllaVersion, id="equal"),
        pytest.param("5.2.0", "5.1.2", True, False, ComparableScyllaVersion, id="minor-greater"),
        pytest.param("5.2.1", "5.2.0", True, False, ComparableScyllaVersion, id="patch-greater"),
        pytest.param("5.2.10", "5.2.9", True, False, ComparableScyllaVersion, id="patch-double-digit"),
        pytest.param(
            "5.2.0",
            "5.2.0~rc1-0.20230207.8ff4717fd010",
            True,
            False,
            ComparableScyllaVersion,
            id="release-over-rc-build",
        ),
        pytest.param(
            "5.2.0",
            "5.2.0-dev-0.20230109.08b3a9c786d9",
            True,
            False,
            ComparableScyllaVersion,
            id="release-over-dev-build",
        ),
        pytest.param("2024.2.0", "2024.2.0~dev", True, False, ComparableScyllaVersion, id="release-over-dev"),
        pytest.param("6.0.0~rc2", "6.0.0~rc1", True, False, ComparableScyllaVersion, id="rc-number"),
        pytest.param("2023.1.0", "2023.1.rc1", True, False, ComparableScyllaVersion, id="release-over-rc"),
        pytest.param("5.2.0", "5.1.rc1", True, False, ComparableScyllaVersion, id="minor-over-rc"),
        pytest.param(
            "5.2.0-dev-0.20230109.8ff4717fd010",
            "5.2.0-dev-0.20230109.08b3a9c786d9",
            False,
            True,
            ComparableScyllaVersion,
            id="same-dev-build-other-hash",
        ),
    ),
)
def test_comparable_scylla_versions_compare(
    version_string_left, version_string_right, is_left_greater, is_equal, comparable_class
):
    """ComparableScyllaVersion orders versions the way the comparison operators say."""
    assert_version_comparison(version_string_left, version_string_right, is_left_greater, is_equal, comparable_class)


@pytest.mark.parametrize(
    "version_string_input, version_string_output",
    (
        pytest.param("5.2.2", "5.2.2", id="release"),
        pytest.param("2023.1.13", "2023.1.13", id="enterprise-release"),
        pytest.param("5.2.0~rc0-0.20230207", "5.2.0-rc0-0.20230207", id="rc-build-tilde"),
        pytest.param("5.2.0-rc1-0.20230207", "5.2.0-rc1-0.20230207", id="rc-build-dash"),
        pytest.param("5.2.0~dev-0.20230207.8ff4717fd010", "5.2.0-dev-0.20230207+8ff4717fd010", id="dev-build-tilde"),
    ),
)
def test_comparable_scylla_versions_to_str(version_string_input, version_string_output):
    """str(ComparableScyllaVersion) renders the normalized version."""
    assert str(ComparableScyllaVersion(version_string_input)) == version_string_output


@pytest.mark.parametrize(
    "version_string, expected",
    (
        pytest.param("v1.8.0", (1, 8, 0, "", ""), id="v-prefix"),
        pytest.param("1.8.0", (1, 8, 0, "", ""), id="plain"),
        pytest.param("1.8.0-rc.0", (1, 8, 0, "rc.0", ""), id="rc"),
        pytest.param("1.9.0-alpha.1", (1, 9, 0, "alpha.1", ""), id="alpha"),
        pytest.param("1.9.0-alpha.1-nightly", (1, 9, 0, "alpha.1", ""), id="alpha-nightly"),
        pytest.param("1.9.0-alpha.1-2-g3321624", (1, 9, 0, "alpha.1-2-g3321624", ""), id="alpha-commits-ahead"),
        pytest.param(
            "1.9.0-alpha.1-2-g3321624-nightly", (1, 9, 0, "alpha.1-2-g3321624", ""), id="alpha-commits-ahead-nightly"
        ),
        pytest.param("v1.9.0-alpha.1-13-gc6a6e05", (1, 9, 0, "alpha.1-13-gc6a6e05", ""), id="v-alpha-commits-ahead"),
        pytest.param("scylla-operator-1.8.0-beta.1", (1, 8, 0, "beta.1", ""), id="scylla-operator-prefix"),
    ),
)
def test_comparable_scylla_operator_version_init_positive(version_string, expected):
    """ComparableScyllaOperatorVersion parses major, minor, patch, pre-release and build parts."""
    comparable_scylla_operator_version = ComparableScyllaOperatorVersion(version_string)
    assert comparable_scylla_operator_version.v_major == expected[0]
    assert comparable_scylla_operator_version.v_minor == expected[1]
    assert comparable_scylla_operator_version.v_patch == expected[2]
    assert comparable_scylla_operator_version.v_pre_release == expected[3]
    assert comparable_scylla_operator_version.v_build == expected[4]


@pytest.mark.parametrize(
    "version_string",
    (
        pytest.param(None, id="none"),
        pytest.param("", id="empty"),
        pytest.param("1", id="major-only"),
        pytest.param("1.alpha", id="non-numeric-minor"),
    ),
)
def test_comparable_scylla_operator_versions_init_negative(version_string):
    """ComparableScyllaOperatorVersion raises ValueError for an input it cannot parse."""
    try:
        ComparableScyllaOperatorVersion(version_string)
    except ValueError:
        pass
    else:
        assert False, (
            f"'ComparableScyllaOperatorVersion' must raise a ValueError for the '{version_string}' provided input"
        )


@pytest.mark.parametrize(
    "version_string_left, version_string_right, is_left_greater, is_equal, comparable_class",
    (
        pytest.param("v1.7.1", "1.7.0", True, False, ComparableScyllaOperatorVersion, id="v-prefix-patch-greater"),
        pytest.param("1.7.1", "v1.7.0", True, False, ComparableScyllaOperatorVersion, id="v-prefix-right"),
        pytest.param("1.8.0", "1.8.0-rc.0", True, False, ComparableScyllaOperatorVersion, id="release-over-rc"),
        pytest.param("1.8.0", "1.0.0-alpha.1", True, False, ComparableScyllaOperatorVersion, id="release-over-alpha"),
        pytest.param(
            "scylla-operator-v1.8.0-alpha.0-100-gf796b97",
            "1.8.0-alpha.0-99-g1234567",
            True,
            False,
            ComparableScyllaOperatorVersion,
            id="commits-ahead-100-over-99",
        ),
        pytest.param(
            "scylla-operator-v1.8.0-alpha.0-10-gf796b97",
            "1.8.0-alpha.0-9-g1234567",
            True,
            False,
            ComparableScyllaOperatorVersion,
            id="commits-ahead-10-over-9",
        ),
        pytest.param(
            "1.9.0-alpha.1-2-g3321624",
            "1.9.0-alpha.1-2-g3321624-nightly",
            False,
            True,
            ComparableScyllaOperatorVersion,
            id="nightly-suffix-equal",
        ),
        pytest.param("v1.8.0", "1.8.0", False, True, ComparableScyllaOperatorVersion, id="v-prefix-equal"),
        pytest.param(
            "scylla-operator-1.8.0-beta.1",
            "1.8.0-beta.1",
            False,
            True,
            ComparableScyllaOperatorVersion,
            id="operator-prefix-equal",
        ),
        pytest.param(
            "scylla-manager-v1.8.0-alpha.0-100-gf796b97",
            "1.8.0-alpha.0-100-gf796b97",
            False,
            True,
            ComparableScyllaOperatorVersion,
            id="manager-prefix-equal",
        ),
    ),
)
def test_comparable_scylla_operator_versions_compare(
    version_string_left, version_string_right, is_left_greater, is_equal, comparable_class
):
    """ComparableScyllaOperatorVersion orders versions the way the comparison operators say."""
    assert_version_comparison(version_string_left, version_string_right, is_left_greater, is_equal, comparable_class)


@pytest.mark.parametrize(
    "version_string_input, version_string_output",
    (
        pytest.param("scylla-operator-1.8.0-beta.1", "1.8.0-beta.1", id="operator-prefix"),
        pytest.param("scylla-manager-1.8.0-beta.1", "1.8.0-beta.1", id="manager-prefix"),
        pytest.param("scylla-1.8.0-beta.1", "1.8.0-beta.1", id="scylla-prefix"),
        pytest.param(
            "scylla-operator-v1.8.0-alpha.0-100-gf796b97",
            "1.8.0-alpha.0-100-gf796b97",
            id="operator-prefix-commits-ahead",
        ),
        pytest.param(
            "scylla-manager-v1.8.0-alpha.0-100-gf796b97",
            "1.8.0-alpha.0-100-gf796b97",
            id="manager-prefix-commits-ahead",
        ),
        pytest.param(
            "scylla-v1.8.0-alpha.0-100-gf796b97", "1.8.0-alpha.0-100-gf796b97", id="scylla-prefix-commits-ahead"
        ),
        pytest.param("v1.8.1", "1.8.1", id="v-prefix"),
        pytest.param("1.8.1", "1.8.1", id="plain"),
        pytest.param("1.8.0-rc.0", "1.8.0-rc.0", id="rc"),
        pytest.param("1.9.0-alpha.1", "1.9.0-alpha.1", id="alpha"),
        pytest.param("1.9.0-alpha.1-nightly", "1.9.0-alpha.1", id="alpha-nightly"),
        pytest.param("1.9.0-alpha.1-2-g3321624", "1.9.0-alpha.1-2-g3321624", id="alpha-commits-ahead"),
        pytest.param("1.9.0-alpha.1-2-g3321624-nightly", "1.9.0-alpha.1-2-g3321624", id="alpha-commits-ahead-nightly"),
    ),
)
def test_comparable_scylla_operator_versions_to_str(version_string_input, version_string_output):
    """str(ComparableScyllaOperatorVersion) drops the product prefix and keeps the build suffix."""
    assert str(ComparableScyllaOperatorVersion(version_string_input)) == version_string_output


@pytest.mark.parametrize(
    "version, expected_repo",
    (
        pytest.param("6.2.2", "scylladb/scylla", id="oss-6-2-2"),
        pytest.param("6.2.3", "scylladb/scylla", id="oss-6-2-3"),
        pytest.param("6.2.4", "scylladb/scylla", id="oss-6-2-4"),
        pytest.param("6.2.66", "scylladb/scylla", id="oss-6-2-66"),
        pytest.param("2024.1.1", "scylladb/scylla-enterprise", id="enterprise-2024-1-1"),
        pytest.param("2024.2.13", "scylladb/scylla-enterprise", id="enterprise-2024-2-13"),
        pytest.param("2024.2.14", "scylladb/scylla-enterprise", id="enterprise-2024-2-14"),
        pytest.param("2025.1.0", "scylladb/scylla", id="oss-2025-1-0"),
        pytest.param("2025.2.99", "scylladb/scylla", id="oss-2025-2-99"),
        pytest.param("2025.4.0", "scylladb/scylla", id="oss-2025-4-0"),
        pytest.param("2026.1.0", "scylladb/scylla", id="oss-2026-1-0"),
        pytest.param("2025.1.0-dev-0.20251217.55f4a2b75472", "scylladb/scylla-nightly", id="nightly-2025-dev"),
        pytest.param("2026.1.0-dev-0.20251217.55f4a2b75472", "scylladb/scylla-nightly", id="nightly-2026-dev"),
        pytest.param("master:latest", "scylladb/scylla-nightly", id="master-latest"),
    ),
)
def test_verify_docker_repo_implicit_resolution_for_scylla_versions(version, expected_repo):
    """get_scylla_docker_repo_from_version picks the release, enterprise or nightly repo for a version."""
    assert get_scylla_docker_repo_from_version(version) == expected_repo


@pytest.mark.parametrize(
    "version_tag,expected_base,expected_build,expected_date,expected_commit",
    [
        # Full version tag with suffix
        pytest.param(
            "2024.2.5-0.20250221.cb9e2a54ae6d-1", "2024.2.5", "0", "20250221", "cb9e2a54ae6d", id="suffix-build-1"
        ),
        # Full version tag without suffix
        pytest.param("4.6.4-0.20220718.b60f14601", "4.6.4", "0", "20220718", "b60f14601", id="no-suffix"),
        # Dev version tag
        pytest.param("5.2.0~dev-0.20220829.67c91e8bcd61", "5.2.0~dev", "0", "20220829", "67c91e8bcd61", id="dev-tilde"),
        # Release candidate version tag
        pytest.param("3.3.rc1-0.20200209.0d0c1d43188", "3.3.rc1", "0", "20200209", "0d0c1d43188", id="rc-build"),
        # Enterprise version tag
        pytest.param("2019.1.4-0.20191217.b59e92dbd", "2019.1.4", "0", "20191217", "b59e92dbd", id="enterprise-build"),
    ],
)
def test_parse_full_version_tag(version_tag, expected_base, expected_build, expected_date, expected_commit):
    """Test parsing various full version tag formats."""
    tag = parse_scylla_version_tag(version_tag)

    assert tag is not None
    assert tag.base_version == expected_base
    assert tag.build == expected_build
    assert tag.date == expected_date
    assert tag.commit_id == expected_commit
    assert tag.full_tag == version_tag


@pytest.mark.parametrize(
    "version_tag",
    [
        pytest.param("", id="empty"),
        pytest.param("invalid", id="word"),
        pytest.param("1.2.3", id="three-part-version"),
        pytest.param("2024.2.5", id="year-version"),
        pytest.param("not-a-version", id="not-a-version"),
        pytest.param("5.2.1", id="simple-version"),  # Simple version
        pytest.param("master:latest", id="master-branch"),  # Branch version
    ],
)
def test_parse_invalid_or_simple_version_returns_none(version_tag):
    """Test that invalid and simple version strings return None."""
    tag = parse_scylla_version_tag(version_tag)
    assert tag is None, f"Expected None for '{version_tag}', got {tag}"


def test_full_version_tag_class_methods():
    """Test FullVersionTag class methods."""
    version_tag = "2024.2.5-0.20250221.cb9e2a54ae6d-1"
    tag = FullVersionTag.parse(version_tag)

    assert tag is not None
    assert isinstance(tag, FullVersionTag)

    # Test that we can access fields
    assert tag.base_version == "2024.2.5"
    assert tag.build == "0"
    assert tag.date == "20250221"
    assert tag.commit_id == "cb9e2a54ae6d"


def test_full_version_tag_with_rc_build():
    """Test parsing a version tag with rc build."""
    version_tag = "4.5.rc3-rc3.20220101.abc123def"
    tag = parse_scylla_version_tag(version_tag)

    # This should match as SCYLLA_VERSION_GROUPED_RE allows rc\d for build
    assert tag is not None
    if tag:
        assert tag.build == "rc3"


# Full Version Tag Tests


@pytest.mark.parametrize(
    "version_string,should_parse,expected_base",
    [
        # Full version tags
        pytest.param("2024.2.5-0.20250221.cb9e2a54ae6d-1", True, "2024.2.5", id="full-with-suffix"),
        pytest.param("5.2.0-dev-0.20220829.67c91e8bcd61", True, "5.2.0-dev", id="full-dev"),
        pytest.param(
            "2026.1.0~dev-0.20260119.4cde34f6f20b", True, "2026.1.0~dev", id="full-dev-tilde"
        ),  # Test ~dev format
        pytest.param("4.6.4-0.20220718.b60f14601", True, "4.6.4", id="full-no-suffix"),
        # Simple and branch versions should not parse
        pytest.param("5.2.1", False, None, id="simple"),
        pytest.param("master:latest", False, None, id="master-branch"),
        pytest.param("branch-2019.1:latest", False, None, id="named-branch"),
    ],
)
def test_version_string_formats(version_string, should_parse, expected_base):
    """Test different version string formats for AWS."""
    tag = parse_scylla_version_tag(version_string)
    if should_parse:
        assert tag is not None, f"Expected {version_string} to parse as full tag"
        assert tag.base_version == expected_base
    else:
        assert tag is None, f"Expected {version_string} NOT to parse as full tag"


@pytest.mark.parametrize(
    "version_string,should_use_branched,is_full_tag",
    [
        pytest.param("2024.2.5-0.20250221.cb9e2a54ae6d-1", False, True, id="full-with-suffix"),
        pytest.param("5.2.0-dev-0.20220829.67c91e8bcd61", False, True, id="full-dev"),
        pytest.param("4.6.4-0.20220718.b60f14601", False, True, id="full-no-suffix"),
        pytest.param("master:latest", True, False, id="master-branch"),
        pytest.param("branch-2019.1:latest", True, False, id="named-branch"),
        pytest.param("5.2.1", False, False, id="simple"),
        pytest.param("2024.2.0", False, False, id="simple-enterprise"),
    ],
)
def test_version_routing_logic(version_string, should_use_branched, is_full_tag):
    """Test that different version formats route to correct lookup methods.

    This simulates the logic in sct_config.py to ensure:
    - Full version tags use get_scylla_ami_versions (NOT get_branched_ami)
    - Branch versions use get_branched_ami
    - Simple versions use get_scylla_ami_versions
    """
    tag = parse_scylla_version_tag(version_string)

    # Check if it's a full version tag
    is_parsed_as_full_tag = tag is not None
    assert is_parsed_as_full_tag == is_full_tag, (
        f"Version '{version_string}' should{'' if is_full_tag else ' NOT'} be parsed as full tag"
    )

    # Determine routing (simulates sct_config.py logic)
    if is_parsed_as_full_tag:
        # Full version tag: use get_scylla_ami_versions
        uses_branched = False
    elif ":" in version_string:
        # Branch version: use get_branched_ami
        uses_branched = True
    else:
        # Simple version: use get_scylla_ami_versions
        uses_branched = False

    assert uses_branched == should_use_branched, f"Version '{version_string}' routing incorrect"


@pytest.mark.parametrize(
    "full_version_tag,expected_base",
    [
        pytest.param("2024.2.5-0.20250221.cb9e2a54ae6d-1", "2024.2.5", id="suffix-with-build"),
        pytest.param("5.2.0-dev-0.20220829.67c91e8bcd61", "5.2.0-dev", id="dev"),
        pytest.param("4.6.4-0.20220718.b60f14601", "4.6.4", id="no-suffix"),
    ],
)
def test_full_version_string_preservation(full_version_tag, expected_base):
    """Test that full version strings are preserved for exact matching.

    This test verifies that the entire version string is passed
    for exact image/AMI tag matching, not just the base version.
    Applies to AWS, GCE, Azure, and Docker lookups.
    """
    tag = parse_scylla_version_tag(full_version_tag)
    assert tag is not None

    # The full tag should be preserved
    assert tag.full_tag == full_version_tag

    # Verify it's NOT simplified to just the base version
    assert tag.full_tag != tag.base_version
    assert tag.base_version == expected_base

    # The full tag should be used for filtering images
    # (not just the base version which would match many images)
    assert len(full_version_tag) > len(tag.base_version)


# GCE Full Version Tag Tests (pytest style)


@pytest.mark.parametrize(
    "version_string,should_parse,expected_normalized",
    [
        pytest.param(
            "2024.2.5-0.20250221.cb9e2a54ae6d-1", True, "2024-2-5-0-20250221-cb9e2a54ae6d-1", id="full-with-suffix"
        ),
        pytest.param("5.2.0-dev-0.20220829.67c91e8bcd61", True, "5-2-0-dev-0-20220829-67c91e8bcd61", id="full-dev"),
        pytest.param("4.6.4-0.20220718.b60f14601", True, "4-6-4-0-20220718-b60f14601", id="full-no-suffix"),
        pytest.param("5.2.1", False, "5-2-1", id="simple"),
        pytest.param("master:latest", False, None, id="master-branch"),
    ],
)
def test_gce_version_normalization(version_string, should_parse, expected_normalized):
    """Test GCE version label normalization (dots → dashes).

    GCE labels don't allow dots, so they must be converted to dashes.
    This test verifies the normalization for both full version tags and simple versions.
    """
    tag = parse_scylla_version_tag(version_string)

    if should_parse:
        # Full version tag
        assert tag is not None, f"Expected {version_string} to parse as full tag"

        # Normalize for GCE labels (dots → dashes)
        normalized = version_string.replace(".", "-")
        assert normalized == expected_normalized
    elif expected_normalized:
        # Simple version (not full tag, but still valid for GCE)
        assert tag is None, f"Expected {version_string} NOT to parse as full tag"

        # Simple versions also get normalized
        normalized = version_string.replace(".", "-")
        assert normalized == expected_normalized
    else:
        # Branch version
        assert tag is None


# Azure Full Version Tag Tests (pytest style)
# Azure uses different routing logic (released vs non-released)


@pytest.mark.parametrize(
    "version_string,should_use_released,is_full_tag",
    [
        pytest.param("2024.2.5-0.20250221.cb9e2a54ae6d-1", False, True, id="full-with-suffix"),
        pytest.param("5.2.0-dev-0.20220829.67c91e8bcd61", False, True, id="full-dev"),
        pytest.param("4.6.4-0.20220718.b60f14601", False, True, id="full-no-suffix"),
        pytest.param("master:latest", False, False, id="master-branch"),
        pytest.param("branch-2019.1:latest", False, False, id="named-branch"),
        pytest.param("5.2.1", True, False, id="simple"),
        pytest.param("2024.2.0", True, False, id="simple-enterprise"),
    ],
)
def test_azure_version_routing_logic(version_string, should_use_released, is_full_tag):
    """Test Azure version routing logic.

    Verifies that:
    - Full version tags use get_scylla_images (exact match)
    - Branch versions use get_scylla_images
    - Simple versions use get_released_scylla_images
    """
    tag = parse_scylla_version_tag(version_string)

    # Check if it's a full version tag
    is_parsed_as_full_tag = tag is not None
    assert is_parsed_as_full_tag == is_full_tag

    # Determine routing (simulates sct_config.py logic for Azure)
    if is_parsed_as_full_tag:
        # Full version tag: use get_scylla_images for exact match
        uses_released = False
    elif ":" in version_string:
        # Branch version: use get_scylla_images
        uses_released = False
    else:
        # Simple version: use get_released_scylla_images
        uses_released = True

    assert uses_released == should_use_released


# Docker version tests


@pytest.mark.parametrize(
    "version,expected_repo",
    [
        # Full version tags → nightly repos
        pytest.param("2026.1.0~dev-0.20260119.4cde34f6f20b", "scylladb/scylla-nightly", id="full-dev-tilde-nightly"),
        pytest.param("5.2.0-dev-0.20220829.67c91e8bcd61", "scylladb/scylla-nightly", id="full-dev-nightly"),
        # Branch/latest versions
        pytest.param("latest", "scylladb/scylla-nightly", id="latest-nightly"),
        pytest.param("master:latest", "scylladb/scylla-nightly", id="master-nightly"),
        # Simple versions
        pytest.param("5.2.1", "scylladb/scylla", id="simple-oss"),  # Release version
        pytest.param("2024.2.0", "scylladb/scylla-enterprise", id="simple-enterprise"),  # Release version
    ],
)
def test_docker_version_routing_logic(version, expected_repo):
    """Test Docker repo selection for various version formats."""
    repo = get_scylla_docker_repo_from_version(version)
    assert repo == expected_repo, f"Version '{version}' should route to '{expected_repo}', got '{repo}'"


@patch("sdcm.utils.version_utils.boto3")
def test_returns_url_for_matching_package(mock_boto3):
    """latest_unified_package returns the https URL of the matching tarball."""
    mock_s3 = MagicMock()
    mock_boto3.client.return_value = mock_s3
    mock_s3.get_paginator.return_value.paginate.return_value = [
        {
            "Contents": [
                {"Key": "unstable/scylla/master/relocatable/latest/00-Build.txt"},
                {
                    "Key": "unstable/scylla/master/relocatable/latest/"
                    "scylla-unified-5.5.0~dev-0.20231113.7b08886e8dd8.x86_64.tar.gz"
                },
            ]
        }
    ]

    result = latest_unified_package()
    assert result == (
        "https://downloads.scylladb.com/unstable/scylla/master/relocatable/latest/"
        "scylla-unified-5.5.0~dev-0.20231113.7b08886e8dd8.x86_64.tar.gz"
    )


@patch("sdcm.utils.version_utils.boto3")
def test_returns_url_for_aarch64(mock_boto3):
    """latest_unified_package selects the aarch64 tarball when asked for it."""
    mock_s3 = MagicMock()
    mock_boto3.client.return_value = mock_s3
    mock_s3.get_paginator.return_value.paginate.return_value = [
        {
            "Contents": [
                {
                    "Key": "unstable/scylla/master/relocatable/latest/"
                    "scylla-unified-5.5.0~dev-0.20231113.7b08886e8dd8.aarch64.tar.gz"
                },
            ]
        }
    ]

    result = latest_unified_package(arch="aarch64")
    assert "aarch64" in result
    assert "scylla-unified" in result


@patch("sdcm.utils.version_utils.boto3")
def test_normalizes_arm64_to_aarch64(mock_boto3):
    """Test that 'arm64' (AWS naming) is normalized to 'aarch64' (package naming)."""
    mock_s3 = MagicMock()
    mock_boto3.client.return_value = mock_s3
    mock_s3.get_paginator.return_value.paginate.return_value = [
        {
            "Contents": [
                {
                    "Key": "unstable/scylla/master/relocatable/latest/"
                    "scylla-unified-5.5.0~dev-0.20231113.7b08886e8dd8.aarch64.tar.gz"
                },
            ]
        }
    ]

    result = latest_unified_package(arch="arm64")
    assert "aarch64" in result
    assert "scylla-unified" in result


@patch("sdcm.utils.version_utils.boto3")
def test_raises_when_no_matching_package(mock_boto3):
    """latest_unified_package raises FileNotFoundError when no tarball is listed."""
    mock_s3 = MagicMock()
    mock_boto3.client.return_value = mock_s3
    mock_s3.get_paginator.return_value.paginate.return_value = [
        {
            "Contents": [
                {"Key": "unstable/scylla/master/relocatable/latest/00-Build.txt"},
            ]
        }
    ]

    with pytest.raises(FileNotFoundError, match="No scylla-unified package found"):
        latest_unified_package()


@patch("sdcm.utils.version_utils.boto3")
def test_raises_when_empty_bucket(mock_boto3):
    """latest_unified_package raises FileNotFoundError for an empty listing page."""
    mock_s3 = MagicMock()
    mock_boto3.client.return_value = mock_s3
    mock_s3.get_paginator.return_value.paginate.return_value = [{}]

    with pytest.raises(FileNotFoundError, match="No scylla-unified package found"):
        latest_unified_package()


@patch("sdcm.utils.version_utils.boto3")
def test_custom_product_and_branch(mock_boto3):
    """latest_unified_package lists the prefix of the given product and branch."""
    mock_s3 = MagicMock()
    mock_boto3.client.return_value = mock_s3
    mock_s3.get_paginator.return_value.paginate.return_value = [
        {
            "Contents": [
                {
                    "Key": "unstable/scylla-enterprise/enterprise/relocatable/latest/"
                    "scylla-unified-2024.1.0~dev.x86_64.tar.gz"
                },
            ]
        }
    ]

    result = latest_unified_package(product="scylla-enterprise", branch="enterprise")
    assert "scylla-enterprise" in result
    mock_s3.get_paginator.assert_called_once_with("list_objects_v2")
    call_kwargs = mock_s3.get_paginator.return_value.paginate.call_args[1]
    assert call_kwargs["Prefix"] == "unstable/scylla-enterprise/enterprise/relocatable/latest/"


@pytest.mark.parametrize(
    "output, expected",
    (
        pytest.param(
            '{"gemini":{"version":"2.3.9","commit_date":"2026-05-08T13:55:22Z",'
            '"commit_sha":"d139eca4bab10c1b2b7a8b73ca85787deadecc6b"},'
            '"scylla-driver":{"version":"v1.18.0","commit_date":"2026-04-29T17:29:04Z",'
            '"commit_sha":"51b5d22d574212cc61b46a794e0f2ca5bdad9431"}}',
            "2.3.9",
            id="version-json-current",
        ),
        pytest.param('{"gemini": {"version": "1.0.1"}}', "1.0.1", id="version-json-minimal"),
        pytest.param('{"scylla-driver": {"version": "v1.0.0"}}', None, id="missing-gemini-key"),
        pytest.param("garbage non-json output", None, id="non-json-returns-none"),
        pytest.param("", None, id="empty-input"),
    ),
)
def test_get_gemini_version(output, expected):
    """get_gemini_version reads the gemini version from the JSON output, or returns None."""
    assert get_gemini_version(output) == expected


@patch("sdcm.utils.version_utils.create_retry_session")
@patch("sdcm.utils.version_utils.boto3")
def test_get_specific_tag_of_docker_image_nightly_uses_master_without_branch_discovery(mock_boto3, mock_create_session):
    """A nightly docker image resolves its tag from master without branch discovery."""
    mock_response = MagicMock()
    mock_response.content = b"docker-image-name: scylla-nightly:5.2.0-dev-0.20220829.67c91e8bcd61\n"
    mock_session = MagicMock()
    mock_session.__enter__.return_value = mock_session
    mock_session.get.return_value = mock_response
    mock_create_session.return_value = mock_session

    result = get_specific_tag_of_docker_image("scylladb/scylla-nightly")

    assert result == "5.2.0-dev-0.20220829.67c91e8bcd61"
    called_url = mock_session.get.call_args[0][0]
    assert "unstable/scylla/master/" in called_url
    mock_boto3.client.return_value.list_objects_v2.assert_not_called()


def test_get_specific_tag_of_docker_image_unsupported_repo_raises():
    """get_specific_tag_of_docker_image raises ValueError for a repo it does not support."""
    with pytest.raises(ValueError, match="doesn't support"):
        get_specific_tag_of_docker_image("scylladb/some-unsupported-repo")
