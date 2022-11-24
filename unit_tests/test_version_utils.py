from __future__ import absolute_import

import os
import unittest
from unittest import mock

import pytest
import requests

import sdcm
from sdcm.utils.version_utils import (
    get_branch_version,
    get_all_versions,
    get_branch_version_for_multiple_repositories,
    get_git_tag_from_helm_chart_version,
    get_scylla_urls_from_repository,
    is_enterprise,
    MethodVersionNotFound,
    RepositoryDetails,
    ScyllaFileType,
    scylla_versions,
    assume_version,
    VERSION_NOT_FOUND_ERROR,
    SCYLLA_VERSION_GROUPED_RE,
    get_specific_tag_of_docker_image,
    get_docker_image_by_version,
)

BASE_S3_DOWNLOAD_URL = 'https://s3.amazonaws.com/downloads.scylladb.com'
DEB_URL = \
    f'{BASE_S3_DOWNLOAD_URL}/enterprise/deb/unstable/stretch/9f724fedb93b4734fcfaec1156806921ff46e956-2bdfa9f7e' \
    f'f592edaf15e028faf3b7f695f39ebc1-525a0255f73d454f8f97f32b8bdd71c8dec35d3d/68/scylladb-2019.1/' \
    f'scylla.list'
RPM_URL = \
    f'{BASE_S3_DOWNLOAD_URL}/enterprise/rpm/unstable/centos/9f724fedb93b4734fcfaec1156806921ff46e956-2bdfa9f7e' \
    f'f592edaf15e028faf3b7f695f39ebc1-525a0255f73d454f8f97f32b8bdd71c8dec35d3d-a6b2b2355c666b1893f702a587287da' \
    f'978aeec22/71/scylla.repo'

BROKEN_URL = 'https://www.google.com'


class TestVersionUtils(unittest.TestCase):

    def check_multiple_urls(self, urls):
        expected_versions, repo_urls = [], []
        for (url, expected_version) in urls:
            repo_urls.append(url)
            expected_versions.append(expected_version)
        for branch_version, expected_version in zip(get_branch_version_for_multiple_repositories(urls=repo_urls),
                                                    expected_versions):
            self.assertEqual(branch_version, expected_version)

    def test_01_get_branch_version_from_list(self):
        self.assertEqual(get_branch_version(DEB_URL), '2019.1.1')

    def test_02_get_branch_version_from_repo(self):
        self.check_multiple_urls(urls=[(RPM_URL, "2019.1.1")])

    def test_03_get_branch_version(self):
        self.check_multiple_urls(urls=[(RPM_URL, "2019.1.1"), (DEB_URL, "2019.1.1")])

    def test_04_get_branch_version_failed(self):
        msg_error = VERSION_NOT_FOUND_ERROR
        self.assertRaisesRegex(ValueError, msg_error, get_branch_version, BROKEN_URL)
        self.assertRaisesRegex(ValueError, msg_error, get_branch_version, BROKEN_URL)
        self.assertRaisesRegex(ValueError, msg_error, get_branch_version, BROKEN_URL)

    def test_05_is_enterprise(self):
        self.assertEqual(is_enterprise('2019.1.1'), True)
        self.assertEqual(is_enterprise('2018'), True)
        self.assertEqual(is_enterprise('3.1'), False)
        self.assertEqual(is_enterprise('2.2'), False)
        self.assertEqual(is_enterprise('666.development'), False)

    def test_06_get_scylla_urls_from_repository_rpm_one_arch(self):
        with unittest.mock.patch.object(sdcm.utils.version_utils, 'get_url_content', return_value='', clear=True):
            urls = get_scylla_urls_from_repository(
                RepositoryDetails(
                    type=ScyllaFileType.YUM,
                    urls=[
                        'baseurl=http://downloads.scylladb.com/unstable/scylla/master/rpm/centos/2021-06-29T00:59:00Z/scylla/$basearch/',
                        '[scylla-generic]', 'enabled=1',
                        'name=Scylla for centos $releasever',
                        '[scylla]',
                        'name=Scylla for Centos $releasever - $basearch',
                        'baseurl=http://downloads.scylladb.com/unstable/scylla/master/rpm/centos/2021-06-29T00:59:00Z/scylla/noarch/',
                        'gpgcheck=0'
                    ]
                )
            )
        self.assertEqual(
            {
                'http://downloads.scylladb.com/unstable/scylla/master/rpm/centos/2021-06-29T00:59:00Z/scylla/noarch'
                '/repodata/repomd.xml',
                'http://downloads.scylladb.com/unstable/scylla/master/rpm/centos/2021-06-29T00:59:00Z/scylla/x86_64'
                '/repodata/repomd.xml',
            },
            urls)

    def test_06_get_scylla_urls_from_repository_deb_one_arch(self):
        with unittest.mock.patch.object(sdcm.utils.version_utils, 'get_url_content', return_value='', clear=True):
            urls = get_scylla_urls_from_repository(RepositoryDetails(
                type=ScyllaFileType.DEBIAN,
                urls=['deb [arch=amd64] http://downloads.scylladb.com/unstable/scylla/master/deb/unified/'
                      '2021-06-23T10:53:35Z/scylladb-master stable main'],
            ))
        self.assertEqual(
            {
                'http://downloads.scylladb.com/unstable/scylla/master/deb/unified/2021-06-23T10:53:35Z/scylladb-master/'
                'dists/stable/main/binary-amd64/Packages',
            },
            urls)

    def test_06_get_scylla_urls_from_repository_deb_two_archs(self):
        with unittest.mock.patch.object(sdcm.utils.version_utils, 'get_url_content', return_value='', clear=True):
            urls = get_scylla_urls_from_repository(RepositoryDetails(
                type=ScyllaFileType.DEBIAN,
                urls=['deb [arch=amd64,arm64] http://downloads.scylladb.com/unstable/scylla/master/deb/unified/'
                      '2021-06-23T10:53:35Z/scylladb-master stable main'],
            ))
        self.assertEqual(
            {
                'http://downloads.scylladb.com/unstable/scylla/master/deb/unified/2021-06-23T10:53:35Z/scylladb-master/'
                'dists/stable/main/binary-amd64/Packages',
                'http://downloads.scylladb.com/unstable/scylla/master/deb/unified/2021-06-23T10:53:35Z/scylladb-master/'
                'dists/stable/main/binary-arm64/Packages',
            },
            urls)

    def test_06_get_scylla_urls_from_repository_no_urls(self):
        with unittest.mock.patch.object(sdcm.utils.version_utils, 'get_url_content', return_value='', clear=True):
            urls = get_scylla_urls_from_repository(RepositoryDetails(
                type=ScyllaFileType.DEBIAN,
                urls=[''],
            ))
        self.assertEqual(
            set(),
            urls)

    def test_07_get_all_versions(self):
        self.assertIn('4.5.3', get_all_versions(
            'https://s3.amazonaws.com/downloads.scylladb.com/rpm/centos/scylla-4.5.repo'))

    def test_08_assume_versions_oss(self):
        with unittest.mock.patch.object(os.environ, 'get', return_value='branch-5.0', clear=True):
            params = {}
            is_version_enterprise, version = assume_version(params)
            self.assertTrue(not is_version_enterprise, 'This should be OSS')
            self.assertEqual(version, 'nightly-5.0', 'Version should be 5.0')

            scylla_version = '5.0'
            is_version_enterprise, version = assume_version(params, scylla_version)
            self.assertTrue(not is_version_enterprise, 'This should be OSS')
            self.assertEqual(version, 'nightly-5.0', 'Version should be 5.0')

            repo_url = 'https://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla/branch-5.0/rpm/centos/' \
                       '2022-05-10T07:40:41Z/scylla.repo'
            params.update({'scylla_repo': repo_url})
            is_version_enterprise, version = assume_version(params)
            self.assertTrue(not is_version_enterprise, 'This should be OSS')
            self.assertEqual(version, 'nightly-5.0', 'Version should be 5.0')

    def test_09_assume_versions_enterprise(self):
        with unittest.mock.patch.object(os.environ, 'get', return_value='branch-2022.1', clear=True):
            params = {}
            is_version_enterprise, version = assume_version(params)
            self.assertTrue(is_version_enterprise, 'This should be enterprise')
            self.assertEqual(version, 'nightly-2022.1', 'Version should be 2022.1')

            scylla_version = '2022.1'
            is_version_enterprise, version = assume_version(params, scylla_version)
            self.assertTrue(is_version_enterprise, 'This should be enterprise')
            self.assertEqual(version, 'nightly-2022.1', 'Version should be 2022.1')

            repo_url = 'http://downloads.scylladb.com/unstable/scylla-enterprise/enterprise-2022.1/deb/unified/' \
                       '2022-05-10T22:12:50Z/scylladb-2022.1/scylla.list'
            params.update({'scylla_repo': repo_url})
            is_version_enterprise, version = assume_version(params)
            self.assertTrue(is_version_enterprise, 'This should be enterprise')
            self.assertEqual(version, 'nightly-2022.1', 'Version should be 2022.1')


@pytest.mark.parametrize("chart_version,git_tag", [
    ("v1.1.0-rc.2-0-gc86ad89", "v1.1.0-rc.2"),
    ("v1.1.0-rc.1-1-g6d35b37", "v1.1.0-rc.1"),
    ("v1.1.0-rc.1", "v1.1.0-rc.1"),
    ("v1.1.0-alpha.0-3-g6594091-nightly", "v1.1.0-alpha.0"),
    ("v1.1.0-alpha.0-3-g6594091", "v1.1.0-alpha.0"),
    ("v1.0.0", "v1.0.0"),
    ("v1.0.0-39-g5bc1839", "v1.0.0"),
    ("v1.0.0-rc0-53-g489398a-nightly", "v1.0.0-rc0"),
    ("v1.0.0-rc0-53-g489398a", "v1.0.0-rc0"),
    ("v1.0.0-rc0-51-ga52c206-latest", "v1.0.0-rc0"),
])
def test_06_get_git_tag_from_helm_chart_version(chart_version, git_tag):
    assert get_git_tag_from_helm_chart_version(chart_version) == git_tag


@pytest.mark.parametrize("chart_version", [
    "", "fake", "V1.0.0", "1.0.0", "1.1.0-rc.1", "1.1.0-rc.1-1-g6d35b37",
])
def test_07_get_git_tag_from_helm_chart_version__wrong_input(chart_version):
    try:
        git_tag = get_git_tag_from_helm_chart_version(chart_version)
    except ValueError:
        return
    assert False, f"'ValueError' was expected, but absent. Returned value: {git_tag}"


class ClassWithVersiondMethods:  # pylint: disable=too-few-public-methods
    def __init__(self, scylla_version, nemesis_like_class):
        params = {"scylla_version": scylla_version}
        if scylla_version.startswith('enterprise:'):
            node_scylla_version = "2023.dev"
        elif scylla_version.startswith('master:') or scylla_version == "":
            node_scylla_version = "4.7.dev"
        else:
            node_scylla_version = "{}.dev".format(scylla_version.split(":")[0])
        nodes = [type("Node", (object,), {"scylla_version": node_scylla_version})]
        if nemesis_like_class:
            self.cluster = type("Cluster", (object,), {
                "params": params,
                "nodes": nodes,
            })
        else:
            self.params = params
            self.nodes = nodes

    @scylla_versions((None, "4.3"))
    def oss_method(self):  # pylint: disable=no-self-use
        return "any 4.3.x and lower"

    @scylla_versions(("4.4.rc1", "4.4.rc1"), ("4.4.rc4", "4.5"))
    def oss_method(self):  # pylint: disable=no-self-use,function-redefined
        return "all 4.4 and 4.5 except 4.4.rc2 and 4.4.rc3"

    @scylla_versions(("4.6.rc1", None))
    def oss_method(self):  # pylint: disable=no-self-use,function-redefined
        return "4.6.rc1 and higher"

    @scylla_versions((None, "2019.1"))
    def es_method(self):  # pylint: disable=no-self-use
        return "any 2019.1.x and lower"

    @scylla_versions(("2020.1.rc1", "2020.1.rc1"), ("2020.1.rc4", "2021.1"))
    def es_method(self):  # pylint: disable=no-self-use,function-redefined
        return "all 2020.1 and 2021.1 except 2020.1.rc2 and 2020.1.rc3"

    @scylla_versions(("2022.1.rc1", None))
    def es_method(self):  # pylint: disable=no-self-use,function-redefined
        return "2022.1.rc1 and higher"

    @scylla_versions((None, "4.3"), (None, "2019.1"))
    def mixed_method(self):  # pylint: disable=no-self-use
        return "any 4.3.x and lower, any 2019.1.x and lower"

    @scylla_versions(("4.4.rc1", "4.4.rc1"), ("4.4.rc4", "4.5"),
                     ("2020.1.rc1", "2020.1.rc1"), ("2020.1.rc4", "2021.1"))
    def mixed_method(self):  # pylint: disable=no-self-use,function-redefined
        return "all 4.4, 4.5, 2020.1 and 2021.1 except 4.4.rc2, 4.4.rc3, 2020.1.rc2 and 2020.1.rc3"

    @scylla_versions(("4.6.rc1", None), ("2022.1.rc1", None))
    def mixed_method(self):  # pylint: disable=no-self-use,function-redefined
        return "4.6.rc1 and higher, 2022.1.rc1 and higher"

    @scylla_versions(("4.6.rc1", None))
    def new_oss_method(self):  # pylint: disable=no-self-use,function-redefined
        return "4.6.rc1 and higher"

    @scylla_versions(("2022.1.rc1", None))
    def new_es_method(self):  # pylint: disable=no-self-use,function-redefined
        return "4.6.rc1 and higher"

    @scylla_versions(("4.6.rc1", None), ("2022.1.rc1", None))
    def new_mixed_method(self):  # pylint: disable=no-self-use,function-redefined
        return "4.6.rc1 and higher"


@pytest.mark.parametrize("scylla_version,method", [(scylla_version, method) for scylla_version in (
    "", "4.2.rc1", "4.2", "4.2.0", "4.2.1",
    "4.3.rc1", "4.3", "4.3.0", "4.3.1",
    "4.4.rc1", "4.4.rc4", "4.4", "4.4.0", "4.4.4",
    "4.5.rc1", "4.5", "4.5.0", "4.5.1",
    "4.6.rc1", "4.6", "4.6.0", "4.6.1",
    "4.7.rc1", "4.7", "4.7.0", "4.7.1",
    "5.0.rc1", "5.0", "5.0.0", "5.0.1",
    "4.7:latest", "master:latest",
) for method in ("oss_method", "mixed_method")] + [(scylla_version, method) for scylla_version in (
    "2019.1", "2019.1.0", "2019.1.1",
    "2020.1", "2020.1.0", "2020.1.1",
    "2021.1", "2021.1.0", "2021.1.1",
    "2022.1", "2022.1.0", "2022.1.1",
    "2023:latest", "enterprise:latest",
) for method in ("es_method", "mixed_method")] + [(scylla_version, method) for scylla_version in (
    "4.6.rc1", "4.6", "4.6.0", "4.6.1", "4.7:latest", "master:latest",
) for method in ("new_oss_method", "new_mixed_method")] + [(scylla_version, method) for scylla_version in (
    "2022.1.rc1", "2022.1", "2022.1.0", "2022.1.1", "2023:latest", "enterprise:latest",
) for method in ("new_es_method", "new_mixed_method")])
def test_scylla_versions_decorator_positive(scylla_version, method):
    for nemesis_like_class in (True, False):
        cls_instance = ClassWithVersiondMethods(
            scylla_version=scylla_version, nemesis_like_class=nemesis_like_class)
        assert getattr(cls_instance, method)()


@pytest.mark.parametrize("scylla_version,method", (
    ("4.4.rc2", "oss_method"),
    ("4.4.rc2", "mixed_method"),
    ("4.4.rc3", "oss_method"),
    ("4.4.rc3", "mixed_method"),
    ("2020.1.rc2", "es_method"),
    ("2020.1.rc2", "mixed_method"),
    ("2020.1.rc3", "es_method"),
    ("2020.1.rc3", "mixed_method"),
    ("4.4", "es_method"),
    ("2020.1", "oss_method"),
    ("4.5", "new_oss_method"),
    ("4.5", "new_mixed_method"),
    ("4.6.rc1", "new_es_method"),
    ("2021.1", "new_es_method"),
    ("2021.1", "new_mixed_method"),
    ("2022.1.rc1", "new_oss_method"),
))
def test_scylla_versions_decorator_negative(scylla_version, method):
    for nemesis_like_class in (True, False):
        try:
            cls_instance = ClassWithVersiondMethods(
                scylla_version=scylla_version, nemesis_like_class=nemesis_like_class)
            getattr(cls_instance, method)()
        except MethodVersionNotFound as exc:
            assert "Method '{}' with version '{}' is not supported in '{}'!".format(
                method, scylla_version, cls_instance.__class__.__name__) in str(exc)
        else:
            assert False, f"Versioned method must have been not found for the '{scylla_version}' scylla version"


def test_scylla_versions_decorator_negative_latest_scylla_no_nodes():
    scylla_version = "master:latest"
    for nemesis_like_class in (True, False):
        try:
            cls_instance = ClassWithVersiondMethods(
                scylla_version=scylla_version, nemesis_like_class=nemesis_like_class)
            try:
                cls_instance.cluster.nodes = []
            except AttributeError:
                cls_instance.nodes = []
            cls_instance.oss_method()
        except MethodVersionNotFound as exc:
            assert "Method 'oss_method' with version 'n/a' is not supported in '{}'!".format(
                cls_instance.__class__.__name__) in str(exc)
        else:
            assert False, f"Versioned method must have been not found for the '{scylla_version}' scylla version"


def test_scylla_versions_decorator_negative_latest_scylla_no_attr():
    scylla_version = "master:latest"
    for nemesis_like_class in (True, False):
        try:
            cls_instance = ClassWithVersiondMethods(
                scylla_version=scylla_version, nemesis_like_class=nemesis_like_class)
            try:
                delattr(cls_instance.cluster.nodes[0], "scylla_version")
            except AttributeError:
                delattr(cls_instance.nodes[0], "scylla_version")
            cls_instance.oss_method()
        except MethodVersionNotFound as exc:
            assert "Method 'oss_method' with version 'n/a' is not supported in '{}'!".format(
                cls_instance.__class__.__name__) in str(exc)
        else:
            assert False, f"Versioned method must have been not found for the '{scylla_version}' scylla version"


@pytest.mark.need_network
@pytest.mark.integration
@pytest.mark.parametrize('docker_repo', ['scylladb/scylla-nightly', 'scylladb/scylla-enterprise-nightly'])
def test_get_specific_tag_of_docker_image(docker_repo):
    assert get_specific_tag_of_docker_image(docker_repo=docker_repo) != 'latest'


@pytest.mark.parametrize("full_version,version,date,commit_id", (
    ("5.1.dev-0.20220713.15ed0a441e18", "5.1.dev", "20220713", "15ed0a441e18"),
    ("5.0.1-20220719.b177dacd3", "5.0.1", "20220719", "b177dacd3"),
    ("5.0.1-20220719.b177dacd3 with build-id 217f31634f8c8722cadcfe57ade8da58af05d415", "5.0.1", "20220719", "b177dacd3"),
    ("2022.1~rc5-20220515.6a1e89fbb", "2022.1~rc5", "20220515", "6a1e89fbb"),
    ("2022.2.dev-20220715.6fd8d82112e1", "2022.2.dev", "20220715", "6fd8d82112e1"),
    ("4.6.rc2-20220102.e8a1cfb6f", "4.6.rc2", "20220102", "e8a1cfb6f")
))
def test_scylla_version_grouped_regexp(full_version, version, date, commit_id):
    parsed_version = SCYLLA_VERSION_GROUPED_RE.match(full_version)
    assert parsed_version.group("version") == version
    assert parsed_version.group("date") == date
    assert parsed_version.group("commit_id") == commit_id


@pytest.mark.integration
@pytest.mark.need_network
@pytest.mark.parametrize("version, expected", (
    ("4.6.rc2-20220102.e8a1cfb6f", "scylladb/scylla:4.6.rc2"),
    ("5.0.1-0.20220719.b177dacd3", "scylladb/scylla:5.0.1"),
    ("5.1", "scylladb/scylla:latest"),
    ("5.1.0~rc1", "scylladb/scylla:5.1.0-rc1"),
    ("5.0.1", "scylladb/scylla:5.0.1"),
    ("5.2.0~dev-0.20220824.6ce5e9079c1e", "scylladb/scylla-nightly:5.2.0-dev-0.20220824.6ce5e9079c1e"),
    ("5.1.0~dev-0.20220726.29c28dcb0c33", "scylladb/scylla-nightly:5.1.0-dev-0.20220726.29c28dcb0c33"),
    ("2022.2.dev-20220515.no_such_sha", "scylladb/scylla-enterprise:latest"),
    ("5.2.0~dev-20220515.no_such_sha", "scylladb/scylla:latest"),
))
def test_get_docker_image_by_version(version, expected):
    assert get_docker_image_by_version(scylla_version=version) == expected


def test_get_docker_image_by_version_broken_string_version():
    with pytest.raises(AssertionError):
        get_docker_image_by_version(scylla_version="broken_version")


def test_get_docker_image_by_version_fallback_on_errors():
    def mock_requests_factory(response_stub):
        return mock.Mock(**{
            'json.return_value': response_stub,
        })

    def raise_request_error(**kwargs):
        raise requests.HTTPError()

    non_existing_version = '5.2.0~dev-0.20221124.999b7f5d9b77'
    expected_fallback = 'scylladb/scylla:latest'
    with unittest.mock.patch("requests.get") as get_mock:
        get_mock.side_effect = lambda **_: mock_requests_factory({})
        assert get_docker_image_by_version(scylla_version=non_existing_version) == expected_fallback

        get_mock.side_effect = lambda **_: mock_requests_factory({"results": [{'name': ''}, {}]})
        assert get_docker_image_by_version(scylla_version=non_existing_version) == expected_fallback

        get_mock.side_effect = raise_request_error
        assert get_docker_image_by_version(scylla_version=non_existing_version) == expected_fallback
