import re

import requests
from pkg_resources import parse_version
from repodataParser.RepoParser import Parser


# Examples of ScyllaDB version strings:
#   - 666.development-0.20200205.2816404f575
#   - 3.3.rc1-0.20200209.0d0c1d43188
#   - 2019.1.4-0.20191217.b59e92dbd
SCYLLA_VERSION_RE = re.compile(r"\d+(\.\d+)?\.[\d\w]+([.~][\d\w]+)?")


def get_branch_version(url):
    try:
        return get_branch_version_from_list(url)
    except ValueError:
        pass

    try:
        return get_branch_version_from_repo(url)
    except ValueError:
        raise ValueError("url isn't a correct deb list or yum repo\n\turl:[{}]".format(url))


def get_branch_version_from_list(url):
    page = requests.get(url).text
    list_regex = re.compile(
        r'deb\s+\[arch=(?P<arch>.*?)\]\s(?P<url>http.*?)\s(?P<version_code_name>.*?)\s(?P<component>.*?)\n', re.DOTALL)
    match = list_regex.search(page)
    if not match:
        raise ValueError("url isn't a correct deb list\n\turl:[{}] ".format(url))
    params = list_regex.search(page).groupdict()

    packages_url = "{url}/dists/{version_code_name}/{component}/binary-{arch}/Packages".format(**params)
    page = requests.get(packages_url).text

    versions_regex = re.compile(r'Version: (.*?)\n', re.DOTALL)
    major_versions = [m.groups()[0].split('-')[0] for m in versions_regex.finditer(page)]

    return max(set(major_versions), key=major_versions.count)


def get_branch_version_from_repo(url):
    page = requests.get(url).text
    repo_regex = re.compile(r'baseurl=(http.*?)\$basearch')

    match = repo_regex.search(page)
    if not match:
        raise ValueError("url isn't a correct yum repo\n\turl:[{}] ".format(url))

    repo_base_url = match.groups()[0]
    url = "{}x86_64/repodata/repomd.xml".format(repo_base_url)
    page = requests.get(url).text

    primary_regex = re.compile(r'="(.*?primary.xml.gz)"')
    primary_path = primary_regex.search(page).groups()[0]

    url = "{}x86_64/{}".format(repo_base_url, primary_path)

    parser = Parser(url=url)
    major_versions = [package['version'][1]['ver'] for package in parser.getList()]

    return max(set(major_versions), key=major_versions.count)


def is_enterprise(version):
    """
    :param version: version string
    :return: True if this version string passed is a scylla enterprise version
    """
    return parse_version(version) > parse_version('2000')


GEMINI_VERSION_RE = re.compile(r'\s(?P<gemini_version>([\d]+\.[\d]+\.[\d]+)?),')
# gemini version 1.0.1, commit ef7c6f422c78ef6b84a6f3bccf52ea9ec846bba0, date 2019-05-16T09:56:16Z


def get_gemini_version(output: str):

    # take only version number - 1.0.1
    result = GEMINI_VERSION_RE.search(output)

    if result:
        return result.groupdict().get("gemini_version", None)
    return None
