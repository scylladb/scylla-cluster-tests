import requests
import re
from repodataParser.RepoParser import Parser


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
    list_regex = re.compile(r'deb\s\[arch=(?P<arch>.*?)\]\s(?P<url>http://downloads.scylladb.com/.*?)\s(?P<version_code_name>.*?)\s(?P<component>.*?)\n', re.DOTALL)
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
    repo_regex = re.compile(r'baseurl=(http://downloads.scylladb.com/.*?)\$basearch')

    match = repo_regex.search(page)
    if not match:
        raise ValueError("url isn't a correct yum repo\n\turl:[{}] ".format(url))

    repo_base_url = match.groups()[0]
    url = "{}x86_64/repodata/repomd.xml".format(repo_base_url)
    page = requests.get(url).text

    primary_regex = re.compile(r'="(.*?primary.xml.gz)"')
    primary_path = primary_regex.search(page).groups()[0]

    url = "{}x86_64/{}".format(repo_base_url, primary_path)

    p = Parser(url=url)
    major_versions = [package['version'][1]['ver'] for package in p.getList()]

    return max(set(major_versions), key=major_versions.count)
