import re
import sys
import csv
import warnings
import logging
from functools import lru_cache, cache, cached_property
from dataclasses import dataclass
from typing import NamedTuple

import github
import github.Auth
from jira import JIRA

from sdcm.keystore import KeyStore
from sdcm.sct_config import SCTConfiguration
from sdcm.sct_events.base import Severity
from sdcm.sct_events.system import TestFrameworkEvent

DEFAULT_GH_USER = "scylladb"
DEFAULT_GH_REPO = "scylladb"

GITHUB_ISSUE_PATTERN = re.compile(
    r"""
        \s*
        (
            (
                ((?P<user_id>[\w-]+)/)?
                (?P<repo_id>[\w-]+)
            )?
            \#
        |
            http(s?)://github.com/
            (?P<url_user_id>[\w-]+)/
            (?P<url_repo_id>[\w-]+)/
            (issues|pull)/
        )?
        (?P<id>\d+)
        \s*
        """,
    re.IGNORECASE | re.VERBOSE,
)

JIRA_ISSUE_PATTERN = re.compile(
    r"""
        \s*
        (
            jira:
            |
            https?://scylladb.atlassian.net/browse/
        )
        (?P<id>[\w-]+)
        \s*
    """,
    re.IGNORECASE | re.VERBOSE,
)


class JiraIssue(NamedTuple):
    key: str

    @property
    def normalized(self) -> str:
        return f"jira:{self.key}"


class GitHubIssue(NamedTuple):
    user: str
    repo: str
    number: int

    @property
    def normalized(self) -> str:
        return f"{self.user}/{self.repo}#{self.number}"


def parse_issue(s: str = "") -> GitHubIssue | JiraIssue | None:
    s = s.strip()
    if not s:
        raise ValueError("empty issue reference")

    if m := JIRA_ISSUE_PATTERN.match(s):
        key = m.groupdict()["id"]
        return JiraIssue(key=key)

    if m := GITHUB_ISSUE_PATTERN.match(s):
        g = m.groupdict()
        user = g.get("user_id") or g.get("url_user_id") or DEFAULT_GH_USER
        repo = g.get("repo_id") or g.get("url_repo_id") or DEFAULT_GH_REPO
        raw_id = g["id"]
        if not raw_id.isdigit():
            raise ValueError(f"invalid GitHub id: {raw_id!r}")
        return GitHubIssue(user=user, repo=repo, number=int(raw_id))

    raise ValueError(f"invalid issue reference: {s!r}")


@dataclass
class Issue:
    number: str | int
    state: str
    labels: list[str]
    title: str | None = None  # Title is optional, for github issue we don't fetch from cache


@lru_cache(maxsize=1)
class CachedJiraIssues:
    """
    This class provides a cache for issue data retrieved from an S3 bucket

    The cache is automatically populated by the `.github/workflows/cache-jira-issues.yaml` workflow every 6 hours.
    """

    def __init__(self):
        self.storage = KeyStore()

    @lru_cache
    def get_project(self, project: str) -> dict[str, Issue]:
        issues_csv = self.storage.get_file_contents(f"issues/jira_scylladb_{project}.csv")
        scsv = issues_csv.strip()
        issues = {}
        for issue in csv.DictReader(scsv.decode().splitlines()[1:], fieldnames=("number", "state", "labels", "title")):
            issue_id = issue["number"]
            labels = [label for label in issue["labels"].strip().split("|") if label]
            issues[issue_id] = Issue(number=issue_id, state=issue["state"].lower(), labels=labels, title=issue["title"])
        return issues

    def get_issue(self, issue_id: str) -> Issue:
        project = issue_id.split("-")[0]
        return self.get_project(project=project).get(issue_id)


@cache
class JiraIssueRetriever:
    """ Class to retrieve Jira issues with caching support """

    def __init__(self):
        self.s3_cache = CachedJiraIssues()

    @cached_property
    def jira(self) -> JIRA | None:
        try:
            credentials = KeyStore().get_jira_credentials()

            return JIRA(server=credentials["jira_server"], basic_auth=(credentials["jira_email"], credentials["jira_api_token"]))
        except Exception as ex:  # noqa: BLE001
            logging.warning(f"failed to create jira client: {ex}")

    @lru_cache
    def get_issue(self, issue_id: str) -> Issue | None:
        try:
            return self.s3_cache.get_issue(issue_id)
        except Exception as ex:  # noqa: BLE001
            warnings.warn(f"failed to get issue from cache: {ex}", DeprecationWarning)

        try:
            if not self.jira:
                raise ValueError("Jira client is not initialized. Check your credentials.")
            jira_issue = self.jira.issue(issue_id, expand="labels,summary,status")
            return Issue(title=jira_issue.fields.summary, number=jira_issue.key, state=jira_issue.fields.status.name.lower(), labels=jira_issue.fields.labels)
        except Exception as ex:  # noqa: BLE001
            warnings.warn(f"failed to get issue: {ex}", RuntimeWarning)


@lru_cache(maxsize=1)
class CachedGitHubIssues:
    """
    This class would cache the issues from the s3 bucket, and return the issue details
    the cache is populated by `.github/workflows/cache-issues.yaml` workflow
    every 6 hours.

    it's main goal is to make sure we don't reach the rate limit of the github api
    """

    def __init__(self):
        self.storage = KeyStore()

    @lru_cache
    def get_repo(self, owner: str, repo: str) -> dict[int, Issue]:
        issues_csv = self.storage.get_file_contents(f"issues/{owner}_{repo}.csv")
        pull_requests_csv = self.storage.get_file_contents(f"issues/pull-requests/{owner}_{repo}.csv")
        scsv = issues_csv.strip() + pull_requests_csv.strip()
        issues = {}
        for issue in csv.DictReader(scsv.decode().splitlines(), fieldnames=("number", "state", "labels", "title")):
            issue_id = int(issue["number"])
            labels = [label for label in issue["labels"].strip().split("|") if label]
            issues[issue_id] = Issue(number=issue_id, state=issue["state"].lower(),
                                     labels=labels, title=issue.get("title", None))
        return issues

    def get_issue(self, owner: str, repo_id: str, issue_id: int) -> Issue:
        repo_issues_mapping = self.get_repo(owner, repo_id)
        return repo_issues_mapping.get(issue_id)


@cache
class GithubIssueRetriever:
    """ Class to retrieve GitHub issues/pull-requests with caching support """

    def __init__(self):
        self.s3_cache = CachedGitHubIssues()

    @cached_property
    def git(self):
        github_access = KeyStore().get_json("github_access.json")
        auth = github.Auth.Token(token=github_access["token"])
        _github = github.Github(auth=auth, retry=None)
        rate = _github.get_rate_limit()
        logging.debug(rate.raw_data)
        return _github

    @lru_cache
    def get_issue(self, user_id: str, repo_id: str, issue_id: int) -> Issue | None:
        try:
            repo = self.s3_cache.get_repo(user_id, repo_id)
            return repo[issue_id]
        except Exception as ex:  # noqa: BLE001
            warnings.warn(f"failed to get issue from cache: {ex}", DeprecationWarning)

        try:
            if not self.git:
                raise ValueError("GitHub client is not initialized. Check your GITHUB_TOKEN.")
            repo = self.git.get_repo(f"{user_id}/{repo_id}", lazy=True)
            issue = repo.get_issue(issue_id)
            return Issue(
                number=issue.number,
                state=issue.state,
                labels=[label.name for label in issue.labels],
                title=issue.title,
            )
        except Exception as ex:  # noqa: BLE001
            warnings.warn(f"failed to get issue: {ex}", RuntimeWarning)


class SkipPerIssues:
    """
    instance of this class would return true, if one of the issue on the list is open
    or one of the issue is tagged with `sct-{branch_version}-skip`

    if version isn't defined on SctConfiguration, it would act as the issue is not labeled with skipped,
    and would return false if all issues closed.
    """

    _github = None

    @classmethod
    @property
    def github(cls):
        if not cls._github:
            github_access = KeyStore().get_json("github_access.json")
            auth = github.Auth.Token(token=github_access["token"])
            cls._github = github.Github(auth=auth, retry=None)
            rate = cls._github.get_rate_limit()
            logging.debug(rate.raw_data)
        return cls._github

    def __init__(self, issues: list[str] | str, params: SCTConfiguration | dict):
        self.params = params
        issues = [issues] if isinstance(issues, str) else issues

        self.issues = [self.get_issue_details(issue) for issue in issues]
        self.issues = list(filter(lambda x:  x, self.issues))  # filter None - unmatched issues

    @lru_cache
    def get_issue_details(self, issue) -> Issue | None:
        try:
            ref = parse_issue(issue)

        except ValueError:
            logging.warning("couldn't parse issue: %s", issue)
            TestFrameworkEvent(source=self.__class__.__name__,
                               message=f"couldn't parse issue: {issue}",
                               severity=Severity.WARNING,
                               trace=sys._getframe().f_back).publish()
            return None

        if isinstance(ref, GitHubIssue):
            issue_id = ref.number
            try:
                return GithubIssueRetriever().get_issue(ref.user, ref.repo, issue_id)
            except Exception as exc:  # noqa: BLE001
                logging.warning("failed to get issue: %s\n %s", issue, exc)
                TestFrameworkEvent(source=self.__class__.__name__,
                                   message=f"failed to get issue {issue}", severity=Severity.ERROR, exception=exc).publish()
                return None

        if isinstance(ref, JiraIssue):
            issue_id = ref.key
            try:
                return JiraIssueRetriever().get_issue(issue_id)
            except Exception as exc:  # noqa: BLE001
                logging.warning("failed to get issue: %s\n %s", issue, exc)
                TestFrameworkEvent(source=self.__class__.__name__,
                                   message=f"failed to get issue {issue}", severity=Severity.ERROR, exception=exc).publish()
                return None

        logging.warning("issue should be github/jira issue: %s", issue)
        TestFrameworkEvent(source=self.__class__.__name__,
                           message=f"issue should be github/jira issue {issue}", severity=Severity.ERROR).publish()
        return None

    def issues_opened(self) -> bool:
        # both final states of jira and github combined
        return any(issue.state not in ('closed', 'merged', 'done', 'duplicate', 'deferred') for issue in self.issues)

    def issues_labeled(self) -> bool:
        if self.params.scylla_version:
            branch_version = '.'.join(self.params.scylla_version.split('.')[0:2])
            issues_labels = sum([issue.labels for issue in self.issues], [])
            return any(f'sct-{branch_version}-skip' in label for label in issues_labels) or any(f'dtest/{branch_version}-skip' in label for label in issues_labels)

        return False

    def __bool__(self):
        return self.issues_opened() or self.issues_labeled()
