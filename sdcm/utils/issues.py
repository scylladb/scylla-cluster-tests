import re
import sys
import csv
import logging
from functools import lru_cache
from dataclasses import dataclass

import github
import github.Auth
import github.Issue
import github.Label
from github.GithubException import UnknownObjectException, RateLimitExceededException
from botocore.exceptions import ClientError
from sdcm.keystore import KeyStore
from sdcm.sct_config import SCTConfiguration
from sdcm.sct_events.base import Severity
from sdcm.sct_events.system import TestFrameworkEvent

github_issue_pattern = re.compile(
    r"^\s*((((?P<user_id>[\w:-]+)/)?(?P<repo_id>[\w:-]+))?#)?(?P<issue_id>\d+)\s*$", re.IGNORECASE)

github_issue_https_pattern = re.compile(
    r"^https?:/.*?/(?P<user_id>[\w:-]+)/(?P<repo_id>[\w:-]+)/.*?/(?P<issue_id>\d+?)$", re.IGNORECASE)


@dataclass
class Issue:
<<<<<<< HEAD
    user_id: str | None
    repo_id: str | None
    issue_id: int | None
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
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
=======
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
    """Class to retrieve Jira issues with caching support"""

    def __init__(self):
        self.s3_cache = CachedJiraIssues()

    @cached_property
    def jira(self) -> JIRA | None:
        try:
            credentials = KeyStore().get_jira_credentials()

            return JIRA(
                server=credentials["jira_server"], basic_auth=(credentials["jira_email"], credentials["jira_api_token"])
            )
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
            return Issue(
                title=jira_issue.fields.summary,
                number=jira_issue.key,
                state=jira_issue.fields.status.name.lower(),
                labels=jira_issue.fields.labels,
            )
        except Exception as ex:  # noqa: BLE001
            warnings.warn(f"failed to get issue: {ex}", RuntimeWarning)
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)


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

    @lru_cache()
    def get_repo_data(self, owner, repo):
        issues_csv = self.storage.get_file_contents(f'issues/{owner}_{repo}.csv')
        pull_requests_csv = self.storage.get_file_contents(f'issues/pull-requests/{owner}_{repo}.csv')
        scsv = issues_csv.strip() + pull_requests_csv.strip()
<<<<<<< HEAD

        issues = {issue['id']: issue for issue in csv.DictReader(
            scsv.decode().splitlines(), fieldnames=("id", "state", "labels", "title"))}
        issues = {issue['id']: issue | dict(
            labels=[dict(name=label) for label in issue['labels'].strip().rstrip('|').split('|')]) for issue in issues.values()}
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        issues = {}
        for issue in csv.DictReader(scsv.decode().splitlines(), fieldnames=("number", "state", "labels", "title")):
            issue_id = int(issue["number"])
            labels = [label for label in issue["labels"].strip().split("|") if label]
            issues[issue_id] = Issue(number=issue_id, state=issue["state"].lower(),
                                     labels=labels, title=issue.get("title", None))
=======
        issues = {}
        for issue in csv.DictReader(scsv.decode().splitlines(), fieldnames=("number", "state", "labels", "title")):
            issue_id = int(issue["number"])
            labels = [label for label in issue["labels"].strip().split("|") if label]
            issues[issue_id] = Issue(
                number=issue_id, state=issue["state"].lower(), labels=labels, title=issue.get("title", None)
            )
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)
        return issues

<<<<<<< HEAD
    def get_issue(self, owner: str, repo_id: str, issue_id: str | int):
        repo_issues_mapping = self.get_repo_data(owner, repo_id)
        return repo_issues_mapping.get(str(issue_id))
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
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
=======
    def get_issue(self, owner: str, repo_id: str, issue_id: int) -> Issue:
        repo_issues_mapping = self.get_repo(owner, repo_id)
        return repo_issues_mapping.get(issue_id)


@cache
class GithubIssueRetriever:
    """Class to retrieve GitHub issues/pull-requests with caching support"""

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
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)


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
        self.cache = CachedGitHubIssues()

        self.params = params
        issues = [issues] if isinstance(issues, str) else issues

        self.issues = [self.get_issue_details(issue) for issue in issues]
        self.issues = list(filter(lambda x: x, self.issues))  # filter None - unmatched issues

    @staticmethod
    def parse_issue(issue_str):
        for pattern in [github_issue_pattern, github_issue_https_pattern]:
            match = pattern.search(issue_str.strip())
            if match:
                obj = match.groupdict()
                repo_id = obj.get("repo_id") or "scylladb"
                return Issue(
                    user_id=obj.get("user_id") or "scylladb",
                    repo_id=(repo_id if repo_id != "scylla" else "scylladb"),
                    issue_id=int(obj.get("issue_id"))
                )
        return None

    @lru_cache
    def get_issue_details(self, issue):
        issue_parsed = self.parse_issue(issue)
        if not issue_parsed:
            logging.warning("couldn't parse issue: %s", issue)
            TestFrameworkEvent(
                source=self.__class__.__name__,
                message=f"couldn't parse issue: {issue}",
                severity=Severity.WARNING,
                trace=sys._getframe().f_back,
            ).publish()
            return None
<<<<<<< HEAD
        try:
            if issue_details := self.cache.get_issue(owner=issue_parsed.user_id, repo_id=issue_parsed.repo_id, issue_id=issue_parsed.issue_id):
                return github.Issue.Issue(requester=None, headers={},
                                          attributes=dict(state=issue_details['state'].lower(),
                                                          labels=issue_details['labels']),
                                          completed=True)
        except ClientError as exc:
            severity = Severity.ERROR
            # some repos are not cached so we just warns about, and fallback
            if issue_parsed.repo_id in ['field-engineering']:
                severity = Severity.WARNING
            else:
                logging.warning("failed to get issue: %s from s3 cache", issue)
            TestFrameworkEvent(source=self.__class__.__name__,
                               message=f"failed to get issue {issue} from s3 cache",
                               severity=severity,
                               exception=exc).publish()
        try:
            return self.github.get_repo(f'{issue_parsed.user_id}/{issue_parsed.repo_id}', lazy=True).get_issue(issue_parsed.issue_id)
        except UnknownObjectException:
            logging.warning("couldn't find issue: %s", issue)
            TestFrameworkEvent(source=self.__class__.__name__,
                               message=f"couldn't find issue: {issue}",
                               severity=Severity.WARNING,
                               trace=sys._getframe().f_back).publish()
            return None
        except RateLimitExceededException as exc:
            logging.debug('RateLimitExceededException raise: %s', str(exc))
            # as a temporary measure return an "open" issue each time we hit rate limiting
            # this would mean that we would assume issue is open, and enable the skips needed, without having the
            # actual data of the issue
            return github.Issue.Issue(requester=None, headers={}, attributes=dict(state='open'), completed=True)
        except Exception as exc:  # noqa: BLE001
            logging.warning("failed to get issue: %s", issue)
            TestFrameworkEvent(source=self.__class__.__name__,
                               message=f"failed to get issue {issue}",
                               severity=Severity.ERROR,
                               exception=exc).publish()
            return None
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)

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
=======

        if isinstance(ref, GitHubIssue):
            issue_id = ref.number
            try:
                return GithubIssueRetriever().get_issue(ref.user, ref.repo, issue_id)
            except Exception as exc:  # noqa: BLE001
                logging.warning("failed to get issue: %s\n %s", issue, exc)
                TestFrameworkEvent(
                    source=self.__class__.__name__,
                    message=f"failed to get issue {issue}",
                    severity=Severity.ERROR,
                    exception=exc,
                ).publish()
                return None

        if isinstance(ref, JiraIssue):
            issue_id = ref.key
            try:
                return JiraIssueRetriever().get_issue(issue_id)
            except Exception as exc:  # noqa: BLE001
                logging.warning("failed to get issue: %s\n %s", issue, exc)
                TestFrameworkEvent(
                    source=self.__class__.__name__,
                    message=f"failed to get issue {issue}",
                    severity=Severity.ERROR,
                    exception=exc,
                ).publish()
                return None

        logging.warning("issue should be github/jira issue: %s", issue)
        TestFrameworkEvent(
            source=self.__class__.__name__,
            message=f"issue should be github/jira issue {issue}",
            severity=Severity.ERROR,
        ).publish()
        return None
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

    def issues_opened(self) -> bool:
<<<<<<< HEAD
        return any(issue.state not in ('closed', 'merged') for issue in self.issues)
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        # both final states of jira and github combined
        return any(issue.state not in ('closed', 'merged', 'done', 'duplicate', 'deferred') for issue in self.issues)
=======
        # both final states of jira and github combined
        return any(issue.state not in ("closed", "merged", "done", "duplicate", "deferred") for issue in self.issues)
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

    def issues_labeled(self) -> bool:
        if self.params.scylla_version:
            branch_version = ".".join(self.params.scylla_version.split(".")[0:2])
            issues_labels = sum([issue.labels for issue in self.issues], [])
<<<<<<< HEAD

            return any(f'sct-{branch_version}-skip' in label.name for label in issues_labels)
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
            return any(f'sct-{branch_version}-skip' in label for label in issues_labels) or any(f'dtest/{branch_version}-skip' in label for label in issues_labels)
=======
            return any(f"sct-{branch_version}-skip" in label for label in issues_labels) or any(
                f"dtest/{branch_version}-skip" in label for label in issues_labels
            )
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

        return False

    def __bool__(self):
        return self.issues_opened() or self.issues_labeled()
