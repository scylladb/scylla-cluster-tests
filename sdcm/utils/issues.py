import re
import sys
import logging
from functools import lru_cache
from dataclasses import dataclass

import github
import github.Auth
import github.Issue
from github.GithubException import UnknownObjectException, RateLimitExceededException

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
    user_id: str | None
    repo_id: str | None
    issue_id: int | None


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

    @staticmethod
    def parse_issue(issue_str):
        for pattern in [github_issue_pattern, github_issue_https_pattern]:
            match = pattern.search(issue_str.strip())
            if match:
                obj = match.groupdict()
                return Issue(
                    user_id=obj.get("user_id") or "scylladb",
                    repo_id=obj.get("repo_id") or "scylladb",
                    issue_id=int(obj.get("issue_id"))
                )
        return None

    @lru_cache
    def get_issue_details(self, issue):
        issue_parsed = self.parse_issue(issue)
        if not issue_parsed:
            logging.warning("couldn't parse issue: %s", issue)
            TestFrameworkEvent(source=self.__class__.__name__,
                               message=f"couldn't parse issue: {issue}",
                               severity=Severity.WARNING,
                               trace=sys._getframe().f_back).publish()  # pylint: disable=protected-access
            return None
        try:
            return self.github.get_repo(f'{issue_parsed.user_id}/{issue_parsed.repo_id}', lazy=True).get_issue(issue_parsed.issue_id)
        except UnknownObjectException:
            logging.warning("couldn't find issue: %s", issue)
            TestFrameworkEvent(source=self.__class__.__name__,
                               message=f"couldn't find issue: {issue}",
                               severity=Severity.WARNING,
                               trace=sys._getframe().f_back).publish()  # pylint: disable=protected-access
            return None
        except RateLimitExceededException as exc:
            logging.debug('RateLimitExceededException raise: %s', str(exc))
            # as a temporary measure return an "open" issue each time we hit rate limiting
            # this would mean that we would assume issue is open, and enable the skips needed, without having the
            # actual data of the issue
            return github.Issue.Issue(requester=None, headers={}, attributes=dict(state='open'), completed=True)
        except Exception as exc:  # pylint: disable=broad-except
            logging.warning("failed to get issue: %s", issue)
            TestFrameworkEvent(source=self.__class__.__name__,
                               message=f"failed to get issue {issue}",
                               severity=Severity.ERROR,
                               exception=exc).publish()
            return None

    def issues_opened(self) -> bool:
        return any(issue.state != 'closed' for issue in self.issues)

    def issues_labeled(self) -> bool:
        if self.params.scylla_version:
            branch_version = '.'.join(self.params.scylla_version.split('.')[0:2])
            issues_labels = sum([issue.labels for issue in self.issues], [])

            return any(f'sct-{branch_version}-skip' in label.name for label in issues_labels)

        return False

    def __bool__(self):
        return self.issues_opened() or self.issues_labeled()
