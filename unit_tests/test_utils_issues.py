import json

import pytest

from sdcm.utils.issues import SkipPerIssues, parse_issue, GitHubIssue, JiraIssue, DEFAULT_GH_USER, DEFAULT_GH_REPO

pytestmark = pytest.mark.integration


def test_no_existing_issue(events, params):
    params.scylla_version = '2023.1.3'
    with events.wait_for_n_events(events.get_events_logger(), count=1, timeout=10):
        assert not SkipPerIssues(["ABC"], params)

    events_list = []
    with events.get_raw_events_log().open() as events_file:
        for line in events_file.readlines():
            events_list.append(json.loads(line))

    assert events_list[-1]['message'] == "couldn't parse issue: ABC"
    assert events_list[-1]['severity'] == 'WARNING'


def test_open_issues(params):
    params.scylla_version = '2023.1.3'
    assert SkipPerIssues(["https://github.com/scylladb/qa-tasks/issues/1613",
                          "scylladb/qa-tasks#1613", ], params)


def test_closed_issues(params):
    params.scylla_version = '2023.1.3'
    assert not SkipPerIssues(
        ["https://github.com/scylladb/qa-tasks/issues/1614",
         "scylladb/qa-tasks#1614"], params)


def test_closed_issues_with_skip_tag(params):
    params.scylla_version = '2023.1.3'
    assert SkipPerIssues(
        ["https://github.com/scylladb/qa-tasks/issues/1615",
         "scylladb/qa-tasks#1615"], params)


def test_no_version(params):
    params.scylla_version = None
    assert SkipPerIssues(
        ["https://github.com/scylladb/qa-tasks/issues/1613",
         "scylladb/qa-tasks#1613"], params)


def test_opened_issue_with_closed_issue(params):
    params.scylla_version = '2023.1.3'
    assert SkipPerIssues(
        ["scylladb/qa-tasks#1613", "scylladb/qa-tasks#1614"], params)

    assert SkipPerIssues(
        ["scylladb/qa-tasks#1614", "scylladb/qa-tasks#1613"], params)


def test_closed_issue_with_tag_not_matching_version(params):
    params.scylla_version = '2019.1.3'
    assert SkipPerIssues(
        ["scylladb/qa-tasks#1613", "scylladb/qa-tasks#1615"], params)

    assert SkipPerIssues(
        ["scylladb/qa-tasks#1615", "scylladb/qa-tasks#1613"], params)


def test_opened_issue_tag_not_matching_version_tag(params):
    params.scylla_version = '2019.1.3'

    assert not SkipPerIssues(
        ["scylladb/qa-tasks#1615"], params)


def test_closed_pull_request(params):
    params.scylla_version = '2019.1.3'

    assert not SkipPerIssues(
        ["scylladb/scylla-cluster-tests#7832"], params)


@pytest.mark.integration
@pytest.mark.parametrize("issue, opened", [
    pytest.param("https://scylladb.atlassian.net/browse/STAG-399", True, id='open-issue-url'),  # open JIRA issue
    pytest.param("jira:STAG-399", True, id='open-issue-id'),  # open JIRA issue
    pytest.param("jira:STAG-585", False, id='closed-issue-id'),  # closed JIRA issue
    pytest.param("https://scylladb.atlassian.net/browse/STAG-100000", False,
                 id='non-existing-issue-url'),  # non-existing JIRA issue
])
def test_jira_issues(params, issue, opened):
    """
    Test that JIRA issues are correctly identified as closed or open based on their state.
    """
    result = SkipPerIssues(
        [issue], params)

    assert bool(
        result) == opened, f"Issue state mismatch for issue {issue}, should be {'opened' if opened else 'closed'}."


@pytest.mark.parametrize(
    "raw, expected_type, expected_norm, exp",
    [
        # GitHub
        ("888", GitHubIssue, f"{DEFAULT_GH_USER}/{DEFAULT_GH_REPO}#888",
         {"user": DEFAULT_GH_USER, "repo": DEFAULT_GH_REPO, "number": 888}),
        ("#888", GitHubIssue, f"{DEFAULT_GH_USER}/{DEFAULT_GH_REPO}#888",
         {"user": DEFAULT_GH_USER, "repo": DEFAULT_GH_REPO, "number": 888}),
        ("my-repo#888", GitHubIssue, f"{DEFAULT_GH_USER}/my-repo#888",
         {"user": DEFAULT_GH_USER, "repo": "my-repo", "number": 888}),
        ("my_user/my_repo#888", GitHubIssue, "my_user/my_repo#888",
         {"user": "my_user", "repo": "my_repo", "number": 888}),
        ("user/repo#000123", GitHubIssue, "user/repo#123", {"user": "user", "repo": "repo", "number": 123}),
        ("http://github.com/u/r/issues/42", GitHubIssue, "u/r#42", {"user": "u", "repo": "r", "number": 42}),
        ("https://github.com/u/r/issues/42", GitHubIssue, "u/r#42", {"user": "u", "repo": "r", "number": 42}),
        ("https://github.com/u/r/pull/42", GitHubIssue, "u/r#42", {"user": "u", "repo": "r", "number": 42}),
        # JIRA
        ("jira:STAG-399", JiraIssue, "jira:STAG-399", {"key": "STAG-399"}),
        ("JIRA:STAG-399", JiraIssue, "jira:STAG-399", {"key": "STAG-399"}),
        ("https://scylladb.atlassian.net/browse/STAG-399", JiraIssue, "jira:STAG-399", {"key": "STAG-399"}),
        ("http://scylladb.atlassian.net/browse/STAG-399", JiraIssue, "jira:STAG-399", {"key": "STAG-399"}),
    ],
    ids=[
        # GH
        "gh-bare-number",
        "gh-hash-number",
        "gh-repo-hash-number",
        "gh-user-repo-hash-number",
        "gh-leading-zeros",
        "gh-url-http-issue",
        "gh-url-https-issue",
        "gh-url-pr",
        # JIRA
        "jira-prefix",
        "jira-prefix-uppercase",
        "jira-url-https",
        "jira-url-http",
    ],
)
def test_parse_issue_success(raw, expected_type, expected_norm, exp):
    ref = parse_issue(raw)

    assert isinstance(ref, expected_type)
    assert ref.normalized == expected_norm

    if expected_type is GitHubIssue:
        assert (ref.user, ref.repo, ref.number) == (exp["user"], exp["repo"], exp["number"])
    else:  # JiraIssue
        assert ref.key == exp["key"]


INVALID_CASES = [
    # empty / whitespace
    pytest.param((), r"^empty issue reference$", id="empty-default"),
    pytest.param(("   ",), r"^empty issue reference$", id="empty-whitespace"),
    # malformed JIRA / GH / no match
    pytest.param(("jira:",), r"^invalid issue reference: 'jira:'$", id="jira-prefix-no-key"),
    pytest.param(("user/repo#",), r"^invalid issue reference: 'user/repo#'$", id="gh-missing-number"),
    pytest.param(("user/repo#abc",), r"^invalid issue reference: 'user/repo#abc'$", id="gh-nonnumeric-id"),
    pytest.param(("not-an-issue",), r"^invalid issue reference: 'not-an-issue'$", id="no-match"),
]


@pytest.mark.parametrize("args, expected_pattern", INVALID_CASES)
def test_parse_issue_invalid_raises(args, expected_pattern):
    with pytest.raises(ValueError, match=expected_pattern):
        parse_issue(*args)
