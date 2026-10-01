import pytest

from sdcm.utils.issues import parse_issue, GitHubIssue, JiraIssue, DEFAULT_GH_USER, DEFAULT_GH_REPO


@pytest.mark.parametrize(
    "raw, expected_type, expected_norm, exp",
    [
        # GitHub
        (
            "888",
            GitHubIssue,
            f"{DEFAULT_GH_USER}/{DEFAULT_GH_REPO}#888",
            {"user": DEFAULT_GH_USER, "repo": DEFAULT_GH_REPO, "number": 888},
        ),
        (
            "#888",
            GitHubIssue,
            f"{DEFAULT_GH_USER}/{DEFAULT_GH_REPO}#888",
            {"user": DEFAULT_GH_USER, "repo": DEFAULT_GH_REPO, "number": 888},
        ),
        (
            "my-repo#888",
            GitHubIssue,
            f"{DEFAULT_GH_USER}/my-repo#888",
            {"user": DEFAULT_GH_USER, "repo": "my-repo", "number": 888},
        ),
        (
            "my_user/my_repo#888",
            GitHubIssue,
            "my_user/my_repo#888",
            {"user": "my_user", "repo": "my_repo", "number": 888},
        ),
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
