# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""Tests for .github/scripts/issues_cache.py, the referenced-only issue cache (SCT-1172)."""

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

from sdcm.utils.issues import CachedGitHubIssues

SCRIPT_PATH = Path(__file__).parents[2] / ".github" / "scripts" / "issues_cache.py"
_spec = importlib.util.spec_from_file_location("issues_cache", SCRIPT_PATH)
issues_cache = importlib.util.module_from_spec(_spec)
sys.modules["issues_cache"] = issues_cache
_spec.loader.exec_module(issues_cache)


@pytest.mark.parametrize(
    "value,short_forms,expected",
    [
        ("https://github.com/scylladb/scylladb/issues/123", False, "scylladb/scylladb#123"),
        ("https://github.com/scylladb/scylla-manager/pull/7", False, "scylladb/scylla-manager#7"),
        ("scylladb/scylla-enterprise#4246", False, "scylladb/scylla-enterprise#4246"),
        (" jira:scylladb-42 ", False, "jira:SCYLLADB-42"),
        ("https://scylladb.atlassian.net/browse/SCT-420", False, "jira:SCT-420"),
        ("1234", False, None),
        ("1234", True, "scylladb/scylladb#1234"),
        ("#1234", True, "scylladb/scylladb#1234"),
        ("scylla#55", True, "scylladb/scylla#55"),  # repo kept as written: readers key files by it
        ("see https://github.com/scylladb/scylladb/issues/1", False, None),  # not a whole-string ref
        ("hello", True, None),
    ],
)
def test_normalize(value, short_forms, expected):
    assert issues_cache.normalize(value, short_forms=short_forms) == expected


def test_extract_python_full_forms_anywhere_short_forms_only_in_calls():
    source = """
SKIP = ["https://github.com/scylladb/scylla-manager/issues/3829"]

def f(params):
    if SkipPerIssues("18180", params):
        pass
    decorate_with_context_if_issues_open(ctx, issue_refs=["scylla-enterprise#9"])
    other_call("1234")
    x = "jira:SCT-7"
"""
    assert issues_cache.extract_python(source, issues_cache.SCT_CALLS) == {
        "scylladb/scylla-manager#3829",
        "scylladb/scylladb#18180",
        "scylladb/scylla-enterprise#9",
        "jira:SCT-7",
    }


def test_extract_python_ignores_syntax_errors():
    assert issues_cache.extract_python("def (:", issues_cache.SCT_CALLS) == set()


def test_extract_yaml():
    source = """
reactor_stall:
  - keyword: "foo"
    issue: https://github.com/scylladb/scylladb/issues/14582
  - keyword: bar
    issue: 'jira:SCYLLADB-1'
"""
    assert issues_cache.extract_yaml(source) == {"scylladb/scylladb#14582", "jira:SCYLLADB-1"}


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def test_scan_writes_manifest_for_matching_branches_only(tmp_path):
    repo = tmp_path / "repo"
    (repo / "sdcm").mkdir(parents=True)
    (repo / "unit_tests").mkdir()
    (repo / "sdcm" / "a.py").write_text(
        'SkipPerIssues("scylladb/scylladb#1", p)\nURL = "https://github.com/pytest-dev/pytest/issues/5"\n'
    )
    (repo / "unit_tests" / "test_a.py").write_text('SkipPerIssues("scylladb/scylladb#2", p)\n')
    _git(repo, "init", "-q")
    _git(repo, "add", ".")
    _git(repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "init")
    _git(repo, "update-ref", "refs/remotes/origin/master", "HEAD")
    _git(repo, "update-ref", "refs/remotes/origin/feature-x", "HEAD")
    out = tmp_path / "refs" / "sct.json"

    issues_cache.main(["scan", "--repo-dir", str(repo), "--out", str(out)])

    manifest = json.loads(out.read_text())
    assert set(manifest) == {"version", "repo", "label", "label_description", "generated_at", "refs"}
    assert manifest["version"] == 1
    assert manifest["label"] == "used-by-sct"
    # unit_tests/ and third-party orgs are excluded; feature-x doesn't match the branch pattern
    assert manifest["refs"] == {"scylladb/scylladb#1": [{"branch": "master", "file": "sdcm/a.py"}]}


def test_csvs_are_readable_by_sct_reader(tmp_path, monkeypatch):
    states = {
        "scylladb/contract-repo#5": {
            "state": "open",
            "labels": ["sct-2026.1-skip", "P1"],
            "title": "a, b",
            "is_pr": False,
        },
        "scylladb/contract-repo#3": {"state": "merged", "labels": [], "title": 'say "hi"', "is_pr": True},
    }
    issues_cache.write_csvs(tmp_path, states)

    def get_file_contents(key):
        return (tmp_path / key.removeprefix("issues/")).read_bytes()

    cache = CachedGitHubIssues()
    monkeypatch.setattr(cache.storage, "get_file_contents", get_file_contents)
    issues = cache.get_repo("scylladb", "contract-repo")

    assert issues[5].state == "open"
    assert issues[5].labels == ["sct-2026.1-skip", "P1"]
    assert issues[5].title == "a, b"
    assert issues[3].state == "merged"
    assert (tmp_path / "pull-requests" / "scylladb_contract-repo.csv").read_text() == ""


@pytest.mark.parametrize(
    "union,previous,failed,ok",
    [(100, 0, 0, True), (60, 100, 0, True), (40, 100, 0, False), (100, 100, 21, False), (100, 100, 20, True)],
)
def test_guards(union, previous, failed, ok):
    if ok:
        issues_cache.check_guards(union, previous, failed)
    else:
        with pytest.raises(SystemExit):
            issues_cache.check_guards(union, previous, failed)


class FakeGitHub:
    def __init__(self, can_label: bool, comments: list | None = None):
        self.can_label, self.comments = can_label, comments or []
        self.writes: list[tuple] = []

    def request(self, method, path, body=None):
        if method == "GET" and path.startswith("/repos/") and path.count("/") == 3:
            return 200, {"permissions": {"triage": self.can_label}}
        if method == "GET" and path == "/user":
            return 200, {"login": "bot"}
        if method == "GET" and "/comments" in path:
            return 200, self.comments
        self.writes.append((method, path, body))
        return 201, {"id": 77}


LABEL_META = {
    "used-by-sct": {"repo": "scylladb/scylla-cluster-tests", "label_description": "SCT"},
    "used-by-dtest": {"repo": "scylladb/scylla-dtest", "label_description": "dtest"},
}


def test_reconcile_labels_add_and_remove_only_managed_labels():
    gh = FakeGitHub(can_label=True)
    rec = issues_cache.Reconciler(gh, "on", LABEL_META, {})

    rec.reconcile("scylladb/scylladb#1", {"used-by-sct": ["master"]}, ["used-by-dtest", "P1"])

    assert gh.writes == [
        ("POST", "/repos/scylladb/scylladb/issues/1/labels", {"labels": ["used-by-sct"]}),
        ("PATCH", "/repos/scylladb/scylladb/labels/used-by-sct", {"color": "c5def5", "description": "SCT"}),
        ("DELETE", "/repos/scylladb/scylladb/issues/1/labels/used-by-dtest", None),
    ]


def test_reconcile_dry_run_writes_nothing():
    gh = FakeGitHub(can_label=False)
    rec = issues_cache.Reconciler(gh, "dry-run", LABEL_META, {})

    rec.reconcile("scylladb/siren#1", {"used-by-sct": ["master"]}, [])

    assert gh.writes == []
    assert rec.plan == ["scylladb/siren#1: add marker comment"]
    assert rec.state == {}


def test_reconcile_one_comment_edited_in_place():
    gh = FakeGitHub(can_label=False)
    state = {}
    rec = issues_cache.Reconciler(gh, "on", LABEL_META, state)

    rec.reconcile("scylladb/siren#1", {"used-by-sct": ["master"]}, [])
    rec.reconcile("scylladb/siren#1", {"used-by-sct": ["master"]}, [])  # unchanged: no write
    rec.reconcile("scylladb/siren#1", {"used-by-sct": ["master"], "used-by-dtest": ["next"]}, [])
    rec.reconcile("scylladb/siren#1", {}, [])

    assert [(m, p) for m, p, _ in gh.writes] == [
        ("POST", "/repos/scylladb/siren/issues/1/comments"),
        ("PATCH", "/repos/scylladb/siren/issues/comments/77"),
        ("PATCH", "/repos/scylladb/siren/issues/comments/77"),
    ]
    assert "used-by-dtest" in gh.writes[1][2]["body"]
    assert "No longer used" in gh.writes[2][2]["body"]
    assert all(issues_cache.MARKER in body["body"] for _, _, body in gh.writes)


def test_reconcile_reuses_existing_marker_comment_when_state_is_lost():
    gh = FakeGitHub(
        can_label=False, comments=[{"id": 5, "body": f"{issues_cache.MARKER}\nold", "user": {"login": "bot"}}]
    )
    rec = issues_cache.Reconciler(gh, "on", LABEL_META, {})

    rec.reconcile("scylladb/siren#1", {"used-by-sct": ["master"]}, [])

    assert [(m, p) for m, p, _ in gh.writes] == [("PATCH", "/repos/scylladb/siren/issues/comments/5")]


def test_compare_reports_diffs_and_new_coverage(tmp_path, caplog):
    new, old = tmp_path / "new", tmp_path / "old"
    issues_cache.write_csvs(
        new,
        {
            "scylladb/r#1": {"state": "open", "labels": ["a"], "title": "t", "is_pr": False},
            "scylladb/r#2": {"state": "closed", "labels": [], "title": "t", "is_pr": False},
            "scylladb/r#3": {"state": "open", "labels": [], "title": "t", "is_pr": False},
        },
    )
    (old / "pull-requests").mkdir(parents=True)
    (old / "scylladb_r.csv").write_text("1,open,a,t, with comma\n2,open,,t\n")
    (old / "pull-requests" / "scylladb_r.csv").write_text("")

    with caplog.at_level("INFO"):
        issues_cache.main(["compare", "--new-dir", str(new), "--old-dir", str(old)])

    assert "1 rows match the old cache" in caplog.text
    assert "scylladb_r#2: old ('open', set()) != new ('closed', set())" in caplog.text
    assert "scylladb_r#3: not in the old cache" in caplog.text
