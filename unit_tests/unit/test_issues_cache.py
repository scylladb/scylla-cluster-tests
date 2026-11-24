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

"""Tests for utils/issues_cache, the referenced-only issue cache (SCT-1172)."""

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from sdcm.utils.issues import CachedGitHubIssues
from utils.issues_cache import github
from utils.issues_cache.__main__ import main
from utils.issues_cache.github import MARKER, Reconciler, check_guards, gh_api, write_csvs
from utils.issues_cache.refs import extract_python, extract_yaml, normalize


@pytest.mark.parametrize(
    "value,expected",
    [
        ("https://github.com/scylladb/scylladb/issues/123", "scylladb/scylladb#123"),
        ("https://github.com/scylladb/scylla-manager/pull/7", "scylladb/scylla-manager#7"),
        ("scylladb/scylla-enterprise#4246", "scylladb/scylla-enterprise#4246"),
        ("scylladb/scylla#55", "scylladb/scylla#55"),  # repo kept as written: readers key files by it
        (" jira:scylladb-42 ", "jira:SCYLLADB-42"),
        ("https://scylladb.atlassian.net/browse/SCT-420", "jira:SCT-420"),
        ("1234", None),  # bare numbers are not refs: no active branch uses them
        ("#1234", None),
        ("see https://github.com/scylladb/scylladb/issues/1", None),  # not a whole-string ref
        ("hello", None),
    ],
)
def test_normalize(value, expected):
    assert normalize(value) == expected


def test_extract_python_finds_full_forms_in_any_literal():
    source = """
SKIP = ["https://github.com/scylladb/scylla-manager/issues/3829"]

def f(params):
    if SkipPerIssues("scylladb/scylladb#18180", params):
        pass
    decorate_with_context_if_issues_open(ctx, issue_refs=["scylladb/scylla-enterprise#9"])
    other_call("1234")
    x = "jira:SCT-7"
"""
    assert extract_python(source) == (
        {"scylladb/scylla-manager#3829", "scylladb/scylladb#18180", "scylladb/scylla-enterprise#9", "jira:SCT-7"},
        [],
    )


def test_extract_python_reports_refs_built_at_runtime(caplog):
    source = """
SkipPerIssues(f"scylladb/scylladb#{n}", params)
SkipPerIssues("https://github.com/scylladb/scylladb/issues/" + str(n), params)
SkipPerIssues("jira:%s" % key, params)
SkipPerIssues("scylladb/scylladb#{}".format(n), params)
SkipPerIssues(f"jira:SCT-{n}", params)
SkipPerIssues("scylladb/scylladb#%(n)s" % v, params)
SkipPerIssues(f"https://github.com/scylladb/{repo}/issues/{n}", params)
log(f"node {name}: done", "%s nodes" % n, "{} up".format(n), "%5d%% done" % n)
"""
    assert extract_python(source, "master:a.py") == (set(), [2, 3, 4, 5, 6, 7, 8])
    assert "master:a.py:2: issue ref built at runtime" in caplog.text


def test_extract_python_warns_and_skips_syntax_errors(caplog):
    assert extract_python("def (:", "master:sdcm/broken.py") == (set(), [])
    assert "skipping master:sdcm/broken.py" in caplog.text


def test_extract_yaml():
    source = """
reactor_stall:
  - keyword: "foo"
    issue: https://github.com/scylladb/scylladb/issues/14582
  - keyword: bar
    issue: 'jira:SCYLLADB-1'
"""
    assert extract_yaml(source) == {"scylladb/scylladb#14582", "jira:SCYLLADB-1"}


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def test_scan_writes_manifest_for_matching_branches_only(tmp_path):
    repo = tmp_path / "repo"
    (repo / "sdcm").mkdir(parents=True)
    (repo / "unit_tests").mkdir()
    (repo / "sdcm" / "a.py").write_text(
        'SkipPerIssues("scylladb/scylladb#1", p)\nURL = "https://github.com/pytest-dev/pytest/issues/5"\n'
        'SkipPerIssues(f"scylladb/scylladb#{n}", p)\n'
    )
    (repo / "unit_tests" / "test_a.py").write_text('SkipPerIssues("scylladb/scylladb#2", p)\n')
    (repo / ".github").mkdir()
    (repo / ".github" / "labels.py").write_text('URL = f"https://api.github.com/repos/{repo}/issues/{n}/labels"\n')
    (repo / "utils" / "issues_cache").mkdir(parents=True)
    (repo / "utils" / "issues_cache" / "github.py").write_text('gh("GET", f"/repos/{owner}/{repo}/issues/{number}")\n')
    _git(repo, "init", "-q")
    _git(repo, "add", ".")
    _git(repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "init")
    _git(repo, "update-ref", "refs/remotes/origin/master", "HEAD")
    _git(repo, "update-ref", "refs/remotes/origin/feature-x", "HEAD")
    out = tmp_path / "refs" / "sct.json"

    main(["scan", "--repo-dir", str(repo), "--out", str(out)])

    manifest = json.loads(out.read_text())
    assert set(manifest) == {"version", "repo", "label", "label_description", "generated_at", "refs", "unresolved"}
    assert manifest["version"] == 1
    assert manifest["label"] == "used-by-sct"
    # unit_tests/, utils/issues_cache/, .github/ and third-party orgs are excluded; feature-x doesn't match the branch pattern
    assert manifest["refs"] == {"scylladb/scylladb#1": [{"branch": "master", "file": "sdcm/a.py"}]}
    assert manifest["unresolved"] == ["master:sdcm/a.py:3"]


def test_csvs_are_readable_by_sct_reader(tmp_path, monkeypatch):
    states = {
        "scylladb/contract-repo#5": {"state": "open", "labels": ["sct-2026.1-skip", "P1"], "title": "a, b"},
        "scylladb/contract-repo#3": {"state": "merged", "labels": [], "title": 'say "hi"'},
    }
    write_csvs(tmp_path, states)

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
        check_guards(union, previous, failed)
    else:
        with pytest.raises(SystemExit):
            check_guards(union, previous, failed)


def test_accept_shrink_skips_only_the_shrink_guard():
    check_guards(40, 100, 0, accept_shrink=True)
    with pytest.raises(SystemExit):
        check_guards(40, 100, 20, accept_shrink=True)


@pytest.mark.parametrize(
    "returncode,stdout,stderr,paginate,expected",
    [
        (0, '{"state": "open"}', "", False, (200, {"state": "open"})),
        (0, "", "", False, (200, None)),  # e.g. DELETE with an empty reply
        (0, '[[{"id": 1}], [{"id": 2}]]', "", True, (200, [{"id": 1}, {"id": 2}])),  # --slurp pages
        (1, "", "gh: Not Found (HTTP 404)", False, (404, None)),
        (1, "", "error connecting to api.github.com", False, (0, None)),
    ],
)
def test_gh_api_reads_status_and_flattens_pages(monkeypatch, returncode, stdout, stderr, paginate, expected):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append((cmd, kwargs.get("input")))
        return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)

    monkeypatch.setattr(github.subprocess, "run", fake_run)

    assert gh_api("POST", "/repos/o/r/issues/1/labels", {"labels": ["x"]}, paginate=paginate) == expected
    cmd, stdin = calls[0]
    assert cmd[:5] == ["gh", "api", "--method", "POST", "/repos/o/r/issues/1/labels"]
    assert ("--paginate" in cmd) is paginate
    assert json.loads(stdin) == {"labels": ["x"]}


class FakeGitHub:
    """Stands in for `gh_api`: answers the reads, records the writes."""

    def __init__(self, can_label: bool, comments: list | None = None, write_status: int = 201):
        self.can_label, self.comments, self.write_status = can_label, comments or [], write_status
        self.writes: list[tuple] = []

    def __call__(self, method, path, body=None, paginate=False):
        if method == "GET" and path.startswith("/repos/") and path.count("/") == 3:
            return 200, {"permissions": {"triage": self.can_label}}
        if method == "GET" and path == "/user":
            return 200, {"login": "bot"}
        if method == "GET" and path.endswith("/comments"):
            assert paginate, "comments must be read with --paginate"
            return 200, self.comments
        self.writes.append((method, path, body))
        return self.write_status, {"id": 77}


LABEL_META = {
    "used-by-sct": {"repo": "scylladb/scylla-cluster-tests", "label_description": "SCT"},
    "used-by-dtest": {"repo": "scylladb/scylla-dtest", "label_description": "dtest"},
}


def test_reconcile_labels_add_and_remove_only_managed_labels():
    gh = FakeGitHub(can_label=True)
    rec = Reconciler(gh, "on", LABEL_META, {})

    rec.reconcile("scylladb/scylladb#1", {"used-by-sct": ["master"]}, ["used-by-dtest", "P1"])

    assert gh.writes == [
        ("POST", "/repos/scylladb/scylladb/issues/1/labels", {"labels": ["used-by-sct"]}),
        ("PATCH", "/repos/scylladb/scylladb/labels/used-by-sct", {"color": "c5def5", "description": "SCT"}),
        ("DELETE", "/repos/scylladb/scylladb/issues/1/labels/used-by-dtest", None),
    ]


def test_reconcile_dry_run_writes_nothing():
    gh = FakeGitHub(can_label=False)
    rec = Reconciler(gh, "dry-run", LABEL_META, {})

    rec.reconcile("scylladb/siren#1", {"used-by-sct": ["master"]}, [])

    assert gh.writes == []
    assert rec.plan == ["scylladb/siren#1: add marker comment"]
    assert rec.state == {}


def test_reconcile_one_comment_edited_in_place():
    gh = FakeGitHub(can_label=False)
    state = {}
    rec = Reconciler(gh, "on", LABEL_META, state)

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
    assert all(MARKER in body["body"] for _, _, body in gh.writes)


def test_reconcile_reuses_existing_marker_comment_when_state_is_lost():
    comments = [{"id": i, "body": "chatter", "user": {"login": "someone"}} for i in range(150)]
    comments.append({"id": 999, "body": f"{MARKER}\nold", "user": {"login": "bot"}})
    gh = FakeGitHub(can_label=False, comments=comments)

    Reconciler(gh, "on", LABEL_META, {}).reconcile("scylladb/siren#1", {"used-by-sct": ["master"]}, [])

    assert [(m, p) for m, p, _ in gh.writes] == [("PATCH", "/repos/scylladb/siren/issues/comments/999")]


def test_reconcile_removes_labels_of_a_manifest_that_disappeared():
    gh = FakeGitHub(can_label=True)
    meta = {"used-by-sct": LABEL_META["used-by-sct"]}  # dtest's manifest is gone
    rec = Reconciler(gh, "on", meta, {}, managed={"used-by-dtest"})

    rec.reconcile("scylladb/scylladb#1", {}, ["used-by-dtest", "P1"])

    assert gh.writes == [("DELETE", "/repos/scylladb/scylladb/issues/1/labels/used-by-dtest", None)]


def test_fetch_states_records_the_canonical_repo():
    def fake_gh(method, path, body=None, paginate=False):
        return 200, {
            "state": "open",
            "labels": [],
            "title": "t",
            "repository_url": "https://api.github.com/repos/scylladb/scylladb",
        }

    states, failed = github.fetch_states(fake_gh, ["scylladb/scylla#5"])

    assert states["scylladb/scylla#5"]["repo"] == "scylladb/scylladb"
    assert failed == {}


def test_reconcile_failed_comment_is_retried_next_run():
    state = {}
    Reconciler(FakeGitHub(can_label=False, write_status=403), "on", LABEL_META, state).reconcile(
        "scylladb/siren#1", {"used-by-sct": ["master"]}, []
    )
    assert state == {}

    gh = FakeGitHub(can_label=False)
    Reconciler(gh, "on", LABEL_META, state).reconcile("scylladb/siren#1", {"used-by-sct": ["master"]}, [])
    assert [(m, p) for m, p, _ in gh.writes] == [("POST", "/repos/scylladb/siren/issues/1/comments")]
    assert state["scylladb/siren#1"]["comment_id"] == 77


def _refresh_runner(tmp_path, monkeypatch, previous_refs, referenced=("scylladb/scylladb#1",)):
    """Run `refresh-github` against a fake GitHub; the manifest references `referenced` (#1)."""
    gh = SimpleNamespace(
        down=set(), down_status=502, write_status=200, writes=[], repo_status=200, labels=["used-by-sct"]
    )

    def fake_gh(method, path, body=None, paginate=False):
        if method == "GET" and path == "/repos/scylladb/scylladb":
            return gh.repo_status, {"permissions": {"triage": True}} if gh.repo_status == 200 else None
        if method == "GET" and path.rsplit("/", 1)[1] in gh.down:
            return gh.down_status, None
        if method == "GET":
            return 200, {
                "state": "open",
                "labels": [{"name": label} for label in gh.labels],
                "title": "t",
                "repository_url": "https://api.github.com/repos/scylladb/scylladb",
            }
        gh.writes.append((method, path))
        return gh.write_status, {}

    monkeypatch.setattr("utils.issues_cache.__main__.gh_api", fake_gh)
    (tmp_path / "refs").mkdir()
    manifest = {"version": 1, "repo": "scylladb/scylla-cluster-tests", "label": "used-by-sct"}
    manifest |= {
        "label_description": "SCT",
        "refs": {ref: [{"branch": "master", "file": "a.py"}] for ref in referenced},
    }
    (tmp_path / "refs" / "sct.json").write_text(json.dumps(manifest))
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"refs": {ref: ["used-by-sct"] for ref in previous_refs}}))

    def run(*extra, labels="on"):
        gh.writes.clear()
        argv = ["refresh-github", "--refs-dir", str(tmp_path / "refs"), "--out-dir", str(tmp_path / "out")]
        main([*argv, "--state", str(state), "--labels", labels, *extra])
        return json.loads(state.read_text())

    return gh, run


DROPPED_DELETE = ("DELETE", "/repos/scylladb/scylladb/issues/2/labels/used-by-sct")


@pytest.mark.parametrize("first_run", ["fetch fails", "removal fails", "dry-run"])
def test_refresh_keeps_a_dropped_ref_until_its_marker_is_removed(tmp_path, monkeypatch, first_run):
    gh, run = _refresh_runner(tmp_path, monkeypatch, ["scylladb/scylladb#1", "scylladb/scylladb#2"])
    if first_run == "fetch fails":
        gh.down = {"2"}
    elif first_run == "removal fails":
        gh.write_status = 500

    state = run(labels="dry-run" if first_run == "dry-run" else "on")
    assert list(state["refs"]) == ["scylladb/scylladb#1"]
    assert list(state["retry"]) == ["scylladb/scylladb#2"]

    gh.down, gh.write_status = set(), 200
    state = run()
    assert gh.writes == [DROPPED_DELETE]
    assert state["retry"] == {}


@pytest.mark.parametrize("status", [404, 410])
def test_refresh_forgets_a_dropped_ref_that_is_gone(tmp_path, monkeypatch, status):
    """A deleted or no-longer-visible item has no marker we could remove: don't refetch it forever."""
    gh, run = _refresh_runner(tmp_path, monkeypatch, ["scylladb/scylladb#1", "scylladb/scylladb#2"])
    gh.down, gh.down_status = {"2"}, status

    state = run(labels="dry-run")

    assert state["retry"] == {}
    assert gh.writes == []


def test_retried_refs_dont_count_for_the_shrink_guard(tmp_path, monkeypatch):
    """After an accepted shrink, unreachable dropped refs must not block the next run."""
    previous = [f"scylladb/scylladb#{n}" for n in range(1, 6)]
    gh, run = _refresh_runner(tmp_path, monkeypatch, previous)
    gh.down = {"2", "3", "4", "5"}

    run("--accept-shrink")
    state = run()  # no --accept-shrink: 1 ref now vs 1 ref last run

    assert list(state["refs"]) == ["scylladb/scylladb#1"]
    assert sorted(state["retry"]) == previous[1:]


def test_refresh_a_failed_fetch_keeps_the_last_good_row(tmp_path, monkeypatch):
    """One 5xx must not drop a row, or the whole CSV of a repo with a single ref."""
    refs = [f"scylladb/scylladb#{n}" for n in range(1, 6)]
    gh, run = _refresh_runner(tmp_path, monkeypatch, refs, referenced=refs)
    run(labels="off")
    gh.down = {"1"}  # 1 of 5 failed: under the 20% guard, so the refresh writes the CSVs

    state = run(labels="off")

    assert state["rows"]["scylladb/scylladb#1"]["state"] == "open"
    assert (tmp_path / "out" / "scylladb_scylladb.csv").read_text().splitlines()[0] == "1,open,used-by-sct,t"


def test_refresh_drops_a_row_carried_for_too_long(tmp_path, monkeypatch, caplog):
    """A deleted issue or a repo the token lost must not keep its last state forever."""
    refs = [f"scylladb/scylladb#{n}" for n in range(1, 6)]
    gh, run = _refresh_runner(tmp_path, monkeypatch, refs, referenced=refs)
    run(labels="off")
    gh.down = {"1"}

    with caplog.at_level("INFO"):
        for _ in range(github.CARRY_LIMIT):
            state = run(labels="off")
    assert state["carried"] == {"scylladb/scylladb#1": github.CARRY_LIMIT}
    assert f"scylladb/scylladb#1: fetch failed, last good row kept ({github.CARRY_LIMIT} runs)" in caplog.text

    state = run(labels="off")
    assert "scylladb/scylladb#1" not in state["rows"]
    assert state["carried"] == {}


def test_refresh_reconciles_the_aliases_of_one_issue_together(tmp_path, monkeypatch):
    """`scylladb/scylla#5` and `scylladb/scylladb#5` are one issue: neither may remove the other's label."""
    gh, run = _refresh_runner(tmp_path, monkeypatch, [], referenced=("scylladb/scylla#5",))
    dtest = {"version": 1, "repo": "scylladb/scylla-dtest", "label": "used-by-dtest", "label_description": "dtest"}
    dtest["refs"] = {"scylladb/scylladb#5": [{"branch": "next", "file": "t.py"}]}
    (tmp_path / "refs" / "dtest.json").write_text(json.dumps(dtest))
    gh.labels = ["used-by-dtest"]

    run()

    # the write goes to the canonical repo: GitHub answers writes on a renamed repo with a 307
    assert gh.writes == [
        ("POST", "/repos/scylladb/scylladb/issues/5/labels"),
        ("PATCH", "/repos/scylladb/scylladb/labels/used-by-sct"),
    ]
    gh.labels = ["used-by-dtest", "used-by-sct"]
    run()
    assert gh.writes == []


def test_refresh_skips_a_repo_whose_permissions_cant_be_read(tmp_path, monkeypatch):
    """A 5xx on GET /repos/... must not fall back to a marker comment on every item."""
    gh, run = _refresh_runner(tmp_path, monkeypatch, ["scylladb/scylladb#1", "scylladb/scylladb#2"])
    gh.repo_status = 502

    state = run()

    assert gh.writes == []
    assert list(state["retry"]) == ["scylladb/scylladb#2"]  # its label removal is retried next run
    assert state.get("comments", {}) == {}


def test_compare_reports_diffs_and_new_coverage(tmp_path, caplog):
    new, old = tmp_path / "new", tmp_path / "old"
    write_csvs(
        new,
        {
            "scylladb/r#1": {"state": "open", "labels": ["a"], "title": "t"},
            "scylladb/r#2": {"state": "closed", "labels": [], "title": "t"},
            "scylladb/r#3": {"state": "open", "labels": [], "title": "t"},
        },
    )
    (old / "pull-requests").mkdir(parents=True)
    (old / "scylladb_r.csv").write_text("1,open,a,t, with comma\n2,open,,t\n")
    (old / "pull-requests" / "scylladb_r.csv").write_text("")
    (tmp_path / "refs").mkdir()
    manifest = {"version": 1, "repo": "scylladb/scylla-cluster-tests", "label": "used-by-sct"}
    manifest["refs"] = {f"scylladb/r#{n}": [] for n in (1, 2, 3, 4)}
    manifest["unresolved"] = ["master:sdcm/a.py:3"]
    (tmp_path / "refs" / "sct.json").write_text(json.dumps(manifest))
    argv = ["compare", "--new-dir", str(new), "--old-dir", str(old), "--refs-dir", str(tmp_path / "refs")]

    with caplog.at_level("INFO"), pytest.raises(SystemExit):
        main(argv)

    assert "1 rows match the old cache, 3 problems" in caplog.text
    assert "scylladb_r#2: old ('open', set()) != new ('closed', set())" in caplog.text
    assert "scylladb/r#4: referenced, but not in the new cache" in caplog.text
    assert "issue ref built at runtime: master:sdcm/a.py:3" in caplog.text
    assert "scylladb_r#3: not in the old cache" in caplog.text


def test_compare_passes_on_new_coverage_only(tmp_path):
    new, old = tmp_path / "new", tmp_path / "old"
    write_csvs(new, {"scylladb/r#1": {"state": "open", "labels": [], "title": "t"}})
    old.mkdir()
    (tmp_path / "refs").mkdir()
    manifest = {"version": 1, "repo": "scylladb/scylla-dtest", "label": "used-by-dtest", "refs": {"scylladb/r#1": []}}
    (tmp_path / "refs" / "dtest.json").write_text(json.dumps(manifest))

    main(["compare", "--new-dir", str(new), "--old-dir", str(old), "--refs-dir", str(tmp_path / "refs")])
