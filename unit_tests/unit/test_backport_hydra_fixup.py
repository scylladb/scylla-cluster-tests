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

"""Tests for the backport hydra fixup used by .github/workflows/backport-hydra-fixup.yaml.

The script lives under ``.github/scripts``, which is not an importable
package, so it is loaded by path.
"""

import importlib.util
import subprocess
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).parents[2] / ".github" / "scripts" / "backport_hydra_fixup.py"

_spec = importlib.util.spec_from_file_location("backport_hydra_fixup", SCRIPT_PATH)
fixup = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fixup)


def lock(**packages):
    return "version = 1\n" + "".join(
        f'\n[[package]]\nname = "{name}"\nversion = "{version}"\n' for name, version in packages.items()
    )


def test_lock_bumps_lists_changed_and_added_packages():
    old = lock(boto3="1.43.0", awscli="1.45.0", attrs="25.1.0")
    new = lock(boto3="1.43.28", awscli="1.45.28", attrs="25.1.0", s3transfer="0.18.0")

    assert fixup.lock_bumps(old, new) == ["awscli==1.45.28", "boto3==1.43.28", "s3transfer==0.18.0"]


def test_lock_bumps_ignores_removed_packages():
    assert fixup.lock_bumps(lock(a="1", b="2"), lock(a="1")) == []


def pr(number=1, head="backport/100/to-2026.1", files=("uv.lock",), labels=(), comments=()):
    return {
        "number": number,
        "headRefName": head,
        "files": [{"path": f} for f in files],
        "labels": [{"name": n} for n in labels],
        "comments": [{"body": b} for b in comments],
    }


@pytest.mark.parametrize(
    "skipped",
    [
        pr(head="renovate/awscli"),
        pr(files=("sdcm/cluster.py",)),
        pr(labels=("New Hydra Version",)),
        pr(comments=(f"{fixup.MARKER}\nDropped ...",)),
    ],
    ids=["not-a-backport", "no-image-input", "already-labeled", "already-handled"],
)
def test_candidates_skips(skipped):
    assert fixup.candidates([skipped]) == []


def test_candidates_picks_unhandled_backport_and_only_pr_reruns_handled_ones():
    fresh = pr(number=1, files=("pyproject.toml", "uv.lock"))
    handled = pr(number=2, labels=("New Hydra Version",))

    assert fixup.candidates([fresh, handled]) == [fresh]
    assert fixup.candidates([fresh, handled], only_pr=2) == [handled]


def test_scan_keeps_going_after_a_failed_pr_and_fails_at_the_end(monkeypatch):
    prs = [pr(number=1), pr(number=2)]
    seen = []

    def process(p, *_):
        seen.append(p["number"])
        if p["number"] == 1:
            raise RuntimeError("git push --force-with-lease failed: stale info")

    monkeypatch.setattr(fixup, "gh", lambda *a, **kw: fixup.json.dumps(prs))
    monkeypatch.setattr(fixup, "run", lambda *a, **kw: "")
    monkeypatch.setattr(fixup, "uv_version", lambda: "0.12.23")
    monkeypatch.setattr(fixup, "process", process)
    args = fixup.argparse.Namespace(bot="scylladbbot", repo="r/r", pr=None, dry_run=False, push_url=None)

    with pytest.raises(SystemExit, match=r"failed on \[1\]"):
        fixup.scan(args)
    assert seen == [1, 2]


def test_edit_todo_drops_hydra_and_relocks_after_each_commit(tmp_path):
    todo = tmp_path / "git-rebase-todo"
    todo.write_text(
        "pick 1111111 fix(deps): update awscli\n"
        "pick 2222222 chore(hydra): create image 1.146-PR15937-a744fac\n"
        "\n# Rebase abc..def onto abc\n"
    )

    fixup.edit_todo(str(todo), "/x/fixup.py")

    lines = todo.read_text().splitlines()
    assert lines[0] == "pick 1111111 fix(deps): update awscli"
    assert lines[1].endswith("/x/fixup.py relock")
    assert len(lines) == 2


def git(repo, *args):
    return subprocess.run(["git", *args], cwd=repo, check=True, text=True, capture_output=True).stdout


def commit(repo, message, **files):
    for name, content in files.items():
        path = repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", message)


def test_fix_branch_drops_conflicted_hydra_commit(tmp_path, monkeypatch):
    """The #14337 shape: a dependency commit, then master's image commit with conflict markers."""
    monkeypatch.setenv("GIT_AUTHOR_NAME", "t")
    monkeypatch.setenv("GIT_AUTHOR_EMAIL", "t@t")
    monkeypatch.setenv("GIT_COMMITTER_NAME", "t")
    monkeypatch.setenv("GIT_COMMITTER_EMAIL", "t@t")
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "branch-2026.1")
    commit(repo, "release tip", **{"docker/env/version": "1.131-PR1-abc\n", "pyproject.toml": "deps = []\n"})
    git(repo, "checkout", "-q", "-b", "backport/14318/to-2026.1")
    commit(repo, "fix(deps): update scylla-driver", **{"pyproject.toml": "deps = ['scylla-driver']\n"})
    commit(
        repo,
        "chore(hydra): create image 1.146-PR15937-a744fac",
        **{"docker/env/version": "<<<<<<< HEAD\n1.131-PR1-abc\n=======\n1.146-PR15937-a744fac\n>>>>>>> a744fac\n"},
    )

    _, dropped, failures, markers = fixup.fix_branch(str(repo), "branch-2026.1")

    assert [d.split(" ", 1)[1] for d in dropped] == ["chore(hydra): create image 1.146-PR15937-a744fac"]
    assert failures == []
    assert markers == []
    assert git(repo, "log", "--format=%s", "branch-2026.1..HEAD").splitlines() == ["fix(deps): update scylla-driver"]
    assert (repo / "docker/env/version").read_text() == "1.131-PR1-abc\n"


def test_fix_branch_reports_lock_added_to_branch_without_one(tmp_path, monkeypatch):
    """The #16383 shape: branch-2025.1 has no uv.lock, and the backported commit adds one."""
    for var in ("GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL", "GIT_COMMITTER_NAME", "GIT_COMMITTER_EMAIL"):
        monkeypatch.setenv(var, "t@t")
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "branch-2025.1")
    commit(repo, "release tip", **{"docker/env/version": "1.120-PR1-abc\n"})
    git(repo, "checkout", "-q", "-b", "backport/16373/to-2025.1")
    commit(
        repo,
        "chore(deps): rewrite uv.lock\n\n(cherry picked from commit 90725d77e8)",
        **{"uv.lock": lock(attrs="25.1.0")},
    )
    commit(
        repo, "chore(hydra): create image 1.148-PR16373-0ba2bb1", **{"docker/env/version": "1.148-PR16373-0ba2bb1\n"}
    )

    _, dropped, failures, markers = fixup.fix_branch(str(repo), "branch-2025.1")

    assert len(dropped) == 1
    assert failures == [
        "chore(deps): rewrite uv.lock: the release branch has no uv.lock, but this commit adds one; drop it from the commit by hand"
    ]
    assert markers == []


def test_relock_crash_restores_uv_lock_and_records_why(tmp_path, monkeypatch):
    """A crash after the release lock was written must leave a clean tree, so the rebase goes on."""
    for var in ("GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL", "GIT_COMMITTER_NAME", "GIT_COMMITTER_EMAIL"):
        monkeypatch.setenv(var, "t@t")
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "branch-2026.1")
    commit(repo, "release tip", **{"uv.lock": lock(attrs="25.1.0")})
    commit(repo, "fix(deps): bump attrs", **{"uv.lock": lock(attrs="25.3.0")})
    failed = tmp_path / "failed"
    monkeypatch.setenv(fixup.FAILED_FILE_ENV, str(failed))
    monkeypatch.chdir(repo)

    def crash_half_way():
        (repo / "uv.lock").write_text(lock(attrs="25.1.0"))
        raise FileNotFoundError("uvx")

    monkeypatch.setattr(fixup, "_relock", crash_half_way)

    fixup.relock()

    assert git(repo, "status", "--porcelain") == ""
    assert failed.read_text() == "fix(deps): bump attrs: re-lock crashed: uvx\n"
