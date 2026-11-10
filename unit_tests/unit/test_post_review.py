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

"""Tests for the review gate used by .github/workflows/claude-review.yml.

Each test runs the script as the workflow does, with a fake ``gh`` on ``PATH``
that returns the PR files and records the review that would be posted.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).parents[2] / ".github" / "scripts" / "post_review.py"

PATCH = """@@ -10,3 +10,4 @@ def f():
 context10
-removed
+added11
+added12
 context13
@@ -40,1 +41,1 @@
-old
+new41"""

FAKE_GH = """#!/bin/sh
case "$*" in
  */files*) cat "$FAKE_GH_DIR/files.json" ;;
  */reviews*) cat > "$FAKE_GH_DIR/posted.json"; echo https://github.com/o/r/pull/1#pullrequestreview-1 ;;
  *) echo "unexpected gh call: $*" >&2; exit 1 ;;
esac
"""


@pytest.fixture
def post_review(tmp_path):
    """Run post_review.py on a review dict; return the script output and the posted review (or None)."""
    (tmp_path / "gh").write_text(FAKE_GH)
    (tmp_path / "gh").chmod(0o755)
    # gh api --paginate --slurp returns a list of pages.
    (tmp_path / "files.json").write_text(json.dumps([[{"filename": "a.py", "patch": PATCH}]]))
    env = os.environ | {"PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}", "FAKE_GH_DIR": str(tmp_path)}

    def run(review: dict, *extra: str) -> tuple[str, dict | None]:
        review_file = tmp_path / "review.json"
        review_file.write_text(json.dumps(review))
        result = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "--repo", "o/r", "--pr", "1", "--head-sha", "abc123",
             "--review-file", str(review_file), *extra],
            env=env, capture_output=True, text=True, check=True,
        )  # fmt: skip
        posted = tmp_path / "posted.json"
        return result.stdout, json.loads(posted.read_text()) if posted.exists() else None

    return run


def test_posts_a_comment_review_pinned_to_the_reviewed_commit(post_review):
    output, posted = post_review({"event": "APPROVE", "commit_id": "other", "body": "LGTM", "comments": []})

    assert posted == {"commit_id": "abc123", "event": "COMMENT", "body": "LGTM", "comments": []}
    assert "posted 0 inline comments" in output


def test_moves_comments_outside_the_diff_to_the_body(post_review):
    _, posted = post_review({
        "body": "Summary.",
        "comments": [
            {"path": "a.py", "line": 11, "body": "🔧 **important:** added line"},
            {"path": "a.py", "line": 41, "body": "⛏ **nit:** second hunk"},
            {"path": "a.py", "line": 14, "body": "❓ **question:** after the hunk"},
            {"path": "a.py", "start_line": 9, "line": 11, "body": "⛏ **nit:** range starts outside"},
            {"path": "a.py", "start_line": 13, "line": 11, "body": "⛏ **nit:** reversed range"},
            {"path": "a.py", "start_line": None, "line": 12, "body": "📝 **note:** null start"},
            {"path": "b.py", "line": 1, "body": "📝 **note:** file not in the PR"},
            {"path": "a.py", "line": 12, "body": "  "},
        ],
    })  # fmt: skip

    assert [(c["line"], c["side"]) for c in posted["comments"]] == [(11, "RIGHT"), (41, "RIGHT"), (12, "RIGHT")]
    assert posted["body"].startswith("Summary.\n\n**Findings outside the diff:**")
    for outside in ("`a.py:14` ❓", "`a.py:11` ⛏ **nit:** range starts outside", "reversed range", "`b.py:1` 📝"):
        assert outside in posted["body"]


def test_keeps_a_multi_line_range_inside_the_diff(post_review):
    _, posted = post_review({"comments": [{"path": "a.py", "start_line": 10, "line": 13, "body": "♻️ **refactor:** x"}]})

    assert posted["comments"] == [
        {
            "path": "a.py",
            "line": 13,
            "side": "RIGHT",
            "body": "♻️ **refactor:** x",
            "start_line": 10,
            "start_side": "RIGHT",
        }
    ]
    assert posted["body"] == "No findings."


@pytest.mark.parametrize("secret", ["sk-ant-api03-abcdefgh1234", "ghs_" + "a" * 36])
def test_refuses_to_post_a_review_that_holds_a_secret(post_review, secret):
    with pytest.raises(subprocess.CalledProcessError) as error:
        post_review({"body": "LGTM", "comments": [{"path": "a.py", "line": 11, "body": f"key: {secret}"}]})

    assert "refusing to post" in error.value.stderr


def test_dry_run_prints_the_review_and_posts_nothing(post_review):
    output, posted = post_review({"body": "LGTM"}, "--dry-run")

    assert posted is None
    assert json.loads(output)["event"] == "COMMENT"
