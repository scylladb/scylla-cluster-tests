"""Run scripts/review_analysis/bot_threads.py with a fake gh and check the rates it prints."""

import copy
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).parents[2] / "scripts" / "review_analysis" / "bot_threads.py"


def thread(*authors, resolved=False, outdated=False):
    return {
        "isResolved": resolved,
        "isOutdated": outdated,
        "comments": {
            "pageInfo": {"hasNextPage": False},
            "nodes": [{"author": {"login": login, "__typename": kind}} for login, kind in authors],
        },
    }


PAGE = {
    "data": {
        "search": {
            "issueCount": 1,
            "pageInfo": {"hasNextPage": False, "endCursor": None},
            "nodes": [
                {
                    "number": 1,
                    "reviews": {"pageInfo": {"hasNextPage": False}, "nodes": [{"author": {"login": "coderabbitai"}}]},
                    "reviewThreads": {
                        "pageInfo": {"hasNextPage": False},
                        "nodes": [
                            # Another bot's reply is not a human reply.
                            thread(("coderabbitai", "Bot"), ("github-actions", "Bot")),
                            thread(("coderabbitai", "Bot"), ("someone", "User")),
                            thread(("copilot-pull-request-reviewer", "Bot"), resolved=True),
                            thread(("qodo-merge-pro", "Bot"), outdated=True),
                            # A thread a human starts is not counted.
                            thread(("someone", "User"), ("coderabbitai", "Bot")),
                        ],
                    },
                }
            ],
        }
    }
}


@pytest.fixture
def bot_threads(tmp_path):
    fake_gh = tmp_path / "gh"
    fake_gh.write_text(f'#!/bin/sh\ncat "{tmp_path / "page.json"}"\n')
    fake_gh.chmod(0o755)
    env = {**os.environ, "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}"}

    def run(page=PAGE):
        (tmp_path / "page.json").write_text(json.dumps(page))
        return subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "--since", "2026-01-01"],
            env=env,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()

    return run


def test_acted_on_rate_per_reviewer(bot_threads):
    lines = bot_threads()

    assert "pull requests scanned: 1" in lines
    assert "coderabbit: prs=1 threads=2 threads_per_pr=2.0 acted=1 (50%)" in lines
    assert "copilot: prs=1 threads=1 threads_per_pr=1.0 acted=1 (100%)" in lines
    assert "qodo: prs=1 threads=1 threads_per_pr=1.0 acted=1 (100%)" in lines


def _page_over_search_limit():
    page = copy.deepcopy(PAGE)
    page["data"]["search"]["issueCount"] = 2019
    return page


def _page_with_cut_threads():
    page = copy.deepcopy(PAGE)
    page["data"]["search"]["nodes"][0]["reviewThreads"]["pageInfo"]["hasNextPage"] = True
    return page


@pytest.mark.parametrize(
    "page,warning",
    [
        pytest.param(
            _page_over_search_limit(),
            "warning: 2019 pull requests match; the search returns the newest 1000",
            id="search-limit",
        ),
        pytest.param(
            _page_with_cut_threads(),
            "warning: threads, replies, or reviews cut at the query limits on PRs [1]",
            id="cut-threads",
        ),
    ],
)
def test_warns_when_counts_are_incomplete(bot_threads, page, warning):
    assert warning in bot_threads(page)
