#!/usr/bin/env python3
"""Acted-on rate of the review threads of each AI reviewer on SCT pull requests.

A thread counts as acted on when it is resolved, outdated by a later commit, or has a
human reply. The spec in docs/plans/ai-tooling/review-tools/ uses this rate to pick the
reviewer that runs automatically.

Usage:
    python scripts/review_analysis/bot_threads.py --since 2026-06-01 [--until 2026-11-08]

The GitHub search API returns at most 1000 pull requests per query.
"""

import argparse
import json
import subprocess
from collections import defaultdict

REPO = "scylladb/scylla-cluster-tests"
# The Claude review posts as github-actions, like other workflows, so it is not counted.
BOTS = {"coderabbitai": "coderabbit", "copilot-pull-request-reviewer": "copilot"}
QODO_PREFIX = "qodo"
SEARCH_LIMIT = 1000

QUERY = """
query($q: String!, $after: String) {
  search(query: $q, type: ISSUE, first: 50, after: $after) {
    issueCount
    pageInfo { hasNextPage endCursor }
    nodes { ... on PullRequest {
      number
      reviewThreads(first: 100) { pageInfo { hasNextPage } nodes {
        isResolved isOutdated
        comments(first: 20) { pageInfo { hasNextPage } nodes { author { login __typename } } }
      } }
      reviews(first: 50) { pageInfo { hasNextPage } nodes { author { login } } }
    } }
  }
}
"""


def search_page(query: str, after: str | None) -> dict:
    args = ["gh", "api", "graphql", "-f", f"query={QUERY}", "-f", f"q={query}"]
    if after:
        args += ["-f", f"after={after}"]
    return json.loads(subprocess.check_output(args))["data"]["search"]


def bot_of(login: str | None) -> str | None:
    if not login:
        return None
    if login in BOTS:
        return BOTS[login]
    if login.startswith(QODO_PREFIX):
        return "qodo"
    return None


def collect(query: str) -> tuple[int, dict, set]:
    stats = defaultdict(lambda: {"prs": set(), "threads": 0, "acted": 0})
    after, total_prs, truncated = None, 0, set()
    while True:
        page = search_page(query, after)
        if after is None and page["issueCount"] > SEARCH_LIMIT:
            print(f"warning: {page['issueCount']} pull requests match; the search returns the newest {SEARCH_LIMIT}")
        for pr in filter(None, page["nodes"]):
            total_prs += 1
            if pr["reviews"]["pageInfo"]["hasNextPage"] or pr["reviewThreads"]["pageInfo"]["hasNextPage"]:
                truncated.add(pr["number"])
            for review in pr["reviews"]["nodes"]:
                if bot := bot_of((review["author"] or {}).get("login")):
                    stats[bot]["prs"].add(pr["number"])
            for thread in pr["reviewThreads"]["nodes"]:
                if thread["comments"]["pageInfo"]["hasNextPage"]:
                    truncated.add(pr["number"])
                authors = [c["author"] or {} for c in thread["comments"]["nodes"]]
                bot = bot_of(authors[0].get("login")) if authors else None
                if not bot:
                    continue
                # Other bots and deleted accounts are not a human reply.
                human_reply = any(a.get("__typename") == "User" for a in authors[1:])
                stats[bot]["prs"].add(pr["number"])
                stats[bot]["threads"] += 1
                stats[bot]["acted"] += thread["isResolved"] or thread["isOutdated"] or human_reply
        if not page["pageInfo"]["hasNextPage"]:
            return total_prs, stats, truncated
        after = page["pageInfo"]["endCursor"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--since", required=True, help="first PR creation date, YYYY-MM-DD")
    parser.add_argument("--until", default="*", help="last PR creation date, YYYY-MM-DD")
    args = parser.parse_args()

    total_prs, stats, truncated = collect(f"repo:{REPO} is:pr created:{args.since}..{args.until}")
    print(f"pull requests scanned: {total_prs}")
    if truncated:
        print(f"warning: threads, replies, or reviews cut at the query limits on PRs {sorted(truncated)}")
    for bot, s in sorted(stats.items()):
        prs = len(s["prs"])
        print(
            f"{bot}: prs={prs} threads={s['threads']} threads_per_pr={s['threads'] / max(prs, 1):.1f} "
            f"acted={s['acted']} ({100 * s['acted'] / max(s['threads'], 1):.0f}%)"
        )


if __name__ == "__main__":
    main()
