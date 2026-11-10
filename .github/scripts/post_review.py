#!/usr/bin/env python3
"""Check the review that Claude wrote, then post it as one pull request review.

Used by .github/workflows/claude-review.yml. Claude has no write access; this
step is the only one that posts. It always posts with the event COMMENT, pins
the review to the head commit that was reviewed, and moves each inline comment
whose line is not in the diff into the review body, because GitHub rejects the
whole review for one such comment.

Usage:
    post_review.py --repo OWNER/REPO --pr N --head-sha SHA --review-file review.json [--dry-run]
"""

import argparse
import json
import re
import subprocess
import sys

BODY_LIMIT = 60000  # GitHub rejects review bodies above 65536 characters.
HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")
# An Anthropic API key, or a GitHub token. A prompt injection could make Claude copy one into the review.
SECRET = re.compile(r"sk-ant-[\w-]{8,}|gh[pousr]_\w{20,}|github_pat_\w{20,}")


def diff_lines(patch: str) -> set[int]:
    """Return the new-file line numbers that a patch shows: the lines a RIGHT comment can anchor on."""
    lines, line = set(), 0
    for row in patch.splitlines():
        if match := HUNK.match(row):
            line = int(match.group(1))
        elif row.startswith(("-", "\\")):
            continue
        elif line:
            lines.add(line)
            line += 1
    return lines


def sanitize(review: dict, head_sha: str, anchors: dict[str, set[int]]) -> dict:
    """Return the review to post: event COMMENT, pinned commit, inline comments only on diff lines."""
    body = str(review.get("body") or "").strip()
    comments, outside = [], []
    for comment in review.get("comments") or []:
        path, line = comment.get("path"), comment.get("line")
        start = comment.get("start_line") or line
        text = str(comment.get("body") or "").strip()
        if not text:
            continue
        # GitHub rejects the whole review (422) for a range that ends before it starts.
        in_diff = isinstance(line, int) and isinstance(start, int) and {start, line} <= anchors.get(path, set())
        if in_diff and start <= line:
            kept = {"path": path, "line": line, "side": "RIGHT", "body": text}
            if start != line:
                kept |= {"start_line": start, "start_side": "RIGHT"}
            comments.append(kept)
        else:
            outside.append(f"- `{path}:{line}` {text}")
    if outside:
        body += "\n\n**Findings outside the diff:**\n" + "\n".join(outside)
    if len(body) > BODY_LIMIT:
        body = body[:BODY_LIMIT] + "\n\n(cut at the GitHub size limit)"
    return {"commit_id": head_sha, "event": "COMMENT", "body": body or "No findings.", "comments": comments}


def gh(*args: str, stdin: str | None = None) -> str:
    return subprocess.run(["gh", *args], input=stdin, capture_output=True, text=True, check=True).stdout


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repo", required=True)
    parser.add_argument("--pr", required=True, type=int)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--review-file", required=True)
    parser.add_argument("--dry-run", action="store_true", help="print the review instead of posting it")
    args = parser.parse_args()

    with open(args.review_file, encoding="utf-8") as review_file:
        review = json.load(review_file)
    files = json.loads(gh("api", "--paginate", "--slurp", f"repos/{args.repo}/pulls/{args.pr}/files"))
    anchors = {f["filename"]: diff_lines(f.get("patch", "")) for page in files for f in page}
    payload = sanitize(review, args.head_sha, anchors)
    if SECRET.search(json.dumps(payload)):
        print("refusing to post: the review holds something that looks like a key or a token", file=sys.stderr)
        return 1
    if args.dry_run:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0
    url = gh(
        "api",
        f"repos/{args.repo}/pulls/{args.pr}/reviews",
        "-X",
        "POST",
        "--input",
        "-",
        "--jq",
        ".html_url",
        stdin=json.dumps(payload),
    )
    print(f"posted {len(payload['comments'])} inline comments: {url.strip()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
