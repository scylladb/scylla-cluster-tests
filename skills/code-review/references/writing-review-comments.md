# Writing Review Comments

How to write the findings of an SCT review: the tags, the comment layout,
and the review summary. `REVIEW.md` decides what to check and how many
findings to report. This file decides how each finding reads.

## Tags

Each inline comment starts with an emoji and a bold severity word
([CREG](https://github.com/erikthedeveloper/code-review-emoji-guide)):

| Tag | Meaning |
|---|---|
| 🚫 **blocker:** | Must fix before merge: a bug, data loss, a security hole, a broken contract |
| 🔧 **important:** | Should change; likely to cause a failure later |
| ❓ **question:** | Needs an answer. Use it for each finding that you did not prove |
| ♻️ **refactor:** | A restructure that makes the change smaller or simpler |
| ⛏ **nit:** | Optional |
| 📌 **follow-up:** | A real problem outside the scope of the PR. Ask for a Jira link |
| 📝 **note:** | Information, no action |

## Comment layout

The first line tells what is wrong without the rest. The author can triage
from that line alone.

- Line 1: the tag and a headline of at most about 12 words.
- A blank line, then the details. GitHub renders each newline, so write one
  sentence per line. Do not write a long paragraph.
- For 🚫 and 🔧: the failure scenario as a short list (input, steps, what
  breaks), then the fix.
- For ⛏, ❓, 📌, and 📝: at most two short lines after the headline.
- The last line is the request, and starts with **Suggest** or **Could we**.
  Use a ```` ```suggestion ```` block when the fix replaces lines directly.

```
🔧 **important:** A failed listing posts the how-to comment twice.

The step reads the comment list in a pipe into `grep -q`.
1. `gh api` fails, or gets SIGPIPE.
2. The pipe returns non-zero, and the `if` reads it as "no comment yet".
3. The step posts a second comment.

Suggest reading the list into a variable first, so a failure stops the step.
```

Rules:

- Problem first, code second. State what goes wrong for the user, then the
  code path that causes it.
- One issue for each comment, on the exact line. Write "Same applies to
  lines 55, 91" and do not repeat the comment.
- Do not repeat a finding that an existing review thread or a bot review
  already holds. At most, write "+1 to <link>".
- Do not raise formatting that pre-commit owns.
- Critique the code, not the person. Use short, plain sentences.
- Do not make up findings. A clean PR gets "LGTM".
- Quote a number, a name, or an error text only when you read it in the code, the PR, or a
  command output. Otherwise leave it out.

## Summary (the review body)

- **Clean PR**: `LGTM`, one `Verified:` line, then optional nits as bullets.
- **Other PRs**: one or two lines on what is good in the approach, a count by
  tag ("1 🔧 important, 2 ⛏ nits, inline."), then the outcome of each pass
  of `REVIEW.md` (`pass`, or the findings).
- **Verified**: one line. Each item that checked out gets a few words, with
  no explanation. Add a `Not run:` line only when the reader expects a check
  that did not run.
- Findings on lines outside the diff go in the summary as
  `tag path:line - text`, because they cannot be inline.
- **CI**: mention only failures, checks that did not run, and checks that
  will fail on merge. Do not mention green checks. For each failure, name
  the check or the test, quote the first line of its message, and state
  whether this PR caused it:

  ```
  **CI:** `jenkins/unittests` failed: `integration.test_kafka.test_02_kafka_scylla_sink_connector`
  (`curl: (22) The requested URL returned error: 504`). Not caused by this PR: a download from
  GitHub releases timed out. Re-run the build.
  ```
- **Suggested reviewers**: one line with at most three people who know the
  touched code, from `CODEOWNERS` and the git history of the files, each
  with a short reason. Leave out the author and the people already
  requested. Do not request them.
