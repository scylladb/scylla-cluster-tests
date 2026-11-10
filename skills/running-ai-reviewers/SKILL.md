---
name: running-ai-reviewers
description: >-
  Runs an AI reviewer of SCT on the local machine, with the same rules as on
  the pull request: Claude with the code-review skill (a Claude team login is
  enough), or the CodeRabbit CLI (free login). Each one reads REVIEW.md and
  prints the review locally; nothing is posted. Qodo and Copilot have no local
  run; the skill points to their pull request commands. Use when the user asks to "run
  coderabbit locally", "run qodo locally", "review my branch before I push",
  "get a second AI review", or to run a review bot without a pull request
  comment. Not for the review itself (use code-review) or for the reviewers
  on GitHub (see docs/contribution-handbook.md section 4.3).
argument-hint: "[claude|coderabbit] [PR number, or empty for the current branch]"
---

# Running an AI Reviewer Locally

Each reviewer reads `REVIEW.md`, so a local run checks the same passes as a
review on the pull request. A local run posts nothing. Run the commands from
the repository root.

| Reviewer | Needs | Reads |
|---|---|---|
| Claude | Claude Code, logged in with the Claude team account | `REVIEW.md`, the `code-review` skill, `docs/standards/` |
| CodeRabbit | CodeRabbit CLI, logged in (free plan) | `.coderabbit.yaml`, and `REVIEW.md` through `--config` |

Qodo and Copilot have no local run without a paid API key. Request them on
the pull request: comment `/agentic_review` for Qodo, and see
`docs/contribution-handbook.md` section 4.3 for Copilot. For a branch without
a pull request, open a draft pull request first. Both post on the pull
request.

## Phase 1: Pick the Reviewer and the Scope

**Entry criteria**: the user wants a local AI review.

1. Take the reviewer from the arguments. When they are empty, use Claude.
   When the user asks for Qodo or Copilot, give the pull request command
   above instead, and stop.
2. Take the scope: a pull request number, or the current branch against
   `upstream/master`, the master of the `scylladb` remote. Find its name with
   `git remote -v`. A CodeRabbit run of the current branch reviews only the
   committed changes.

**Exit criteria**: the reviewer and the scope are known.

## Phase 2: Run It

**Entry criteria**: Phase 1 complete.

### Claude

In a Claude Code session in the repository, invoke the `code-review` skill:
`/code-review <PR number>`, or `/code-review` for the current branch.

Without a session, run it headless. Claude can only read the code, the pull
request, and its checks. It cannot edit files, run tests, or post:

```bash
claude -p "Review pull request #<N> with the code-review skill. Follow REVIEW.md and report the outcome of each pass. Do not post anything." \
  --tools "Read,Grep,Glob,Skill,Bash" --setting-sources project --permission-mode dontAsk \
  --allowedTools "Read,Grep,Glob,Skill,Bash(git fetch upstream:*),Bash(git diff:*),Bash(git log:*),Bash(git show:*),Bash(gh pr view:*),Bash(gh pr diff:*),Bash(gh pr checks:*),Bash(gh api:*)" \
  --disallowedTools "Bash(git add:*),Bash(.claude/scripts/fix-backport.sh:*),Bash(git *--output*),Bash(git *--no-index*),Bash(gh api *-X*),Bash(gh api *--method*),Bash(gh api *-f*),Bash(gh api *-F*),Bash(gh api *--input*)"
```

Replace `upstream` with the name of your `scylladb` remote.

Each flag closes a way around the read-only list:
- `--tools` removes `Write` and `Edit`.
- `--setting-sources project` ignores the allow rules in your own settings.
- `--permission-mode dontAsk` denies each tool that no list allows.
- `--allowedTools` allows `git fetch` only from the `scylladb` remote. A
  fetch from a local path with `--upload-pack=<cmd>` runs `<cmd>`.
- `--disallowedTools` denies the `gh api` calls that write, `git` with
  `--output` (writes a file) or `--no-index` (reads a file outside the
  repository), and the two writing commands that `.claude/settings.json`
  allows. The `gh api` globs have no space after the flag, because
  `-fbody=x` (a field, so a POST) is valid. They also match `--field`
  and `--raw-field`.

Claude can fetch the pull request into a local branch. Delete the branch after
the review.

For the current branch, replace the first sentence with
"Review the changes of this branch against upstream/master".

### CodeRabbit

Install the CLI once, as the [CLI docs](https://docs.coderabbit.ai/cli)
say (`brew install coderabbit` on macOS), then log in:

```bash
coderabbit auth login
```

Review the committed changes of the current branch. Use the master of the
`scylladb` remote (`upstream` here), because a local `master` is often old:

```bash
git fetch upstream master
coderabbit review --base upstream/master --committed --config REVIEW.md
```

The CLI reads `.coderabbit.yaml` by itself. `--config` adds `REVIEW.md`.
The free plan has a daily limit.

**Exit criteria**: the review is printed.

## Phase 3: Report

**Entry criteria**: Phase 2 complete.

1. Show the findings, grouped by the passes of `REVIEW.md`.
2. Treat each finding as a suggestion. Check it against the code before you
   act on it.

**Exit criteria**: the user has the findings.

## When NOT to Use

- To request a reviewer on the pull request. Use the commands in
  `docs/contribution-handbook.md` section 4.3.
- To change what the reviewers check. Change `REVIEW.md` or
  `docs/standards/` (use updating-standards).
