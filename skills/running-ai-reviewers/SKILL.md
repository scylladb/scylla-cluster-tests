---
name: running-ai-reviewers
description: >-
  Runs an AI reviewer of SCT on the local machine, with the same rules as on
  the pull request: Claude with the code-review skill (a Claude team login is
  enough), or the CodeRabbit CLI (GitHub login, scylladb plan). Each one reads
  REVIEW.md and prints the review locally; nothing is posted. Qodo and
  Copilot have no local run; the skill points to their pull request commands.
  Use when the user asks to "run coderabbit locally", "run qodo locally",
  "review my branch before I push", "get a second AI review", or to run a
  review bot without a pull request comment. Not for the review itself (use
  code-review) or for the reviewers on GitHub (see
  docs/contribution-handbook.md section 4.3).
argument-hint: "[claude|coderabbit] [PR number, or empty for the current branch]"
---

# Running an AI Reviewer Locally

Each reviewer reads `REVIEW.md`, so a local run checks the same passes as a
review on the pull request. A local run posts nothing. Run the commands from
the repository root.

| Reviewer | Needs | Reads |
|---|---|---|
| Claude | Claude Code, logged in with the Claude team account | `REVIEW.md`, the `code-review` skill, `docs/standards/` |
| CodeRabbit | CodeRabbit CLI, logged in with GitHub; reviews count against the scylladb plan | `.coderabbit.yaml`, and `REVIEW.md` through `--config` |

Qodo and Copilot have no local run here (Qodo would need a paid API key).
Request them on the pull request: comment `/agentic_review` for Qodo, and see
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
request, and its checks, through `git diff`, `git log`, `git show`, and
`gh`. It cannot fetch, edit files, run other commands, or post.

Fetch first, because Claude cannot. Fetch the release branches too: the
trigger-matrix check reads them. Stay on a checkout of `master`, not of the
pull request: Claude reads `.claude/settings.json` and the skills from the
checkout, and a pull request must not choose its own permissions.

```bash
git fetch upstream master 'refs/heads/branch-*:refs/remotes/upstream/branch-*' pull/<N>/head:pr-<N>
claude -p "Review pull request #<N> with the code-review skill. Its head is the local branch pr-<N> and its base is upstream/master: read the diff with git diff upstream/master...pr-<N> and its files with git show pr-<N>:<path>. Read its description and labels with gh pr view <N>. Run git in the current directory, without -C. Follow REVIEW.md and report the outcome of each pass. Do not post anything." \
  --tools "Read,Grep,Glob,Skill,Bash" --setting-sources project --strict-mcp-config --permission-mode dontAsk \
  --allowedTools "Skill,Bash(git diff:*),Bash(git log:*),Bash(git show:*),Bash(gh pr view:*),Bash(gh pr diff:*),Bash(gh pr checks:*),Bash(gh api:*)" \
  --disallowedTools "Bash(gh *://*),Bash(gh *--hostname*),Bash(gh *-R*),Bash(gh *--repo*),Bash(git fetch:*),Bash(git grep:*),Bash(git add:*),Bash(.claude/scripts/fix-backport.sh:*),Bash(git *--output*),Bash(git *--no-index*),Bash(gh api *-X*),Bash(gh api *--method*),Bash(gh api *-f*),Bash(gh api *-F*),Bash(gh api *--input*)"
git branch -D pr-<N>
```

Replace `upstream` with the name of your `scylladb` remote.

Each flag closes a way around the read-only list:
- `--tools` removes `Write` and `Edit`.
- `--setting-sources project` ignores the allow rules in your own settings.
- `--strict-mcp-config` loads no MCP server, so Claude cannot call your
  connectors (a GitHub connector, for example). Without it they load even
  with `--setting-sources project`.
- `--permission-mode dontAsk` denies each tool that no list allows. The
  allow rules match the start of the command, so `git -C <path> log` is
  denied; the prompt asks Claude to run git without `-C`.
- `Read`, `Grep`, and `Glob` are in `--tools` but not in `--allowedTools`.
  `dontAsk` then lets them read the repository only. An allow rule would
  also let them read `~/.aws/credentials`.
- `--disallowedTools` denies what `.claude/settings.json` allows and a
  review must not do, because a deny beats an allow from any source:
  - `git fetch`: a fetch from a local path with `--upload-pack=<cmd>` runs
    `<cmd>`, and a refspec with `+` rewrites a local branch.
  - `git grep`: `-O<cmd>` runs `<cmd>`. The `Grep` tool searches instead.
  - `git add` and `fix-backport.sh`, which write.
  - `git` with `--output` (writes a file) or `--no-index` (reads a file
    outside the repository).
  - The `gh api` calls that write. The globs have no space after the flag,
    because `-fbody=x` (a field, so a POST) is valid. They also match
    `--field` and `--raw-field`.
  - `gh` with a full URL, `--hostname`, `-R`, or `--repo`, which connect
    to any host and can send data out in the URL. The skill reads only
    `gh api repos/<repo>/...` and the current repository. These globs are
    best effort; a sandbox with a GitHub-only network allowlist is the
    real boundary.

For the current branch, fetch only `master` and the release branches, and
replace the first three sentences of the prompt with "Review the changes of
this branch: read the diff with git diff upstream/master...HEAD."

### CodeRabbit

Install the CLI once, as the [CLI docs](https://docs.coderabbit.ai/cli)
say (`brew install coderabbit` on macOS), then log in:

```bash
coderabbit auth login
```

Review the committed changes of the current branch against its merge base
with the master of the `scylladb` remote (`upstream` here). A local `master`
is often old, and `--base upstream/master` also reviews the commits that
master gained after the branch started:

```bash
git fetch upstream master
coderabbit review --base-commit "$(git merge-base upstream/master HEAD)" --committed --deep --config REVIEW.md
```

For a pull request, run the same command in a worktree of its head:

```bash
git fetch upstream master pull/<N>/head:pr-<N>
git worktree add ../review-pr-<N> pr-<N>
```

The CLI reads `.coderabbit.yaml` by itself. `--config` adds `REVIEW.md`.
`--deep` applies the full pull request review policy. It takes a few
minutes and finds more than the default run. Remove the worktree and
`pr-<N>` after the review.

The CLI finds the repository in the scylladb organization, so the review
counts against the scylladb CodeRabbit plan, which has a limit per rolling
hour. `coderabbit review --usage` shows what is left.

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
