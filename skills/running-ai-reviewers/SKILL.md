---
name: running-ai-reviewers
description: >-
  Runs an AI reviewer of SCT on the local machine, with the same rules as on
  the pull request: Claude with the code-review skill (a Claude team login is
  enough), the CodeRabbit CLI (GitHub login, scylladb plan), or the Qodo
  Agentic Toolbox CLI (a Qodo login with credits). Each one reads REVIEW.md
  and prints the review locally; nothing is posted. Copilot has no local run;
  the skill points to its pull request command.
  Use when the user asks to "run coderabbit locally", "run qodo locally",
  "review my branch before I push", "get a second AI review", or to run a
  review bot without a pull request comment. Not for the review itself (use
  code-review) or for the reviewers on GitHub (see
  docs/contribution-handbook.md section 4.3).
argument-hint: "[claude|coderabbit|qodo] [PR number, or empty for the current branch]"
---

# Running an AI Reviewer Locally

Each reviewer reads `REVIEW.md`, so a local run checks the same passes as a
review on the pull request. A local run posts nothing. Run the commands from
the repository root.

| Reviewer | Needs | Reads |
|---|---|---|
| Claude | Claude Code, logged in with the Claude team account | `REVIEW.md`, the `code-review` skill, `docs/standards/` |
| CodeRabbit | CodeRabbit CLI, logged in with GitHub; reviews count against the scylladb plan | `.coderabbit.yaml`, and `REVIEW.md` through `--config` |
| Qodo | Qodo Agentic Toolbox CLI, logged in to a Qodo organization with credits | `REVIEW.md`, passed through `--context-file` |

Copilot has no local run. Request it on the pull request, as
`docs/contribution-handbook.md` section 4.3 says. For a branch without a pull
request, open a draft pull request first.

## Phase 1: Pick the Reviewer and the Scope

**Entry criteria**: the user wants a local AI review.

1. Take the reviewer from the arguments. When they are empty, use Claude.
   When the user asks for Copilot, give the pull request command above
   instead, and stop. When the user asks for Qodo and has no Qodo login with
   credits, give the pull request command `/agentic_review` instead, and stop.
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

### Qodo

Install the Qodo Agentic Toolbox CLI once (Node.js 20 or later), as the
[CLI docs](https://docs.qodo.ai/agentic-toolbox/cli) say, and log in. The
older `@qodo/command` npm package is deprecated and its servers are gone.

The installer is a vendor script. Download it outside the repository, read
it, then run it:

```bash
installer=$(mktemp)
curl -fsSL https://get.qodo.ai -o "$installer"
less "$installer"
sh "$installer" && rm -f "$installer"
qodo login        # browser sign-in; --no-browser prints the URL instead
qodo whoami       # the account and the organization the reviews bill
```

Each review uses the credits of the organization you log in to. The browser
sign-in can pick a personal account whose organization has none. With an API
key of the organization that has credits, pass the key in the environment.
`read -s` keeps it out of the shell history:

```bash
read -rs QODO_API_KEY && export QODO_API_KEY QODO_BASE_URL=https://sdk.qodo.ai
qodo whoami
```

The CLI reads `QODO_API_KEY` and `QODO_BASE_URL` before the stored login.

When the organization has no credits, the review uploads the diff and then
fails:

```text
■  review failed: a2a_passthrough_failed: remote terminal_state=failed: {"error":"AGENT-QUOTA-EXCEEDED: This organization's credit balance is exhausted; the review was not run. Add credits or wait for the next billing cycle, then retry.", ...}
```

Check `qodo whoami` and log in to the right organization. When no
organization you can use has credits, request `/agentic_review` on the pull
request instead.

Review the changes of the current branch, committed and uncommitted.
`--base` is required in SCT: the default base is the merge base with
`origin/main`, and `origin` is a fork whose main branch is `master`. Without
it the review fails with `could not resolve base ref "origin/main"`.
`--base` takes an exact ref, so pass the merge base: with the tip of
`upstream/master`, a branch that is behind master gets the newer master
commits in its review. `--repo` defaults to `origin` too, and the reviewer
clones the base from that repository. A fork's master is often older than
the merge base, so pass `--repo scylladb/scylla-cluster-tests`.

Qodo has no `REVIEW.md` setting for a local run. Put the text of
`REVIEW.md` in the `notes` of a context file (the limit is 32,768
characters):

```bash
git fetch upstream master
ctx=$(mktemp)
jq -n --rawfile guide REVIEW.md '{notes: ("Review this change with the review guide below and the docs/standards/ files it links. Report the outcome of each of its three passes.\n\n" + $guide)}' > "$ctx"
qodo review --repo scylladb/scylla-cluster-tests --base "$(git merge-base upstream/master HEAD)" --context-file "$ctx"
rm -f "$ctx"
```

Replace `upstream` with the name of your `scylladb` remote.

The review posts nothing on GitHub, but it uploads the local changes to
Qodo, untracked files included. It skips gitignored files and secret-like
paths. Commit or remove scratch files before the review. `--deep` or `--fast` sets
the depth, and git pathspecs after the options limit the files. For a pull
request, run it in a worktree of its head, as for CodeRabbit.

The local run is not the `/agentic_review` bot: `.pr_agent.toml`
(`enable_review_md`, the severity threshold, the cap on findings) applies
only on the pull request.

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
