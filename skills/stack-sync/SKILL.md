---
name: stack-sync
description: >-
  Manage stacked PRs in scylla-cluster-tests using the gh-stack CLI extension
  (GitHub's native stacked PR support, public preview since 2026-07-31). Use
  when asked to split a large change into a chain of small reviewable PRs,
  check stack status, add/push/sync/rebase/merge stack layers, or navigate
  between stack branches. Covers the fork-vs-upstream remote nuance specific
  to SCT's contribution model. Adapted from the official github/gh-stack
  agent skill.
argument-hint: "[view | init <branches...> | add <branch> | push | submit | sync | rebase | trunk | merge [stack-number | pr-number] | checkout <target> | modify | unstack [number]]"
---

# Stacked PRs for SCT

You are a stacked PR assistant using the official GitHub CLI extension `gh-stack` (`gh stack ...`, v0.1.0+).

Source: adapted from the official agent skill shipped in [github/gh-stack](https://github.com/github/gh-stack) (`skills/gh-stack/SKILL.md`, MIT licensed), with SCT-specific notes layered on top (fork/upstream remotes, alias collision, default trunk). Full docs: https://docs.github.com/en/pull-requests/get-started/about-stacked-prs

## Background

GitHub's native stacked pull requests went to public preview on 2026-07-31 - available to any repository automatically, no per-repo toggle or approval needed.

## The fork caveat (read this first)

**Cross-fork stacks are not supported.** Every branch in a stack must live in the same repository. Regular SCT contributions here go through a personal fork (`origin` -> `fruch/scylla-cluster-tests`, PRs opened cross-repo against `upstream` -> `scylladb/scylla-cluster-tests`) - that model does **not** work for a multi-layer stack, because a mid-stack PR's base branch has to exist in the same repo as its head, and a fork-only branch isn't visible to `upstream` as a valid base.

You (this account) have `admin`/push access directly on `scylladb/scylla-cluster-tests`, so the fix is: **for stacked work only**, push the stack's branches directly to `upstream` instead of to the fork. `init` is a local-only command (it only takes `--base`); the remote comes into play on the commands that talk to GitHub:
```bash
gh stack init my-stack-root --base master
gh stack submit --remote upstream --auto
```
Keep using the fork (`origin`) as before for regular, single-branch PRs - only stacks need the `--remote upstream` treatment. `--remote` is accepted by `push`, `submit`, `sync`, and `rebase`; `checkout`, `modify`, and `trunk` have no `--remote` flag; if `upstream` isn't your `git config remote.pushDefault`, set it before using those, or resolve the remote explicitly.

## Prerequisites

Check the extension is installed and current:
```bash
gh extension list | grep gh-stack || gh extension install github/gh-stack
gh extension upgrade stack   # upgrade takes the extension name, not the repo slug; flags below assume v0.1.0+
```

Pre-enable `git rerere` before the first `gh stack init` in a repo - `init` enables it for you but may ask for confirmation on first run, which hangs a non-interactive session even when branch arguments are given:
```bash
git config rerere.enabled true
```

**Do not run `gh stack alias`'s default.** It aliases `gh stack` to `gs`, which collides with Ghostscript on machines that have it installed. If you want a shorthand, pick a name that doesn't collide:
```bash
gh stack alias gst
```
Everything below uses the unambiguous `gh stack ...` form. Default trunk for this repo is `master`, not `main`.

## Agent rules (must-follow, non-interactive use)

1. **Always supply branch names as positional arguments** to `init`, `add`, and `checkout`. Without them these commands launch interactive prompts that hang in a non-interactive session. Branch arguments alone don't make first-time `init` fully non-interactive - it can still prompt to enable `git rerere`; pre-configure it as shown in Prerequisites.
2. **Always use `--auto` with `gh stack submit`.** Without it, `submit` opens an interactive editor. With `--auto`, new PRs default to **draft**; pass `--open` too if they should be ready for review.
3. **Always use `--json` with `gh stack view`.** Without it, `view` renders an interactive view.
4. **Always pass `--remote upstream`** to `push`/`submit`/`sync`/`rebase` for stacked work here (see fork caveat above).
5. **Plan layers by dependency order** before writing code: foundational changes (shared types, config) go in lower branches; dependent changes (consumers, UI) go in higher branches.
6. **Use standard `git add`/`git commit`** to control exactly which changes land in which branch. The `add -Am "msg" branch` shortcut is available but bypasses deliberate staging - reserve it for single-commit layers.
7. **Navigate down to fix a lower layer.** If you're on a high layer and need to change something below it, don't patch around it in place - `gh stack down` (or `checkout`) to the right branch, commit there, then `gh stack rebase --upstack --remote upstream` and navigate back up.
8. **Use `gh stack merge --yes`** to merge stacked PRs; `gh pr merge` does not understand stacks. It merges bottom-up; direct merges are atomic, all-or-nothing. If the base branch uses a merge queue, the PRs are instead enqueued together but may land in separate groups - don't assume rollback semantics there. Scope with a PR number (`gh stack merge 15601 --yes` merges up to and including that PR) or a stack number.
9. **`gh stack checkout <pr-number>` can hit an unbypassable conflict prompt** if a different local stack already exists on those branches. Run `gh stack unstack --local` first (keeps the GitHub stack intact), then retry.

---

## Arguments

$ARGUMENTS may contain:
- `view` (default) - show current stack state
- `init <branches...>` - start a new stack from the current branch (remember `--base master`; `init` has no `--remote` flag)
- `add <branch>` - add a new layer on top of the current stack
- `push` - push every branch in the stack (remember `--remote upstream`)
- `submit` - push + create/update PRs for the whole stack (remember `--remote upstream`)
- `sync` - fetch, cascade-rebase, push, and sync PR state (the routine command; remember `--remote upstream`)
- `rebase` - cascading rebase across the stack (finer control / conflict resolution)
- `trunk` - jump to the stack's trunk branch
- `merge [stack-number | pr-number]` - merge a PR and everything below it in the stack
- `checkout <stack-number | pr-number | pr-url | branch>` - check out a stack
- `modify` - interactive TUI to reorder/rename/fold/drop branches (not agent-drivable)
- `unstack [number]` - remove local tracking and/or the GitHub stack grouping

---

## view

```bash
gh stack view --json      # required for non-interactive use
gh stack view --short     # compact, human use only
```
Fields per branch: `name`, `head`, `base`, `isCurrent`, `isMerged`, `isQueued`, `needsRebase`, `pr.{number,url,state}`.

---

## init <branches...>

```bash
gh stack init auth --base master
gh stack init branch-a branch-b branch-c --base master
```
Existing branches are adopted; missing ones are created from the trunk. Checks out the last branch given. `init` is local-only and has no `--remote` flag - the upstream remote is chosen later, on `push`/`submit`/`sync`/`rebase`.

---

## add <branch>

```bash
gh stack add api-routes
git add sdcm/api/routes.py
git commit -m "feature(api-routes): add REST routes for cluster operations" \
           -m "Expose the cluster API endpoints consumed by the next stack layer."

# shortcut: stage + commit + branch in one step (message must still follow SCT's
# conventional-commit format: type(scope): subject header plus a body)
gh stack add -Am "feature(api-routes): add REST routes for cluster operations

Expose the cluster API endpoints consumed by the next stack layer." api-routes
```
Must be run from the topmost branch (or trunk, for the first layer). Branch names are used verbatim. Commit messages must pass SCT's commitlint rules (`type(scope): subject` header, body of 30+ characters - see `.github/copilot-instructions.md`).

---

## push / submit

```bash
gh stack push --remote upstream
gh stack submit --remote upstream --auto           # required for non-interactive use; new PRs default to draft
gh stack submit --remote upstream --auto --open    # create as ready for review instead
```
Creates a PR per branch that lacks one (base = nearest non-merged ancestor, i.e. `master` for the bottom layer), updates base branches for existing PRs, and links them into a **Stack** on GitHub.

---

## sync

```bash
gh stack sync --remote upstream
gh stack sync --remote upstream --prune    # also delete local branches for merged PRs
```
If a rebase conflict is hit, all branches are restored and it tells you to run `gh stack rebase` instead.

---

## rebase

```bash
gh stack rebase --remote upstream            # whole stack
gh stack rebase --upstack --remote upstream    # current branch upward only
gh stack rebase --downstack --remote upstream  # trunk to current branch only
gh stack rebase --continue                   # after resolving conflicts (git add the files first); local-only, no --remote
gh stack rebase --abort                      # bail out, restore all branches; local-only, no --remote
```

---

## trunk / navigation

```bash
gh stack trunk
gh stack up [n] / gh stack down [n]
gh stack top / gh stack bottom
```

---

## merge [stack-number | pr-number]

```bash
gh stack merge --yes           # current branch's PR + everything below it
gh stack merge 15601 --yes     # merge up to and including PR #15601
```
The optional argument is a stack number or a PR number - branch names are not accepted. Direct merges are all-or-nothing: if any PR can't be merged, none are. With a merge queue on the base branch, the PRs are enqueued together instead but may land in separate queue groups.

---

## checkout <target>

```bash
gh stack checkout 7                 # by stack number
gh stack checkout 15601             # by PR number (pulls from GitHub)
gh stack checkout feature-auth      # by branch, local tracking only
```

---

## unstack [number]

```bash
gh stack unstack             # current stack: local tracking + GitHub grouping (PRs are NOT deleted)
gh stack unstack 7            # a specific stack, from anywhere in the repo
gh stack unstack --local      # local tracking only, never touches GitHub
```

---

## Exit codes

| Code | Meaning | Action |
|------|---------|--------|
| 0 | Success | Proceed normally |
| 1 | Generic error | Read stderr for details (may be a commit/push failure) |
| 2 | Not in a stack | `gh stack init` |
| 3 | Rebase conflict | Resolve conflicted files, `git add`, `gh stack rebase --continue` |
| 4 | GitHub API failure | Check `gh auth status`, retry |
| 5 | Invalid arguments or flags | Fix the command invocation |
| 6 | Branch belongs to multiple stacks | `gh stack checkout <specific-branch>` to disambiguate |
| 7 | Rebase already in progress | `gh stack rebase --continue` (after resolving) or `gh stack rebase --abort` |
| 8 | Stack is locked | Another `gh stack` process is writing; wait and retry |
| 9 | Stacked PRs unavailable on this repo | Report it to the user; there is no per-repo toggle to flip (public preview) - the platform or rollout doesn't cover this repository yet |
| 10 | Interrupted `modify` session needs recovery | `gh stack modify --abort` to restore the pre-modify state (this skill doesn't drive `modify`) |

## Key principles

- Each layer is a focused, independently reviewable change - a stack should read as one cohesive story
- Stacks are strictly linear - one parent, at most one child per branch; use separate stacks for parallel workstreams
- CI and branch protection are enforced on every PR in the stack, not just ones targeting `master`
- Merges go bottom-up; `gh stack merge` handles the ordering
- Stacked branches live on `upstream` (`scylladb/scylla-cluster-tests`), not the personal fork
