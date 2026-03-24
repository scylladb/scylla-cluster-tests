---
name: fix-backport-conflicts
description: >-
  Fix inline merge conflict markers in backport PRs by resolving
  conflicts and recommitting cleanly with original metadata preserved.
  Use when a backport PR has unresolved conflict markers, a cherry-pick
  produced merge conflicts, or a PR has the 'conflicts' label and needs
  to be made ready for review. Supports bulk mode for multiple PRs.
disable-model-invocation: true
argument-hint: <PR-number> [PR-number...]
---

# Fix Inline Conflicts in Backport PR

You are fixing inline merge conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`) that were
left in commits of a backport PR. The goal is to resolve the conflicts and produce clean
commits with original authorship and messages preserved.

## Input

- `$ARGUMENTS` — one or more PR numbers to fix (e.g. `13920` or `13920 13921 13925`)

## Dispatch — ALWAYS use a subagent

**Every PR MUST be dispatched to a subagent with `isolation: "worktree"`.**
Never run the workflow in the current working directory — it checks out branches, resets HEAD,
and rewrites commits, which would destroy any in-progress work. The worktree gives each PR
a completely isolated git state.

For each PR number in `$ARGUMENTS`, launch a parallel Agent with:
- `isolation: "worktree"`
- `subagent_type: "general-purpose"`
- A prompt containing: the PR number, and the full Workflow below (copy it into the prompt)

After all subagents complete, summarize which PRs succeeded and which failed or had ambiguous
conflicts requiring user input. Remind the user to review the changes.

## Helper Script

All dangerous git operations (rebase, reset, commit --no-verify, force-push) are wrapped in
`.claude/scripts/fix-backport.sh`. This script is the ONLY command allowed in project
permissions, keeping the blast radius small. Available subcommands:

| Subcommand | Purpose |
|------------|---------|
| `checkout <PR>` | Checkout PR branch, print base/head/headRepoOwner as JSON |
| `rebase <BASE_REF>` | Fetch latest base branch and rebase PR onto it |
| `rebase-continue` | Continue rebase after resolving conflicts |
| `resolve-commit` | Stage all + temp commit (--no-verify) |
| `prepare-recommit <BASE_REF>` | Tag resolved tree, hard-reset to base for clean recommit |
| `recommit <HASH>` | Copy files from resolved tree for one commit, commit with original author |
| `push <REMOTE> <BRANCH>` | Force-push with --force-with-lease |
| `update-pr <PR>` | Remove conflicts label + mark ready |
| `verify` | Check no conflict markers remain |

## Workflow (executed by the subagent inside its worktree)

**All commands run from the worktree's working directory. Never use `git -C`.**

### 1. Checkout the PR

```
.claude/scripts/fix-backport.sh checkout $PR_NUMBER
```

This prints JSON with `base`, `head`, and `headRepoOwner` fields. Save all three.
The `headRepoOwner` is the git remote name to push to (e.g. `scylladbbot`, not `origin`).
Backport PRs are typically created by bots on their own forks.

### 2. Rebase onto the latest base branch

```
.claude/scripts/fix-backport.sh rebase upstream/<baseRefName>
```

This fetches the latest state of the base branch and rebases the PR commits onto it.
Rebasing BEFORE resolving conflicts ensures we work against the current target code.
If we resolved first and rebased after, the rebase could introduce new conflicts,
requiring double work.

**If the rebase prints `REBASE_OK`:** The PR is now up to date with the base. Proceed
to step 3.

**If the rebase prints `REBASE_CONFLICTS`:** The rebase stopped because git couldn't
auto-resolve the cherry-pick against the latest base. This is the expected case for
backport PRs with the `conflicts` label. The conflicted files are in the working tree.
Skip to step 3 (the conflict markers are already there from the rebase).

### 3. Check for inline conflict markers

```
.claude/scripts/fix-backport.sh verify
```

If it prints "CLEAN", the rebase resolved everything automatically. Run
`rebase-continue` if needed, then skip to step 7 (push).

### 4. Understand each conflict

Read each conflicted file. For each conflict block, identify:
- **HEAD side** — changes already in the base branch
- **Incoming side** — changes from the cherry-picked/backported commit
- **Parent (if diff3)** — the common ancestor version

Determine the correct resolution by combining both sides:
- Keep new features/fixes from both HEAD and the incoming change
- Use the incoming commit's architectural approach when that's the purpose of the backport
- Preserve additions from HEAD that don't conflict with the incoming approach

### 5. Resolve the conflicts

Edit the files to remove all conflict markers and produce the correct merged code.
Then verify:
```
.claude/scripts/fix-backport.sh verify
```
Must print "CLEAN" before proceeding.

Then complete the rebase:
```
git add <resolved-files>
.claude/scripts/fix-backport.sh rebase-continue
```

If there are multiple commits being rebased and the next one also has conflicts,
repeat steps 3-5 for each.

### 6. Verify the result

After the rebase completes:

```
.claude/scripts/fix-backport.sh verify
git log --oneline upstream/<baseRefName>..HEAD
git show --stat HEAD
```

Confirm:
- No conflict markers remain
- Commit count matches the original PR
- Each commit's `--stat` matches expected files (compare with parent PR if needed)
- No files outside the original commit's scope are included

### 7. Push and update PR

```
.claude/scripts/fix-backport.sh push <headRepoOwner> <headRefName>
.claude/scripts/fix-backport.sh update-pr $PR_NUMBER
```

**CRITICAL:** Use `headRepoOwner` from step 1 as the remote — NOT `origin`.
Backport PRs are created by bots (e.g. `scylladbbot`) on their forks. Pushing
to `origin` goes to the wrong fork and leaves the PR unchanged.

## Important rules

- **Always dispatch to a worktree subagent** — never run the workflow in the current working directory
- **Always rebase onto the latest base branch first** — resolving stale conflicts wastes work if the rebase introduces new ones
- **Use `.claude/scripts/fix-backport.sh`** for all dangerous operations — raw git reset/commit --no-verify/push are not in the permission allowlist
- **Never use `git stash`** — stashes are global across worktrees
- **Never use `git -C`** — all commands run from the worktree working directory
- **Verify file scope after rebase** — each commit must touch ONLY the files from the original commit, nothing else
- Never modify commit messages beyond what's needed (preserve co-author lines, cherry-pick references, etc.)
- If a conflict resolution is ambiguous, ask the user before proceeding
