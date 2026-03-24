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

## Modes

### Single PR mode

When `$ARGUMENTS` contains a single PR number, follow the Workflow below directly.

### Bulk mode

When `$ARGUMENTS` contains multiple PR numbers (space-separated):

1. **Dispatch each PR to a parallel subagent** using the Agent tool with `isolation: "worktree"`.
   Each subagent receives one PR number and follows the full Workflow below independently.
   Because each subagent runs in its own worktree, there are no branch or stash collisions.
2. **Collect results** — after all subagents complete, summarize which PRs succeeded and which
   failed or had ambiguous conflicts requiring user input.
3. **Force-push reminder** — remind the user to force-push each PR branch after reviewing.

## Workflow

### 1. Checkout the PR

```
gh pr checkout $PR_NUMBER
```

Get the PR's base branch from `gh pr view $PR_NUMBER --json baseRefName`.

### 2. Fetch the base branch

```
git fetch upstream <baseRefName>
```

### 3. Check for inline conflict markers

Search all tracked files for conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`).
If none are found, report "No conflicts found" and stop.

### 4. Understand each conflict

For each conflict block, identify:
- **HEAD side** — changes already in the base branch
- **Incoming side** — changes from the cherry-picked/backported commit
- **Parent (if diff3)** — the common ancestor version

Determine the correct resolution by combining both sides:
- Keep new features/fixes from both HEAD and the incoming change
- Use the incoming commit's architectural approach (e.g. list-based vs single) when that's the purpose of the backport
- Preserve additions from HEAD that don't conflict with the incoming approach (e.g. new parameters, new method calls)

### 5. Resolve the conflicts

Edit the files to remove all conflict markers and produce the correct merged code.
Verify no conflict markers remain with a grep.

### 6. Recommit cleanly

This is the critical step — the resolved changes must be attributed to the correct commits.

**Important: Do NOT use `git stash`** — stashes are shared across all worktrees and will collide
when running multiple PRs in parallel. Instead, use a temporary commit to hold resolved state.

1. **Save resolved state as a temporary commit**: `git add -A && git commit --no-verify -m "TEMP: resolved conflicts"`
   (Use `--no-verify` only for this temporary commit — it will be discarded. Pre-commit hooks may
   fail in worktrees that lack installed dependencies.)
2. **Identify commits** ahead of the base (excluding the temp commit): `git log --oneline upstream/<baseRefName>..HEAD`
   Note the original commit hashes (all except the TEMP commit).
3. **Determine which files belong to which commit** by inspecting each commit's diff with `git show <hash> --stat`
4. **Mixed reset** to the base: `git reset upstream/<baseRefName>`
   (Use mixed reset — NOT `--soft` — so all changes become unstaged. This prevents accidentally
   committing files that belong to other commits.)
5. For each original commit (in order, oldest first):
   a. Stage only the files belonging to that commit: `git add <file1> <file2> ...`
   b. Commit with the **original author** and **original commit message**:
      ```
      GIT_AUTHOR_NAME="<name>" GIT_AUTHOR_EMAIL="<email>" git commit --no-verify -m "<message>"
      ```
      Copy the full message from `git show --format="%B" --no-patch <original-hash>`.
      Use `--no-verify` because pre-commit hooks may not work in worktrees.
6. **Verify** the final log matches the original commit count and messages: `git log --oneline upstream/<baseRefName>..HEAD`

### 7. Final verification

- Confirm no conflict markers remain in any file
- Confirm commit count and messages match the original PR
- Confirm each commit's `--stat` matches the expected file set

### 8. Update PR status

After the conflicts are resolved and recommitted:

1. **Remove the `conflicts` label** from the PR:
   ```
   gh pr edit $PR_NUMBER --remove-label "conflicts"
   ```
2. **Mark the PR as ready for review** (take it out of draft):
   ```
   gh pr ready $PR_NUMBER
   ```

## Important rules

- **Never use `git stash`** — stashes are global across worktrees and cause collisions in parallel runs. Use a temporary commit instead.
- Never modify commit messages beyond what's needed (preserve co-author lines, cherry-pick references, etc.)
- Preserve original author attribution using `GIT_AUTHOR_NAME` and `GIT_AUTHOR_EMAIL` environment variables
- Do NOT force push — leave that decision to the user
- If a conflict resolution is ambiguous, ask the user before proceeding
- In bulk mode, each PR runs in its own worktree via `isolation: "worktree"` to avoid branch and state collisions
