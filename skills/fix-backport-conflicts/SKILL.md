---
name: fix-backport-conflicts
description: Fix inline merge conflict markers in backport PRs by resolving conflicts and recommitting cleanly with original metadata preserved.
disable-model-invocation: true
argument-hint: <PR-number>
---

# Fix Inline Conflicts in Backport PR

You are fixing inline merge conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`) that were
left in commits of a backport PR. The goal is to resolve the conflicts and produce clean
commits with original authorship and messages preserved.

## Input

- `$ARGUMENTS` — the PR number to fix (e.g. `13920`)

## Workflow

### 1. Checkout the PR

```
gh pr checkout $ARGUMENTS
```

Get the PR's base branch from `gh pr view $ARGUMENTS --json baseRefName`.

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

1. **Stash** the resolved changes: `git stash`
2. **Identify commits** ahead of the base: `git log --oneline upstream/<baseRefName>..HEAD`
3. **Determine which files belong to which commit** by inspecting each commit's diff with `git show <hash> --stat`
4. **Soft reset** to the base: `git reset --soft upstream/<baseRefName>`
5. **Pop the stash**: `git stash pop`
6. For each original commit (in order, oldest first):
   a. Stage only the files belonging to that commit (use `git checkout <original-hash> -- <file>` for files without conflicts, and `git add <file>` for conflict-resolved files)
   b. Unstage files belonging to later commits with `git reset HEAD -- <file>`
   c. Commit with the **original author** (`GIT_AUTHOR_NAME`, `GIT_AUTHOR_EMAIL`) and **original commit message** (copy it exactly from `git show --format="%s%n%n%b" --no-patch <hash>`)
7. **Verify** the final log matches the original commit count and messages: `git log --oneline upstream/<baseRefName>..HEAD`

### 7. Final verification

- Confirm no conflict markers remain in any file
- Confirm commit count and messages match the original PR
- Confirm each commit's `--stat` matches the expected file set

### 8. Update PR status

After the conflicts are resolved and recommitted:

1. **Remove the `conflicts` label** from the PR:
   ```
   gh pr edit $ARGUMENTS --remove-label "conflicts"
   ```
2. **Mark the PR as ready for review** (take it out of draft):
   ```
   gh pr ready $ARGUMENTS
   ```

## Important rules

- Never modify commit messages beyond what's needed (preserve co-author lines, cherry-pick references, etc.)
- Preserve original author attribution using `GIT_AUTHOR_NAME` and `GIT_AUTHOR_EMAIL` environment variables
- Do NOT force push — leave that decision to the user
- If a conflict resolution is ambiguous, ask the user before proceeding
