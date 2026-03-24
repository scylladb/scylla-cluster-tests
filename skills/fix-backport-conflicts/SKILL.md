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

All dangerous git operations (reset, commit --no-verify, force-push) are wrapped in
`.claude/scripts/fix-backport.sh`. This script is the ONLY command allowed in project
permissions, keeping the blast radius small. Available subcommands:

| Subcommand | Purpose |
|------------|---------|
| `checkout <PR>` | Checkout PR branch, print base/head ref as JSON |
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

Then fetch the base branch:
```
git fetch upstream <baseRefName>
```

### 2. Check for inline conflict markers

```
.claude/scripts/fix-backport.sh verify
```

If it prints "CLEAN", report "No conflicts found" and stop.

### 3. Understand each conflict

Read each conflicted file. For each conflict block, identify:
- **HEAD side** — changes already in the base branch
- **Incoming side** — changes from the cherry-picked/backported commit
- **Parent (if diff3)** — the common ancestor version

Determine the correct resolution by combining both sides:
- Keep new features/fixes from both HEAD and the incoming change
- Use the incoming commit's architectural approach when that's the purpose of the backport
- Preserve additions from HEAD that don't conflict with the incoming approach

### 4. Resolve the conflicts

Edit the files to remove all conflict markers and produce the correct merged code.
Then verify:
```
.claude/scripts/fix-backport.sh verify
```
Must print "CLEAN" before proceeding.

### 5. Recommit cleanly

This is the most critical step. The resolved files must be attributed to the correct
commits, and ONLY those files — no unrelated diffs must leak in.

**Why this is tricky:** After resolving conflicts, the working tree contains the resolved
state on top of the PR branch. A naive `git reset` + `git add <files>` would stage the
full diff of each file against the base — including unrelated changes that were already
in the branch before the backport. The script avoids this by:
1. Saving the resolved tree as a git tag
2. Hard-resetting to the base (clean slate)
3. Extracting ONLY the specific files from the resolved tree via `git show <tag>:<file>`

**Steps:**

1. **Save resolved state as temp commit**:
   ```
   .claude/scripts/fix-backport.sh resolve-commit
   ```

2. **Identify commits** ahead of base (excluding the TEMP commit):
   ```
   git log --oneline upstream/<baseRefName>..HEAD
   ```
   Note the original commit hashes (all except TEMP).

3. **Prepare for recommit** — tags resolved tree and hard-resets to base:
   ```
   .claude/scripts/fix-backport.sh prepare-recommit upstream/<baseRefName>
   ```
   After this, the working tree is clean and matches the base branch exactly.

4. **Recommit each original commit** (oldest first):
   ```
   .claude/scripts/fix-backport.sh recommit <original-hash>
   ```
   The script automatically:
   - Reads the original commit's file list from `git show --name-only`
   - Copies ONLY those files from the saved resolved tree (`_fix_backport_resolved` tag)
   - Commits with the original author name, email, and full message

5. **Verify** commit count, messages, and file lists:
   ```
   git log --oneline upstream/<baseRefName>..HEAD
   git show --stat HEAD
   ```
   The commit count MUST match the original PR. Each commit MUST touch only the
   files from the original commit — no extra files.

### 6. Final verification

```
.claude/scripts/fix-backport.sh verify
```

Also confirm:
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
- **Use `.claude/scripts/fix-backport.sh`** for all dangerous operations — raw git reset/commit --no-verify/push are not in the permission allowlist
- **Never use `git stash`** — stashes are global across worktrees
- **Never use `git -C`** — all commands run from the worktree working directory
- **Verify file scope after recommit** — each commit must touch ONLY the files from the original commit, nothing else. If `git show --stat` shows extra files, the recommit is wrong.
- Never modify commit messages beyond what's needed (preserve co-author lines, cherry-pick references, etc.)
- If a conflict resolution is ambiguous, ask the user before proceeding
