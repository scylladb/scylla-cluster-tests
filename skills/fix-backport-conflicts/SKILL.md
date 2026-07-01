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
the helper script. **Use the absolute path** — the script lives on `master` and does NOT
exist on PR branches:

```
SCRIPT=/home/fruch/projects/scylla-cluster-tests/.claude/scripts/fix-backport.sh
```

**Always use `$SCRIPT` — never a relative path.** The script is already permitted in
`.claude/settings.json` via its absolute path.

Available subcommands:

| Subcommand | Purpose |
|------------|---------|
| `checkout <PR>` | Checkout PR branch, print base/head/headRepoOwner + push command |
| `rebase <BASE_REF>` | Fetch latest base branch and rebase PR onto it |
| `rebase-continue` | Continue rebase after resolving conflicts |
| `resolve-commit` | Stage tracked changes + temp commit (--no-verify) |
| `prepare-recommit <BASE_REF>` | Tag resolved tree, hard-reset to base for clean recommit |
| `recommit <HASH>` | Copy files from resolved tree for one commit, commit with original author |
| `push <REMOTE> <BRANCH>` | Force-push the fixed branch (--force-with-lease) |
| `update-pr <PR>` | Remove 'conflicts' label + mark ready |
| `verify` | Check no conflict markers remain in tracked files |

## Remotes

- `upstream` = target repo (scylladb/scylla-cluster-tests) — rebase target, NEVER push here
- `origin` = user's fork — NEVER push here for backport PRs
- `<headRepoOwner>` (e.g. `scylladbbot`) = the bot's fork where the PR lives — **push HERE only**

## Workflow (executed by the subagent inside its worktree)

**All commands run from the worktree's working directory. Never use `git -C`.**

**CRITICAL — SCRIPT PATH (read this FIRST):**
```bash
SCRIPT=/home/fruch/projects/scylla-cluster-tests/.claude/scripts/fix-backport.sh
```
This absolute path points to the main worktree. It works from ANY directory on this machine.
- **DO NOT** use relative paths like `.claude/scripts/fix-backport.sh`
- **DO NOT** look for the script locally or in the current worktree
- **DO NOT** copy the script anywhere
- **JUST USE** `$SCRIPT <subcommand>` directly — it is already permitted

The default case: conflict markers are baked INTO the commits from a bad cherry-pick.
The workflow resolves them and recommits cleanly.

### 1. Set up and checkout the PR

```bash
SCRIPT=/home/fruch/projects/scylla-cluster-tests/.claude/scripts/fix-backport.sh
$SCRIPT checkout $PR_NUMBER
```

This prints JSON with `base`, `head`, `headRepoOwner` fields, followed by:

```
=== PUSH INSTRUCTIONS ===
Remote: scylladbbot
Branch: backport/XXXX/to-YYYY
Command: $SCRIPT push scylladbbot backport/XXXX/to-YYYY
```

Save the base ref name and the push command for later.

### 2. Rebase onto the latest base branch

```bash
$SCRIPT rebase upstream/<baseRefName>
```

- `REBASE_OK` → rebase succeeded. Proceed to step 3.
- `REBASE_CONFLICTS` → rebase stopped with conflicts. Resolve them (step 4 logic),
  then `git add <files>` and `$SCRIPT rebase-continue`. Repeat if next commit also conflicts.
- `ERROR: Found N commits` → wrong base ref or branch doesn't share ancestry. Abort.

### 3. Check for inline conflict markers

```bash
$SCRIPT verify
```

If "CLEAN" — no conflicts remain. Skip to step 5 (verify + push).

If markers are found — the commits contain baked-in conflict markers from the original
cherry-pick. This is the **default case**. Proceed to step 4.

### 4. Resolve conflicts and recommit

This is the standard flow for backport PRs:

**4a. Identify the PR commits:**
```bash
git log --oneline upstream/<baseRefName>..HEAD
```
Save the commit hashes (oldest first).

**4b. For each file with conflict markers**, read it, understand both sides:
- **HEAD side** (`<<<<<<<` to `=======`) — code already in the base branch
- **Incoming side** (`=======` to `>>>>>>>`) — the backported change

Resolution: take the incoming (backported) version as the intent of the PR. Keep any
additions from HEAD that don't conflict with the backport's approach.

**4c. Edit files to remove ALL conflict markers.**

**4d. Verify clean:**
```bash
$SCRIPT verify
```
Must print "CLEAN".

**4e. Create temp commit and prepare for recommit:**
```bash
$SCRIPT resolve-commit
$SCRIPT prepare-recommit upstream/<baseRefName>
```

**4f. Recommit each original commit cleanly:**
```bash
$SCRIPT recommit <original-commit-hash>
```
Repeat for each commit (oldest first). This preserves original author and message.

### 5. Verify the result

```bash
$SCRIPT verify
git log --oneline upstream/<baseRefName>..HEAD
git show --stat HEAD
```

Confirm:
- No conflict markers remain
- Commit count matches the original PR
- Each commit's `--stat` matches expected files
- No files outside the original commit's scope are included

### 6. Push and update PR

Use the push command from step 1:
```bash
$SCRIPT push <headRepoOwner> <headRefName>
$SCRIPT update-pr $PR_NUMBER
```

## Important rules

- **Always use the absolute `$SCRIPT` path** — never relative, never copy the script
- **Always dispatch to a worktree subagent** — never run in the current working directory
- **Always rebase onto the latest base branch first** — resolving stale conflicts wastes work
- **Never use `git stash`** — stashes are global across worktrees
- **Never use `git -C`** — all commands run from the worktree working directory
- **Verify file scope after recommit** — each commit must touch ONLY the files from the original commit
- Never modify commit messages beyond what's needed (preserve co-author lines, cherry-pick refs)
- If a conflict resolution is ambiguous, ask the user before proceeding
