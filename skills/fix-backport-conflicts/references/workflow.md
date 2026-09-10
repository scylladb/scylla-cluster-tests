# Fix-Backport Workflow Reference

Detailed step-by-step instructions for the subagent executing inside its worktree.

## Subcommand Reference

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

## Workflow Steps

**All commands run from the worktree's working directory. Never use `git -C`.**

### Step 0. Bootstrap the script

The helper script lives on `master` and does NOT exist on PR branches. Derive its
path from the main worktree:

```bash
SCRIPT="$(git worktree list | head -1 | awk '{print $1}')/.claude/scripts/fix-backport.sh"
```

Verify it exists:

```bash
if [ ! -x "$SCRIPT" ]; then
    echo "Script not found at $SCRIPT — bootstrapping from upstream/master..."
    git show upstream/master:.claude/scripts/fix-backport.sh > /tmp/fix-backport.sh
    chmod +x /tmp/fix-backport.sh
    SCRIPT="/tmp/fix-backport.sh"
fi
```

Use `$SCRIPT` for all subsequent commands.

### Step 1. Checkout the PR

```bash
$SCRIPT checkout $PR_NUMBER
```

This prints JSON with `base`, `head`, `headRepoOwner` fields, followed by:

```text
=== PUSH INSTRUCTIONS ===
Remote: scylladbbot
Branch: backport/XXXX/to-YYYY
Command: /path/to/fix-backport.sh push scylladbbot backport/XXXX/to-YYYY
```

Save the base ref name and the push command for later.

### Step 2. Rebase onto the latest base branch

```bash
$SCRIPT rebase upstream/<baseRefName>
```

- `REBASE_OK` → rebase succeeded. Proceed to step 3.
- `REBASE_CONFLICTS` → rebase stopped with conflicts. Resolve them (step 4 logic),
  then `git add <files>` and `$SCRIPT rebase-continue`. Repeat if next commit also conflicts.
- `ERROR: Found N commits` → wrong base ref or branch doesn't share ancestry. Abort.

### Step 3. Check for inline conflict markers

```bash
$SCRIPT verify
```

If "CLEAN" — no conflicts remain. Skip to step 5 (verify + push).

If markers are found — the commits contain baked-in conflict markers from the original
cherry-pick. This is the **default case**. Proceed to step 4.

### Step 4. Resolve conflicts and recommit

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

### Step 5. Verify the result

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

### Step 6. Push and update PR

Use the push command from step 1:

```bash
$SCRIPT push <headRepoOwner> <headRefName>
$SCRIPT update-pr $PR_NUMBER
```

## Important Rules

- **Always derive `$SCRIPT` dynamically** — never hardcode an absolute path
- **Always dispatch to a worktree subagent** — never run in the current working directory
- **Always rebase onto the latest base branch first** — resolving stale conflicts wastes work
- **Never use `git stash`** — stashes are global across worktrees
- **Never use `git -C`** — all commands run from the worktree working directory
- **Verify file scope after recommit** — each commit must touch ONLY the files from the original commit
- Never modify commit messages beyond what's needed (preserve co-author lines, cherry-pick refs)
- If a conflict resolution is ambiguous, ask the user before proceeding
