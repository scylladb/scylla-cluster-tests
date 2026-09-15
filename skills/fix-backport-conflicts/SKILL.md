---
name: fix-backport-conflicts
description: >-
  Fix inline merge conflict markers in backport PRs by resolving
  conflicts and recommitting cleanly with original metadata preserved.
  Use when a backport PR has unresolved conflict markers, a cherry-pick
  produced merge conflicts, a PR has the 'conflicts' label and needs
  to be made ready for review, or files show <<<<<<< / ======= / >>>>>>>
  markers from a bad cherry-pick. Supports bulk mode for multiple PRs.
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
- A prompt containing: the PR number, and the full Workflow from `references/workflow.md`

After all subagents complete, summarize which PRs succeeded and which failed or had ambiguous
conflicts requiring user input. Remind the user to review the changes.

## Helper Script

All dangerous git operations (rebase, reset, commit --no-verify, force-push) are wrapped in
the helper script at `.claude/scripts/fix-backport.sh`. The script lives on `master` and
does NOT exist on PR branches.

**Derive the script path dynamically — never hardcode an absolute path:**

```bash
SCRIPT="$(git worktree list | head -1 | awk '{print $1}')/.claude/scripts/fix-backport.sh"
```

**Bootstrap fallback** — if `$SCRIPT` is not accessible (e.g. main worktree unavailable):

```bash
if [ ! -x "$SCRIPT" ]; then
    git show upstream/master:.claude/scripts/fix-backport.sh > /tmp/fix-backport.sh
    chmod +x /tmp/fix-backport.sh
    SCRIPT="/tmp/fix-backport.sh"
fi
```

The script is already permitted in `.claude/settings.json`. Always use `$SCRIPT <subcommand>`
for all git operations — never run raw git rebase/reset/push directly.

## Remotes

- `upstream` = target repo (scylladb/scylla-cluster-tests) — rebase target, NEVER push here
- `origin` = user's fork — NEVER push here for backport PRs
- `<headRepoOwner>` (e.g. `scylladbbot`) = the bot's fork where the PR lives — **push HERE only**

## Workflow Summary

See `references/workflow.md` for detailed step-by-step instructions. High-level flow:

1. **Bootstrap** — derive `$SCRIPT` path, verify or fallback
2. **Checkout** — `$SCRIPT checkout $PR_NUMBER` → save base ref + push command
3. **Rebase** — `$SCRIPT rebase upstream/<baseRefName>` → handle OK/conflicts/error
4. **Verify** — `$SCRIPT verify` → if CLEAN skip to push, otherwise resolve
5. **Resolve & recommit** — edit files, remove markers, `resolve-commit`, `prepare-recommit`, `recommit` per original hash
6. **Push** — use the push command from checkout output, then `update-pr`

## Decision Points

| Situation | Action |
|-----------|--------|
| `REBASE_OK` + `verify` = CLEAN | Skip to push (step 6) |
| `REBASE_CONFLICTS` | Resolve in-place, `git add`, `$SCRIPT rebase-continue` |
| `verify` finds markers after rebase | Full recommit flow (step 4 in workflow) |
| `ERROR: Found N commits` (>50) | Wrong base ref — abort, report to user |
| Ambiguous conflict resolution | Ask user before proceeding |
| Script not found at derived path | Use bootstrap fallback to `/tmp/fix-backport.sh` |

## Critical Rules

- **Always derive `$SCRIPT` dynamically** — never hardcode absolute paths
- **Always dispatch to a worktree subagent** — never run in current working directory
- **Always rebase onto latest base first** — resolving stale conflicts wastes work
- **Never use `git stash`** — stashes are global across worktrees
- **Never use `git -C`** — all commands run from the worktree working directory
- **Verify file scope after recommit** — each commit must touch ONLY its original files
- If a conflict resolution is ambiguous, ask the user before proceeding
