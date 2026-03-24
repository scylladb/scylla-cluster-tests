#!/usr/bin/env bash
# fix-backport.sh — Scoped automation script for the fix-backport-conflicts skill.
# Each subcommand performs one step of the backport fix workflow.
# This script exists so that .claude/settings.json can allow ONLY this script,
# keeping dangerous git operations (reset, force-push, commit --no-verify)
# out of the blanket permission list.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: fix-backport.sh <command> [args...]

Commands:
  checkout <PR_NUMBER>              Checkout the PR branch and print base branch info
  resolve-commit                    Stage all changes and create a temporary commit
  prepare-recommit <BASE_REF>       Hard-reset to base, keeping resolved tree accessible via TEMP tag
  recommit <ORIGINAL_HASH>          Apply one commit's file changes from the resolved tree and commit
  push <REMOTE> <BRANCH>            Force-push the fixed branch (--force-with-lease)
  update-pr <PR_NUMBER>             Remove 'conflicts' label and mark PR ready
  verify                            Check no conflict markers remain in tracked files
EOF
    exit 1
}

cmd_checkout() {
    local pr_number="${1:?PR number required}"
    gh pr checkout "$pr_number"
    # Print metadata for the caller to parse
    gh pr view "$pr_number" --json baseRefName,headRefName --jq '{base: .baseRefName, head: .headRefName}'
}

cmd_resolve_commit() {
    git add -A
    git commit --no-verify -m "TEMP: resolved conflicts"
}

cmd_prepare_recommit() {
    local base_ref="${1:?Base ref required (e.g. upstream/branch-2026.1)}"

    # Tag the current HEAD (which has the resolved tree) so we can reference it after reset
    git tag -f _fix_backport_resolved HEAD

    # Hard reset to base — working tree is now clean and matches the base branch exactly
    git reset --hard "$base_ref"

    echo "Ready to recommit. Resolved tree saved as tag _fix_backport_resolved."
    echo "Commits to recommit:"
    # Show what was between base and resolved (excluding TEMP)
    git log --oneline "$base_ref".._fix_backport_resolved -- | grep -v "^.* TEMP: resolved conflicts$" || true
}

cmd_recommit() {
    local original_hash="${1:?Original commit hash required}"

    # Get original author info and message
    local author_name author_email message
    author_name="$(git show --format="%an" --no-patch "$original_hash")"
    author_email="$(git show --format="%ae" --no-patch "$original_hash")"
    message="$(git show --format="%B" --no-patch "$original_hash")"

    # Get the list of files this commit touched
    local files
    files="$(git show --format="" --name-only "$original_hash" | grep -v '^$')"

    # For each file, copy the resolved version from _fix_backport_resolved
    # This ensures we get ONLY the resolved content, not stale working-tree diffs
    echo "$files" | while IFS= read -r f; do
        [ -z "$f" ] && continue
        # Check if the file exists in the resolved tree
        if git cat-file -e "_fix_backport_resolved:$f" 2>/dev/null; then
            # File exists in resolved tree — extract it
            git show "_fix_backport_resolved:$f" > "$f"
            git add "$f"
        else
            # File was deleted in the resolved tree
            git rm -f "$f" 2>/dev/null || true
        fi
    done

    # Commit with original authorship
    GIT_AUTHOR_NAME="$author_name" GIT_AUTHOR_EMAIL="$author_email" \
        git commit --no-verify -m "$message"

    echo "Recommitted: $(echo "$message" | head -1)"
}

cmd_push() {
    local remote="${1:?Remote required (e.g. origin)}"
    local branch="${2:?Branch name required}"
    git push --force-with-lease "$remote" "HEAD:$branch"
}

cmd_update_pr() {
    local pr_number="${1:?PR number required}"
    gh pr edit "$pr_number" --remove-label "conflicts" 2>/dev/null || true
    gh pr ready "$pr_number" 2>/dev/null || true
}

cmd_verify() {
    # Search for conflict markers in tracked files, excluding known false positives
    local markers
    markers="$(git grep -l -E '^(<<<<<<<|>>>>>>>)' -- ':(exclude)*.md' ':(exclude)docker/jepsen/*' 2>/dev/null || true)"
    if [ -n "$markers" ]; then
        echo "CONFLICT MARKERS FOUND in:"
        echo "$markers"
        exit 1
    else
        echo "CLEAN"
        exit 0
    fi
}

[ $# -lt 1 ] && usage

command="$1"
shift

case "$command" in
    checkout)          cmd_checkout "$@" ;;
    resolve-commit)    cmd_resolve_commit "$@" ;;
    prepare-recommit)  cmd_prepare_recommit "$@" ;;
    recommit)          cmd_recommit "$@" ;;
    push)              cmd_push "$@" ;;
    update-pr)         cmd_update_pr "$@" ;;
    verify)            cmd_verify "$@" ;;
    *)                 echo "Unknown command: $command" >&2; usage ;;
esac
