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
  checkout <PR_NUMBER>         Checkout the PR branch and print base branch info
  resolve-commit               Stage all changes and create a temporary commit
  recommit <BASE_REF> <HASH>   Recommit one original commit with preserved authorship
  push <REMOTE> <BRANCH>       Force-push the fixed branch (--force-with-lease)
  update-pr <PR_NUMBER>        Remove 'conflicts' label and mark PR ready
  verify                       Check no conflict markers remain in tracked files
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

cmd_recommit() {
    local base_ref="${1:?Base ref required (e.g. upstream/branch-2026.1)}"
    local original_hash="${2:?Original commit hash required}"

    # Get original author info
    local author_name author_email message
    author_name="$(git show --format="%an" --no-patch "$original_hash")"
    author_email="$(git show --format="%ae" --no-patch "$original_hash")"
    message="$(git show --format="%B" --no-patch "$original_hash")"

    # Get the list of files this commit touched
    local files
    files="$(git show --format="" --name-only "$original_hash")"

    # Stage only the files belonging to this commit
    echo "$files" | while IFS= read -r f; do
        [ -z "$f" ] && continue
        if [ -e "$f" ]; then
            git add "$f"
        fi
    done

    # Commit with original authorship
    GIT_AUTHOR_NAME="$author_name" GIT_AUTHOR_EMAIL="$author_email" \
        git commit --no-verify -m "$message"
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
    checkout)       cmd_checkout "$@" ;;
    resolve-commit) cmd_resolve_commit "$@" ;;
    recommit)       cmd_recommit "$@" ;;
    push)           cmd_push "$@" ;;
    update-pr)      cmd_update_pr "$@" ;;
    verify)         cmd_verify "$@" ;;
    *)              echo "Unknown command: $command" >&2; usage ;;
esac
