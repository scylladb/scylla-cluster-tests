#!/usr/bin/env bash
set -euo pipefail

CMD="${1:?Usage: fix-backport.sh <subcommand> [args...]}"
shift

case "$CMD" in
  checkout)
    PR="${1:?Usage: fix-backport.sh checkout <PR_NUMBER>}"
    # Get PR metadata
    PR_JSON=$(gh pr view "$PR" --repo scylladb/scylla-cluster-tests --json baseRefName,headRefName,headRepository,headRepositoryOwner,number)
    BASE=$(echo "$PR_JSON" | jq -r '.baseRefName')
    HEAD_REF=$(echo "$PR_JSON" | jq -r '.headRefName')
    HEAD_REPO_OWNER=$(echo "$PR_JSON" | jq -r '.headRepositoryOwner.login')

    # Add remote for the head repo owner if not already present
    if ! git remote get-url "$HEAD_REPO_OWNER" &>/dev/null; then
      HEAD_REPO_NAME=$(echo "$PR_JSON" | jq -r '.headRepository.name')
      git remote add "$HEAD_REPO_OWNER" "https://github.com/${HEAD_REPO_OWNER}/${HEAD_REPO_NAME}.git" 2>/dev/null || true
    fi

    # Fetch and checkout the PR branch
    git fetch "$HEAD_REPO_OWNER" "$HEAD_REF"
    git checkout -B "pr-${PR}" "$HEAD_REPO_OWNER/$HEAD_REF"

    echo '{"base":"'"$BASE"'","head":"'"$HEAD_REF"'","headRepoOwner":"'"$HEAD_REPO_OWNER"'"}'
    ;;

  rebase)
    BASE_REF="${1:?Usage: fix-backport.sh rebase <BASE_REF>}"
    git fetch upstream "${BASE_REF#upstream/}" 2>/dev/null || true
    if git rebase "$BASE_REF" 2>/dev/null; then
      echo "REBASE_OK"
    else
      echo "REBASE_CONFLICTS"
    fi
    ;;

  rebase-continue)
    GIT_EDITOR=true git rebase --continue
    ;;

  resolve-commit)
    git add -A
    git commit --no-verify -m "resolve conflicts" --allow-empty 2>/dev/null || true
    echo "COMMITTED"
    ;;

  prepare-recommit)
    BASE_REF="${1:?Usage: fix-backport.sh prepare-recommit <BASE_REF>}"
    git tag -f resolved-tree HEAD
    git reset --hard "$BASE_REF"
    echo "READY"
    ;;

  recommit)
    HASH="${1:?Usage: fix-backport.sh recommit <HASH>}"
    git checkout resolved-tree -- .
    AUTHOR=$(git log -1 --format='%an <%ae>' "$HASH")
    DATE=$(git log -1 --format='%aI' "$HASH")
    MSG=$(git log -1 --format='%B' "$HASH")
    GIT_AUTHOR_NAME="$(git log -1 --format='%an' "$HASH")" \
    GIT_AUTHOR_EMAIL="$(git log -1 --format='%ae' "$HASH")" \
    GIT_AUTHOR_DATE="$DATE" \
    git commit --no-verify -m "$MSG"
    echo "RECOMMITTED"
    ;;

  push)
    REMOTE="${1:?Usage: fix-backport.sh push <REMOTE> <BRANCH>}"
    BRANCH="${2:?Usage: fix-backport.sh push <REMOTE> <BRANCH>}"
    git push --force-with-lease "$REMOTE" "HEAD:$BRANCH"
    echo "PUSHED"
    ;;

  update-pr)
    PR="${1:?Usage: fix-backport.sh update-pr <PR_NUMBER>}"
    gh pr edit "$PR" --repo scylladb/scylla-cluster-tests --remove-label conflicts 2>/dev/null || true
    gh pr ready "$PR" --repo scylladb/scylla-cluster-tests 2>/dev/null || true
    echo "PR_UPDATED"
    ;;

  verify)
    if grep -rn '<<<<<<< \|=======$\|>>>>>>> ' --include='*.py' --include='*.yaml' --include='*.yml' --include='*.json' --include='*.md' . 2>/dev/null; then
      echo "CONFLICTS_FOUND"
      exit 1
    else
      echo "CLEAN"
    fi
    ;;

  *)
    echo "Unknown subcommand: $CMD" >&2
    exit 1
    ;;
esac
