#!/bin/bash
set -eu

TARGET_BRANCH=""
TARGET_BRANCH_ARG=""
EXTRA_ARGS=()

# Separate target branch argument from other pre-commit arguments
# If the first argument does not start with a '-', treat it as the target branch.
if [[ -n "${1-}" && ! "$1" =~ ^- ]]; then
  TARGET_BRANCH_ARG="$1"
  shift # Consume the branch argument, so the rest can be processed as extra args
fi
EXTRA_ARGS=("$@") # Store the rest of the arguments

# 1. Check for GitHub Actions pull_request event
if [ "${GITHUB_EVENT_NAME-}" == "pull_request" ] && [ -n "${GITHUB_BASE_REF-}" ]; then
  echo "GitHub Action PR detected. Base ref: ${GITHUB_BASE_REF}"
  # In GH Actions for PRs, the base branch is fetched into refs/remotes/origin/<base_ref>
  TARGET_BRANCH="origin/${GITHUB_BASE_REF}"
fi

# 2. Check for local 'gh pr checkout' context
if [ -z "$TARGET_BRANCH" ]; then
  # `gh pr view` returns non-zero if not on a PR branch, so this also checks context.
  if command -v gh &> /dev/null && BASE_REF_NAME=$(gh pr view --json baseRefName --jq .baseRefName 2>/dev/null); then
    if [ -n "$BASE_REF_NAME" ]; then
        echo "Local 'gh' PR branch detected. Base ref: ${BASE_REF_NAME}"
        # Prefer 'upstream' remote if it exists, otherwise 'origin'
        if git remote | grep -q '^upstream$'; then
            REMOTE="upstream"
        else
            REMOTE="origin"
        fi
        TARGET_BRANCH="$REMOTE/$BASE_REF_NAME"
        echo "Using '$TARGET_BRANCH' as base."
    fi
  fi
fi

# 3. Fallback to argument or default
if [ -z "$TARGET_BRANCH" ]; then
  TARGET_BRANCH="${TARGET_BRANCH_ARG:-upstream/master}"
  echo "Using fallback target branch: $TARGET_BRANCH"
fi

# 4. Validate that the target branch reference exists, otherwise try alternatives
if ! git rev-parse --verify "$TARGET_BRANCH" &>/dev/null; then
  echo "Warning: '$TARGET_BRANCH' does not exist in the working tree."

  # Try common alternatives
  for alternative in "origin/master" "origin/main" "master" "main"; do
    if git rev-parse --verify "$alternative" &>/dev/null; then
      echo "Using alternative: '$alternative'"
      TARGET_BRANCH="$alternative"
      break
    fi
  done

  # If still not found, try to fetch and use merge-base
  if ! git rev-parse --verify "$TARGET_BRANCH" &>/dev/null; then
    echo "Attempting to find merge-base with available branches..."
    # Get the merge-base with the first available remote master/main
    for remote_branch in "origin/master" "origin/main"; do
      if git rev-parse --verify "$remote_branch" &>/dev/null; then
        MERGE_BASE=$(git merge-base HEAD "$remote_branch" 2>/dev/null || echo "")
        if [ -n "$MERGE_BASE" ]; then
          echo "Using merge-base: $MERGE_BASE (from $remote_branch)"
          TARGET_BRANCH="$MERGE_BASE"
          break
        fi
      fi
    done
  fi

  # Final check - if still not resolved, fail with helpful message
  if ! git rev-parse --verify "$TARGET_BRANCH" &>/dev/null; then
    echo "Error: Could not resolve a valid base branch reference."
    echo "Available branches:"
    git branch -a | head -20
    exit 1
  fi
fi

echo "Running pre-commit from '$TARGET_BRANCH' to 'HEAD' with extra args: ${EXTRA_ARGS[*]}"

# Run pre-commit with any extra arguments
pre-commit run --from-ref "$TARGET_BRANCH" --to-ref HEAD "${EXTRA_ARGS[@]}"
