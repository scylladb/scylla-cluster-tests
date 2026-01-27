#!/usr/bin/env bash
set -euo pipefail


if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <target-commit>"
  exit 1
fi

target="$1"

RET_POINT=$(git symbolic-ref --short -q HEAD || git rev-parse HEAD)
for c in $(git rev-list "$target"..HEAD --no-merges); do
  echo "Linting commit $c"
  git checkout -q "$c"
  pre-commit run --hook-stage commit-msg --commit-msg-filename .git/hooks/commit-msg || exit 1
done
git checkout "$RET_POINT" --quiet
