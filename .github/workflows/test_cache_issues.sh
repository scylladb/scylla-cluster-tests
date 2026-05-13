#!/bin/bash
# Test script for cache-issues workflow pagination.
# Usage:
#   export GH_TOKEN="$(gh auth token)"
#   ./test_cache_issues.sh [repo] [state]
# Examples:
#   ./test_cache_issues.sh scylladb closed
#   ./test_cache_issues.sh scylla-dtest closed

set -euo pipefail

REPO="${1:-scylla-dtest}"
STATE="${2:-closed}"

echo "=== Testing cache-issues pagination for scylladb/$REPO (state=$STATE) ==="

ISSUES_FILE=$(mktemp)
PRS_FILE=$(mktemp)
trap 'rm -f "$ISSUES_FILE" "$PRS_FILE"' EXIT

echo ""
echo "--- Fetching $STATE issues (using gh api --paginate) ---"
time gh api --paginate "repos/scylladb/$REPO/issues?state=$STATE&per_page=100&direction=asc" \
  --jq '.[] | select(.pull_request == null) | "\(.number),\(.state),\([.labels[].name] | join("|")),\(.title)"' \
  > "$ISSUES_FILE"

echo ""
echo "--- Fetching $STATE PRs (using gh api --paginate) ---"
time gh api --paginate "repos/scylladb/$REPO/pulls?state=$STATE&per_page=100&direction=asc" \
  --jq '.[] | "\(.number),\(if .merged_at then "MERGED" elif .state == "closed" then "closed" else .state end),\([.labels[].name] | join("|")),\(.title)"' \
  > "$PRS_FILE"

echo ""
echo "=== Results ==="
ISSUE_COUNT=$(wc -l < "$ISSUES_FILE")
PR_COUNT=$(wc -l < "$PRS_FILE")
echo "Issues fetched: $ISSUE_COUNT"
echo "PRs fetched:    $PR_COUNT"

echo ""
echo "--- Validation ---"

PASS=true

if [ "$REPO" = "scylladb" ] && [ "$STATE" = "closed" ]; then
    if grep -q "^9216," "$ISSUES_FILE"; then
        echo "PASS: Issue #9216 found (the issue that triggered DTEST-220)"
    else
        echo "FAIL: Issue #9216 NOT found — pagination is broken!"
        PASS=false
    fi

    if [ "$ISSUE_COUNT" -lt 10000 ]; then
        echo "FAIL: Expected 10000+ closed issues for scylladb/scylladb, got $ISSUE_COUNT"
        PASS=false
    else
        echo "PASS: Got $ISSUE_COUNT closed issues (expected 10000+)"
    fi
fi

echo ""
echo "--- Sample output (first 3 lines) ---"
head -3 "$ISSUES_FILE"
echo "--- Sample output (last 3 lines) ---"
tail -3 "$ISSUES_FILE"

echo ""
if [ "$PASS" = true ]; then
    echo "=== All checks passed ==="
else
    echo "=== FAILURES DETECTED ==="
    exit 1
fi
