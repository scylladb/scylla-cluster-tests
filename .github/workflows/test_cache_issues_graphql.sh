#!/bin/bash
set -euo pipefail

REPO="${1:-scylladb}"
STATE="${2:-closed}"

echo "=== Testing GraphQL pagination for scylladb/$REPO (state=$STATE) ==="

GQL_STATE=$(echo "$STATE" | tr '[:lower:]' '[:upper:]')
OUTPUT=$(mktemp)
trap 'rm -f "$OUTPUT"' EXIT

CURSOR=""
TOTAL=0
PAGE=0

while true; do
  PAGE=$((PAGE + 1))
  if [ -z "$CURSOR" ]; then
    AFTER_CLAUSE=""
  else
    AFTER_CLAUSE=", after: \"$CURSOR\""
  fi

  RESPONSE=$(gh api graphql -f query="
    query {
      repository(owner: \"scylladb\", name: \"$REPO\") {
        issues(states: [$GQL_STATE], first: 100, orderBy: {field: CREATED_AT, direction: ASC}$AFTER_CLAUSE) {
          pageInfo { hasNextPage endCursor }
          nodes {
            number
            state
            labels(first: 10) { nodes { name } }
            title
          }
        }
      }
    }
  ")

  COUNT=$(echo "$RESPONSE" | jq '.data.repository.issues.nodes | length')
  TOTAL=$((TOTAL + COUNT))

  echo "$RESPONSE" | jq -r '.data.repository.issues.nodes[] | "\(.number),\(.state | ascii_downcase),\([.labels.nodes[].name] | join("|")),\(.title)"' >> "$OUTPUT"

  HAS_NEXT=$(echo "$RESPONSE" | jq -r '.data.repository.issues.pageInfo.hasNextPage')
  CURSOR=$(echo "$RESPONSE" | jq -r '.data.repository.issues.pageInfo.endCursor')

  if [ "$((PAGE % 10))" -eq 0 ]; then
    echo "  page $PAGE: total so far $TOTAL"
  fi

  if [ "$HAS_NEXT" != "true" ]; then
    break
  fi
done

echo ""
echo "=== Results ==="
echo "Pages:          $PAGE"
echo "Issues fetched: $TOTAL"

echo ""
echo "--- Validation ---"
PASS=true

if [ "$REPO" = "scylladb" ] && [ "$STATE" = "closed" ]; then
  if grep -q "^9216," "$OUTPUT"; then
    echo "PASS: Issue #9216 found"
  else
    echo "FAIL: Issue #9216 NOT found"
    PASS=false
  fi

  if [ "$TOTAL" -lt 10000 ]; then
    echo "FAIL: Expected 10000+ closed issues, got $TOTAL"
    PASS=false
  else
    echo "PASS: Got $TOTAL closed issues (expected 10000+)"
  fi
fi

echo ""
echo "--- Sample (first 3) ---"
head -3 "$OUTPUT"
echo "--- Sample (last 3) ---"
tail -3 "$OUTPUT"

echo ""
if [ "$PASS" = true ]; then
  echo "=== All checks passed ==="
else
  echo "=== FAILURES DETECTED ==="
  exit 1
fi
