#!/bin/bash
# PostToolUse hook: auto-run pytest when Claude edits a unit test file

INPUT=$(cat)  # read hook JSON from stdin (not env var)

# file_path is nested under tool_input, not at top level
file=$(echo "$INPUT" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d.get('tool_input', {}).get('file_path', ''))
")

# Only act on unit test files
if [[ "$file" != */unit_tests/test_* ]]; then
    exit 0
fi

# Run tests, capture all output
output=$(uv run python -m pytest "$file" -x -q --no-header --tb=short -n0 2>&1)
exit_code=$?

if [ $exit_code -ne 0 ]; then
    # Show failure details via stderr (visible to Claude as hook feedback)
    echo "Unit tests FAILED for: $file" >&2
    echo "$output" | tail -20 >&2
    exit 2  # Block the edit
fi

# Tests passed — show summary via stderr so user sees it
echo "Unit tests PASSED for: $file" >&2
echo "$output" | tail -5 >&2
exit 0
