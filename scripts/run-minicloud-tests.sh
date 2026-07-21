#!/bin/bash
set -euo pipefail

# Comprehensive minicloud test runner.
# Runs all available minicloud test scenarios and reports results.
#
# Usage:
#   MINICLOUD_BINARY=/path/to/minicloud \
#   MINICLOUD_SETUP_SCRIPT=/path/to/minicloud-setup.sh \
#   bash scripts/run-minicloud-tests.sh [--aws-only|--gce-only]

MODE="${1:-all}"
RESULTS=()
FAILED=0

run_test() {
    local name="$1"; shift
    echo ""
    echo "=============================="
    echo "=== Running: $name"
    echo "=============================="
    if "$@"; then
        RESULTS+=("PASS: $name")
    else
        RESULTS+=("FAIL: $name")
        FAILED=$((FAILED + 1))
    fi
}

# AWS tests
if [[ "$MODE" == "all" || "$MODE" == "--aws-only" ]]; then
    run_test "aws-artifact (1 DB, service check)" \
        bash scripts/run-minicloud-artifact-test.sh aws

    run_test "aws-provision (3 DB + loader, stress + nemesis)" \
        bash scripts/run-minicloud-provision-test.sh
fi

# GCE tests
if [[ "$MODE" == "all" || "$MODE" == "--gce-only" ]]; then
    run_test "gce-artifact (1 DB, service check)" \
        bash scripts/run-minicloud-artifact-test.sh gce

    run_test "gce-provision (3 DB + loader, stress + nemesis)" \
        bash scripts/run-minicloud-gce-provision-test.sh
fi

# Summary
echo ""
echo "=============================="
echo "=== MINICLOUD TEST RESULTS ==="
echo "=============================="
printf '%s\n' "${RESULTS[@]}"
echo ""
echo "Total: ${#RESULTS[@]} tests, $FAILED failed"

exit $FAILED
