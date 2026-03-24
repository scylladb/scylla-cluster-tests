#!/usr/bin/env bash
# Provision a Cassandra AWS cluster once, then reuse it for multiple test runs.
#
# Usage:
#   ./run-aws-cassandra-provision-reuse.sh provision [cassandra_version]
#   ./run-aws-cassandra-provision-reuse.sh reuse [test_id]
#   ./run-aws-cassandra-provision-reuse.sh destroy [test_id]
#
# Subcommands:
#   provision  - Provision VMs, install Cassandra, run a short test, and keep
#                instances alive. Saves test_id for later reuse.
#   reuse      - Rerun test on an existing cluster (skips provisioning and
#                Cassandra install). Reads test_id from .cassandra-test-id
#                unless an explicit test_id is given.
#   destroy    - Clean up all AWS resources for the given test_id.
#
# The provision step uses `sct.py provision-resources` to create VMs, then
# `sct.py run-test` (with the same test_id) to install Cassandra and run the
# workload. Instances are kept alive for subsequent reuse runs that skip
# provisioning and Cassandra install entirely.
#
# Prerequisites:
#   - AWS credentials configured (OKTA or aws configure)
#   - SCT_AMI_ID_DB_CASSANDRA set to a Ubuntu 22.04 AMI, or rely on the default SSM resolve
#   - SCT_REGION_NAME set (e.g., eu-west-1)
#
# Examples:
#   ./run-aws-cassandra-provision-reuse.sh provision        # Cassandra 4.1
#   ./run-aws-cassandra-provision-reuse.sh provision 5.0    # Cassandra 5.0
#   ./run-aws-cassandra-provision-reuse.sh reuse            # reuse last provisioned cluster
#   ./run-aws-cassandra-provision-reuse.sh reuse abc-123    # reuse specific test_id
#   ./run-aws-cassandra-provision-reuse.sh destroy          # destroy last provisioned cluster

set -euo pipefail

STATE_FILE=".cassandra-test-id"
TEST_NAME="longevity_test.LongevityTest.test_custom_time"
CONFIGS=(
    test-cases/cassandra-aws-provision-test.yaml
    configurations/network_config/test_communication_public.yaml
)

usage() {
    echo "Usage: $0 <provision|reuse|destroy> [arg]"
    echo ""
    echo "  provision [version]  - Provision VMs + install Cassandra (default: 4.1)"
    echo "  reuse [test_id]      - Rerun on existing cluster (skip install)"
    echo "  destroy [test_id]    - Clean up AWS resources"
    exit 1
}

setup_common_env() {
    export SCT_USE_MGMT=false
    export SCT_IP_SSH_CONNECTIONS=public
    export SCT_INSTANCE_PROVISION=on_demand
    export SCT_AMI_ID_DB_CASSANDRA="${SCT_AMI_ID_DB_CASSANDRA:-resolve:ssm:/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id}"
    # Keep instances alive
    export SCT_POST_BEHAVIOR_DB_NODES=keep
    export SCT_POST_BEHAVIOR_LOADER_NODES=keep
    export SCT_POST_BEHAVIOR_MONITOR_NODES=keep
}

build_config_args() {
    local args=""
    for cfg in "${CONFIGS[@]}"; do
        args+=" --config $cfg"
    done
    echo "$args"
}

get_saved_test_id() {
    local test_id="${1:-}"
    if [[ -n "$test_id" ]]; then
        echo "$test_id"
        return
    fi
    if [[ -f "$STATE_FILE" ]]; then
        cat "$STATE_FILE"
        return
    fi
    echo "ERROR: No test_id provided and $STATE_FILE not found." >&2
    echo "       Run 'provision' first or pass a test_id." >&2
    exit 1
}

cmd_provision() {
    local version="${1:-4.1}"
    export SCT_CASSANDRA_VERSION="$version"
    setup_common_env

    # Generate a test_id upfront for provision-resources
    local test_id
    test_id=$(python3 -c "from uuid import uuid4; print(uuid4())")
    export SCT_TEST_ID="$test_id"

    echo "=== Provisioning Cassandra ${version} AWS cluster ==="
    echo "    test_id: $test_id"
    echo "    AMI: ${SCT_AMI_ID_DB_CASSANDRA}"
    echo "    Region: ${SCT_REGION_NAME:-eu-west-1}"

    # Save test_id early so destroy works even if provision fails
    echo "$test_id" > "$STATE_FILE"

    # Step 1: Provision VMs only (fast — no Cassandra install)
    echo ""
    echo "--- Step 1/2: Provisioning VMs via sct.py provision-resources ---"
    # shellcheck disable=SC2046
    uv run sct.py provision-resources \
        --backend aws \
        --test-name "$TEST_NAME" \
        $(build_config_args)

    # Step 2: Run test with the same test_id (full node_setup + workload)
    echo ""
    echo "--- Step 2/2: Installing Cassandra + running test ---"
    # shellcheck disable=SC2046
    uv run sct.py run-test "$TEST_NAME" \
        --backend aws \
        $(build_config_args)

    echo ""
    echo "=== Cluster provisioned ==="
    echo "    test_id: $test_id"
    echo "    Saved to: $STATE_FILE"
    echo "    Reuse:   $0 reuse"
    echo "    Destroy: $0 destroy"
}

cmd_reuse() {
    local test_id
    test_id=$(get_saved_test_id "${1:-}")
    setup_common_env

    export SCT_REUSE_CLUSTER="$test_id"

    echo "=== Reusing Cassandra cluster: $test_id ==="
    # shellcheck disable=SC2046
    uv run sct.py run-test "$TEST_NAME" \
        --backend aws \
        $(build_config_args)
}

cmd_destroy() {
    local test_id
    test_id=$(get_saved_test_id "${1:-}")

    echo "=== Destroying AWS resources for test_id: $test_id ==="
    uv run sct.py clean-resources --test-id "$test_id"

    if [[ -f "$STATE_FILE" ]]; then
        rm "$STATE_FILE"
        echo "Removed $STATE_FILE"
    fi
    echo "=== Done ==="
}

COMMAND="${1:-help}"
shift || true

case "$COMMAND" in
    provision) cmd_provision "$@" ;;
    reuse)     cmd_reuse "$@" ;;
    destroy)   cmd_destroy "$@" ;;
    *)         usage ;;
esac
